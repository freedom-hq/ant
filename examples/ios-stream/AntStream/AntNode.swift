import Foundation
import Network
import SwiftUI

/// Swift wrapper around the hand-written C API in `crates/ant-ffi/`.
///
/// Same shape as `examples/ios-drive`'s wrapper — one opaque
/// `AntHandle*` for the app's lifetime, FFI calls on detached tasks,
/// decoded models published back on the main actor — with two AntStream
/// differences:
///
/// * The account key comes from ``AccountKeystore`` (Keychain / Secure
///   Enclave) and is passed in through `ant_init_with_identity`, so the
///   library never writes `identity.json` into the app container.
/// * After startup we bring up the in-process bee-shaped HTTP gateway
///   (`ant_start_gateway`, `light_mode`) on localhost. That is the
///   surface the publisher (#67) and the HLS viewer (#66) will push
///   segments and feed updates through.
@MainActor
final class AntNode: ObservableObject {
    enum Status: Equatable {
        case idle
        case starting
        case ready
        case failed(String)

        var label: String {
            switch self {
            case .idle: return "Idle"
            case .starting: return "Starting…"
            case .ready: return "Connected"
            case .failed(let m): return "Offline: \(m)"
            }
        }

        var isReady: Bool { if case .ready = self { return true }; return false }
    }

    @Published private(set) var status: Status = .idle
    @Published private(set) var peerCount: Int = 0
    @Published private(set) var plan: StoragePlan?
    @Published private(set) var account: AccountInfo?
    @Published private(set) var settlement: SettlementInfo?
    /// Whether the deployed chequebook actually backs its cheques.
    /// Fetched on demand (`refreshSettlementDeposit`) since it reads
    /// chain; `nil` until then.
    @Published private(set) var settlementDeposit: SettlementDeposit?
    /// Remaining lifetime of the connected plan. Fetched on demand
    /// (`refreshValidity`) since it needs a chain RPC; `nil` until then.
    @Published private(set) var validity: StorageValidity?
    /// `true` once the in-process gateway is listening on
    /// ``gatewayURL``. The publisher and viewer tracks talk to it over
    /// HTTP rather than through more C entry points.
    @Published private(set) var gatewayUp: Bool = false
    /// `true` while the device has no usable network path
    /// (`NWPathMonitor`). The node is suspended for the duration.
    @Published private(set) var isOffline: Bool = false
    /// Which interface the node is currently reaching the network over
    /// — "Wi-Fi", "Cellular", "Wired" or "Offline". The #67 throughput
    /// table is *per network type*, and asking the operator to type
    /// that in is how a cellular row ends up labelled Wi-Fi.
    @Published private(set) var networkLabel: String = "Unknown"
    /// How the account key is protected at rest, for the Storage tab.
    @Published private(set) var keyProtection: AccountKeystore.Protection?

    /// Where the in-process bee gateway listens. Fixed rather than
    /// ephemeral so the publisher/viewer can be pointed at it without
    /// plumbing the port around.
    static let gatewayAddress = "127.0.0.1:1633"
    var gatewayURL: URL? { URL(string: "http://\(Self.gatewayAddress)") }

    private var handle: OpaquePointer?
    private var peerPollTask: Task<Void, Never>?
    private var pathMonitor: NWPathMonitor?
    /// Serialises the node's lifecycle transitions (`start`, `shutdown`,
    /// `restoreAccount`), so exactly one is ever in flight.
    ///
    /// `status` cannot do that job on its own: `restoreAccount` has to
    /// force it back to `.idle` — a `.failed` node is exactly the case
    /// Restore exists for — which would let a second `start()` slip past
    /// the idle guard while the first is still inside
    /// `ant_init_with_identity`. Two inits over one data dir race in
    /// `bind_account_state` (each parking the other's postage /
    /// chequebook / SWAP state, worst case hitting its "refusing to
    /// start: both exist" abort), leave whichever node lost the `handle`
    /// assignment running unshut-down over state now scoped to the other
    /// account, and fight over the gateway's fixed port.
    private var lifecycle: Task<Void, Never>?
    /// How many FFI calls are currently *borrowing* ``handle`` — see
    /// ``withHandle(_:)``.
    private var borrowCount = 0
    /// Parked ``performShutdown()`` calls waiting for `borrowCount` to
    /// reach zero.
    private var drainWaiters: [CheckedContinuation<Void, Never>] = []
    /// Gnosis RPC handed to the gateway so its `/wallet` and `/stamps`
    /// surfaces read real on-chain state (the `chain` build feature).
    private var gnosisRpc: String = AntNode.defaultRpc

    static let defaultRpc = "https://rpc.gnosischain.com"

    // MARK: - Lifecycle

    /// Run `body` once every lifecycle transition queued ahead of it has
    /// finished. Code already running *inside* a transition must call the
    /// `perform…` variants directly — coming back through here would make
    /// it wait on the queue entry it is.
    private func serialized(_ body: @escaping @MainActor () async -> Void) async {
        let previous = lifecycle
        let task = Task { @MainActor in
            await previous?.value
            await body()
        }
        lifecycle = task
        await task.value
    }

    /// Run `body` with the live handle, holding it open for the call's
    /// whole duration. Returns `nil` — without calling `body` — when
    /// there is no node.
    ///
    /// Every ordinary FFI call hands its pointer to a detached task and
    /// then suspends, so it outlives the `handle` read that produced it:
    /// a chain-scanning `ant_storage_discover`, a `refreshValidity` RPC
    /// or a backgrounding `ant_suspend` can still be inside the node
    /// seconds later. `ant_shutdown` deallocates that handle, so freeing
    /// it under one of them is a use-after-free — worst case the
    /// still-running call writing the old account's state into a data dir
    /// `bind_account_state` has already re-scoped to the restored one.
    ///
    /// The lifecycle queue cannot prevent that: it orders transitions
    /// against each other, not against ordinary borrows. So borrows are
    /// counted here instead, and ``performShutdown()`` waits for the
    /// count to fall to zero before it frees anything.
    private func withHandle<T>(_ body: (OpaquePointer) async throws -> T) async rethrows -> T? {
        guard let h = handle else { return nil }
        borrowCount += 1
        defer { endBorrow() }
        return try await body(h)
    }

    private func endBorrow() {
        borrowCount -= 1
        guard borrowCount == 0, !drainWaiters.isEmpty else { return }
        let waiters = drainWaiters
        drainWaiters = []
        for waiter in waiters { waiter.resume() }
    }

    /// Wait until no FFI call is inside the node any more. Callers must
    /// have cleared `handle` first, so no *new* borrow can start.
    private func drainBorrows() async {
        while borrowCount > 0 {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                drainWaiters.append(continuation)
            }
        }
    }

    /// Borrow the handle for one FFI call that returns an owned C string
    /// (see ``withHandle(_:)`` and ``string(name:_:)``). Throws
    /// ``AntError/notReady`` when there is no node.
    private func ffiString(
        name: String,
        _ body: @escaping (OpaquePointer, UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?)
            -> UnsafeMutablePointer<CChar>?
    ) async throws -> String {
        guard let value = try await withHandle({ h in
            try await Self.string(name: name) { errPtr in body(h, errPtr) }
        }) else {
            throw AntError.notReady
        }
        return value
    }

    func start(rpc: String? = nil) async {
        await serialized { await self.performStart(rpc: rpc) }
    }

    private func performStart(rpc: String?) async {
        guard case .idle = status, handle == nil else { return }
        status = .starting
        if let rpc, !rpc.trimmingCharacters(in: .whitespaces).isEmpty {
            gnosisRpc = rpc
        }

        let dataDir = Self.resolveDataDir()
        Self.seedPeerstoreIfNeeded(in: dataDir)
        // Defence in depth: if any build ever left a plaintext key in the
        // container, pull it into the Keychain and delete the file.
        _ = AccountKeystore.migrateLegacyIdentityFile(at: dataDir.appendingPathComponent("identity.json"))

        let identity: String
        do {
            // Minting is only ever right on a genuine first launch. An
            // empty Keychain on a data dir that already ran an account
            // means the key was lost, not that there never was one — most
            // sharply when another device in the iCloud circle turned the
            // sync toggle off and the deletion propagated here. Creating
            // a fresh account there would bury the funded one under a
            // stranger's marker; failing sends the user to Restore.
            identity = try AccountKeystore.loadOrCreateIdentity(
                allowCreate: !Self.hasPriorAccount(in: dataDir))
        } catch {
            // An unreadable stored key (e.g. the enclave wrapping key is
            // gone) must not be a dead end: the Storage tab's Restore
            // flow still works from here and re-runs `start()`.
            keyProtection = AccountKeystore.currentProtection()
            status = .failed(error.localizedDescription)
            return
        }
        keyProtection = AccountKeystore.currentProtection()

        let path = dataDir.path
        let (raw, initError): (OpaquePointer?, String?) = await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let h = path.withCString { cpath in
                identity.withCString { cid in
                    ant_init_with_identity(cpath, nil, cid, &errPtr)
                }
            }
            // Keep the failure detail: `bind_account_state`'s deliberate
            // "refusing to start: … move one aside by hand" abort is an
            // instruction to the user, and `restoreAccount` rethrows this
            // status straight into the Restore sheet.
            let detail = h == nil ? errPtr.map { String(cString: $0) } : nil
            if let errPtr { ant_free_string(errPtr) }
            return (h, detail)
        }.value

        guard let raw else {
            status = .failed(initError ?? "could not start")
            return
        }
        handle = raw
        status = .ready
        startPollingPeers()
        startNetworkMonitor()
        await startGateway()
        await refreshAll()
    }

    func shutdown() async {
        await serialized { await self.performShutdown() }
    }

    private func performShutdown() async {
        guard let h = handle else { return }
        handle = nil
        peerPollTask?.cancel()
        pathMonitor?.cancel()
        pathMonitor = nil
        peerCount = 0
        gatewayUp = false
        // `handle` is nil from here, so no further call can borrow it —
        // wait for the ones already inside the node to come back before
        // `ant_shutdown` frees the memory they are running on.
        await drainBorrows()
        // Everything account-scoped goes too: after a restore the
        // Keychain holds the *new* account, and if the subsequent start
        // fails these would keep rendering the old account's plan,
        // chequebook and address as live — nothing refreshes them while
        // `handle == nil`. Cleared after the drain, because an in-flight
        // refresh publishes its (now stale) result before the drain
        // resumes us; a successful start repopulates via `refreshAll()`.
        plan = nil
        account = nil
        settlement = nil
        validity = nil
        await Task.detached(priority: .userInitiated) {
            _ = ant_stop_gateway(h)
            ant_shutdown(h)
        }.value
        status = .idle
    }

    /// Bring up the in-process bee-shaped HTTP gateway in `light_mode`
    /// (publish / feed / SOC writes allowed — exactly what broadcasting
    /// needs; ultra-light is read-only). Idempotent node-side.
    @discardableResult
    func startGateway() async -> Bool {
        let addr = Self.gatewayAddress
        let rpc = gnosisRpc
        guard let ok = await withHandle({ h in
            await Task.detached(priority: .userInitiated) { () -> Bool in
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let ok = addr.withCString { caddr in
                    rpc.withCString { crpc in
                        ant_start_gateway(h, caddr, true, crpc, &errPtr)
                    }
                }
                if !ok, let errPtr { ant_free_string(errPtr) }
                return ok
            }.value
        }) else { return false }
        gatewayUp = ok
        return ok
    }

    /// System-suspend before the app loses execution (backgrounding) or
    /// the network disappears. Returns once node state is checkpointed
    /// (bounded node-side at ~5 s), so run it inside a
    /// `beginBackgroundTask` window.
    func suspend() async {
        _ = await withHandle { h in
            await Task.detached(priority: .userInitiated) {
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                if ant_suspend(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
            }.value
        }
    }

    /// Undo `suspend()` on foreground / network-restored transitions.
    /// `ant_resume` re-warms the peer connections (after a long
    /// suspension the sockets are half-open) and `ant_wake` restarts the
    /// work the suspension paused.
    func wake() async {
        guard handle != nil else { return }
        _ = await withHandle { h in
            await Task.detached(priority: .userInitiated) {
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                if ant_resume(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
                var wakeErr: UnsafeMutablePointer<CChar>? = nil
                if ant_wake(h, &wakeErr) != 0, let wakeErr { ant_free_string(wakeErr) }
            }.value
        }
        // `ant_resume` recovers the swarm only: if the OS also tore down
        // the gateway's localhost listener it has to be rebound
        // separately (see ant.h). Re-binding a live gateway is a no-op
        // success, so this is safe on every foreground.
        await restartGateway()
    }

    private func restartGateway() async {
        guard handle != nil else { return }
        _ = await withHandle { h in
            await Task.detached(priority: .userInitiated) { _ = ant_stop_gateway(h) }.value
        }
        await startGateway()
    }

    /// Watch the device's network path. Offline ⇒ suspend (retries
    /// against a dead network only burn battery); back online ⇒ wake.
    private func startNetworkMonitor() {
        guard pathMonitor == nil else { return }
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] netPath in
            let offline = netPath.status != .satisfied
            let label = Self.interfaceLabel(for: netPath)
            Task { @MainActor [weak self] in
                guard let self else { return }
                if self.networkLabel != label { self.networkLabel = label }
                guard self.isOffline != offline else { return }
                self.isOffline = offline
                if offline {
                    await self.suspend()
                } else {
                    await self.wake()
                }
            }
        }
        monitor.start(queue: DispatchQueue(label: "antstream.netpath"))
        pathMonitor = monitor
    }

    /// `nonisolated`: this runs on `NWPathMonitor`'s queue, inside the
    /// path handler, before the hop back onto the main actor.
    private nonisolated static func interfaceLabel(for path: NWPath) -> String {
        guard path.status == .satisfied else { return "Offline" }
        if path.usesInterfaceType(.wifi) { return "Wi-Fi" }
        if path.usesInterfaceType(.cellular) { return "Cellular" }
        if path.usesInterfaceType(.wiredEthernet) { return "Wired" }
        return "Unknown"
    }

    // MARK: - Storage

    /// True once a storage plan is connected — gates broadcasting.
    var hasStorage: Bool { plan?.enabled == true }

    /// The checklist's first step: the node is up *and* actually talking
    /// to the network. `status.isReady` only says `ant_init` returned —
    /// it is true one line after launch, offline included — so a
    /// broadcast gate has to look at the live path and the peer count
    /// too, or a device in airplane mode reads as ready.
    var isOnNetwork: Bool { !isOffline && status.isReady && peerCount > 0 }

    /// Everything a first launch has to get through before the device can
    /// go live — the conjunction of the Broadcast tab's readiness
    /// checklist, row for row. Keep the two in step: this is the same
    /// gate capture + publish attach to, so anything the user sees
    /// unchecked must hold `isReadyToBroadcast` false.
    var isReadyToBroadcast: Bool {
        isOnNetwork && keyProtection != nil && hasStorage
            && settlement?.enabled == true && gatewayUp
    }

    /// Price a plan (no transaction). Returns the payment information.
    func quoteStorage(rpc: String, depth: UInt8, days: UInt64) async throws -> StorageQuote {
        let json = try await ffiString(name: "price plan") { h, errPtr in
            rpc.withCString { ant_storage_quote(h, $0, depth, days, errPtr) }
        }
        guard let q = StreamDecoder.quote(from: json) else {
            throw AntError.op("could not read plan price")
        }
        return q
    }

    /// Buy + activate a plan funding only with xDAI: the node swaps the
    /// xBZZ shortfall on-chain itself, then buys the plan. Spends real funds.
    func buyStorage(rpc: String, depth: UInt8, amountPerChunk: String, immutable: Bool) async throws {
        let json = try await ffiString(name: "activate storage") { h, errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_buy_xdai(h, crpc, depth, camt, immutable ? 1 : 0, errPtr)
                }
            }
        }
        if let p = StreamDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// Price extending the connected plan's lifetime by `days`.
    func quoteTopUp(rpc: String, days: UInt64) async throws -> StorageQuote {
        let json = try await ffiString(name: "price extension") { h, errPtr in
            rpc.withCString { ant_storage_topup_quote(h, $0, days, errPtr) }
        }
        guard let q = StreamDecoder.quote(from: json) else {
            throw AntError.op("could not read extension price")
        }
        return q
    }

    /// Extend the connected plan's lifetime, funding only with xDAI.
    /// Spends real funds. Publishes the refreshed validity.
    func topUpStorage(rpc: String, amountPerChunk: String) async throws {
        let json = try await ffiString(name: "extend storage") { h, errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_topup_xdai(h, crpc, camt, errPtr)
                }
            }
        }
        if let v = StreamDecoder.validity(from: json) { validity = v }
        await refreshAll()
    }

    /// Auto-discover every storage plan the account owns on Gnosis. Also
    /// the retry path for the one-time chequebook deploy, so a plan that
    /// activated without settlement can be repaired without `antctl`.
    func discoverStorage(rpc: String) async throws {
        _ = try await ffiString(name: "find storage") { h, errPtr in
            rpc.withCString { ant_storage_discover(h, $0, errPtr) }
        }
        await refreshAll()
    }

    // MARK: - Account

    func exportKey() async throws -> String {
        try await ffiString(name: "export key") { h, errPtr in
            ant_account_export_key(h, errPtr)
        }
    }

    /// Move the stored account key between iCloud-backed and device-bound
    /// protection. Takes effect at rest immediately; the running node is
    /// unaffected (it already holds the key in memory). Turning it off
    /// also removes the key from the user's *other* devices, so callers
    /// confirm first — see `AccountKeystore.setICloudBackup`.
    func setICloudBackup(_ enabled: Bool) throws {
        try AccountKeystore.setICloudBackup(enabled)
        keyProtection = AccountKeystore.currentProtection()
    }

    /// Replace the stored account key with a backed-up one. The node has
    /// to restart to adopt it, so we tear it down and start again.
    ///
    /// The restart is also what re-scopes the node's on-disk state to the
    /// restored account: `ant_init_with_identity` parks the previous
    /// account's postage batches, chequebook association and SWAP ledgers
    /// under `<data dir>/accounts/<its address>/` and swaps in whatever
    /// the restored account left behind (see `bind_account_state` in
    /// `ant-ffi`). Without that the checklist would keep reporting the
    /// old account's plan as active while every stamp the restored key
    /// signs over it is rejected by peers.
    ///
    /// Restoring is one transition, run on the lifecycle queue: a Restore
    /// tapped while the launch `start()` is still inside
    /// `ant_init_with_identity` has to wait for it rather than race a
    /// second init over the same data dir. The teardown additionally
    /// waits for any ordinary FFI call still inside the old node (see
    /// ``withHandle(_:)``), which the queue does not cover: an on-chain
    /// discover left running by a dismissed Connect sheet must not be
    /// executing on a handle `ant_shutdown` has freed.
    func restoreAccount(fromKey key: String) async throws {
        // Validate before queueing so a typo comes back straight away
        // instead of behind an in-flight start. Nothing is written yet.
        _ = try AccountKeystore.identity(fromAccountKey: key)
        var failure: Error?
        await serialized {
            do {
                _ = try AccountKeystore.restore(fromAccountKey: key)
            } catch {
                failure = error
                return
            }
            await self.performShutdown()
            // `performShutdown()` is a no-op for a node that never came
            // up, so reset the status by hand — otherwise the idle guard
            // would strand a `.failed` node (exactly the case Restore
            // exists for). Safe only because the queue guarantees no
            // other start is in flight.
            self.status = .idle
            await self.performStart(rpc: nil)
            if case .failed(let m) = self.status { failure = AntError.op(m) }
        }
        if let failure { throw failure }
    }

    // MARK: - Publisher throughput bench (#67 stage 1)

    /// Start a synthetic-segment publisher benchmark on the node.
    ///
    /// `batchId` selects the mode: the connected plan's batch publishes
    /// real segments through `POST /bzz` on the in-process gateway (the
    /// go/no-go measurement), `nil` measures only the local chunk +
    /// stamp pipeline, which needs neither network nor plan.
    ///
    /// The label carries the device and network the numbers describe —
    /// a row in the results table is meaningless without it.
    func startBench(
        label: String,
        bitrateKbps: UInt32,
        durationSeconds: UInt64,
        batchId: String?,
        notes: String
    ) async throws {
        let config: [String: Any] = [
            "label": label,
            "bitrate_kbps": bitrateKbps,
            "segment_ms": 2000,
            "duration_s": durationSeconds,
            // Long runs average over 30 s of peer-set warm-up; short
            // ones would have nothing left to measure.
            "warmup_s": durationSeconds > 120 ? 30 : 5,
            "max_in_flight": 4,
            "gateway": "http://\(Self.gatewayAddress)",
            "batch_id": batchId ?? "",
            "notes": notes,
        ]
        guard let data = try? JSONSerialization.data(withJSONObject: config),
              let json = String(data: data, encoding: .utf8) else {
            throw AntError.op("could not encode the bench configuration")
        }
        // `nil` from `withHandle` means there is no node; a non-nil
        // inner value is the failure detail (`nil` inside = started).
        let failure: String?? = await withHandle { h in
            await Task.detached(priority: .userInitiated) { () -> String? in
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let ok = json.withCString { ant_bench_start(h, $0, &errPtr) }
                // Keep the node's message: "a benchmark is already
                // running", "batch_id is not hex" and "invalid config"
                // are all things the operator has to act on.
                let detail = ok ? nil : errPtr.map { String(cString: $0) }
                if let errPtr { ant_free_string(errPtr) }
                return ok ? nil : (detail ?? "could not start the benchmark")
            }.value
        }
        guard let started = failure else { throw AntError.notReady }
        if let message = started { throw AntError.op(message) }
    }

    /// Live progress of the running benchmark, or `nil` when there is
    /// none (or the node went away underneath it).
    func benchProgress() async -> BenchSnapshot? {
        guard let json = try? await ffiString(name: "bench progress", { h, errPtr in
            ant_bench_progress(h, errPtr)
        }) else { return nil }
        return StreamDecoder.benchSnapshot(from: json)
    }

    /// Stop the benchmark and take its final report.
    ///
    /// `ant_bench_stop` blocks until the in-flight segments land (up to
    /// ~65 s), so this must not run on the main actor's thread —
    /// ``ffiString`` already hops onto a detached task for exactly that
    /// reason.
    func stopBench() async throws -> BenchReport {
        let json = try await ffiString(name: "stop benchmark") { h, errPtr in
            ant_bench_stop(h, errPtr)
        }
        guard let report = StreamDecoder.benchReport(from: json) else {
            throw AntError.op("could not read the benchmark report")
        }
        return report
    }

    // MARK: - Refresh

    func refreshAll() async {
        await refreshPlan()
        await refreshAccount()
        await refreshSettlement()
    }

    func refreshPlan() async {
        // Keep an installed screenshot sample from being clobbered by a
        // real read (which on a fresh runner account returns "no plan").
        if screenshotSampleActive { return }
        if let json = try? await ffiString(name: "storage status", { h, errPtr in
            ant_storage_status(h, errPtr)
        }) {
            let decoded = StreamDecoder.plan(from: json)
            // Re-check after the await: a sample may have been installed
            // while this read was in flight.
            if !screenshotSampleActive, decoded != plan { plan = decoded }
        }
    }

    func refreshAccount() async {
        if let json = try? await ffiString(name: "account info", { h, errPtr in
            ant_account_info(h, errPtr)
        }) {
            let decoded = StreamDecoder.account(from: json)
            if decoded != account { account = decoded }
        }
    }

    /// Fetch the connected plan's remaining lifetime from chain. Needs a
    /// Gnosis RPC URL; a no-op when none is set or no plan is connected.
    func refreshValidity(rpc: String) async {
        guard !rpc.isEmpty, hasStorage else { return }
        if let json = try? await ffiString(name: "storage validity", { h, errPtr in
            rpc.withCString { ant_storage_validity(h, $0, errPtr) }
        }), let v = StreamDecoder.validity(from: json) {
            validity = v
        }
    }

    func refreshSettlement() async {
        if screenshotSampleActive { return }
        if let json = try? await ffiString(name: "settlement status", { h, errPtr in
            ant_storage_settlement_status(h, errPtr)
        }) {
            let decoded = StreamDecoder.settlement(from: json)
            if !screenshotSampleActive, decoded != settlement { settlement = decoded }
        }
    }

    /// Read from chain how much xBZZ stands behind the chequebook. Needs a
    /// Gnosis RPC URL and a deployed chequebook; best-effort, so a flaky
    /// RPC leaves the previous value rather than clearing the card.
    func refreshSettlementDeposit(rpc: String) async {
        if screenshotSampleActive { return }
        guard !rpc.isEmpty, settlement?.enabled == true else { return }
        if let json = try? await ffiString(name: "settlement deposit", { h, errPtr in
            rpc.withCString { ant_storage_settlement_deposit(h, $0, errPtr) }
        }), let d = StreamDecoder.settlementDeposit(from: json) {
            settlementDeposit = d
        }
    }

    /// Fund the chequebook up to the settlement deposit, paying with xDAI
    /// (the node swaps for the xBZZ it needs). Spends real funds. Publishes
    /// the refreshed deposit so the card updates in place.
    func topUpSettlementDeposit(rpc: String) async throws {
        let json = try await ffiString(name: "top up settlement deposit") { h, errPtr in
            rpc.withCString { ant_storage_settlement_topup(h, $0, errPtr) }
        }
        if let d = StreamDecoder.settlementDeposit(from: json) { settlementDeposit = d }
        await refreshAll()
    }

    // MARK: - Screenshot hooks (antstream-visual CI)

    /// Set once a screenshot sample has been installed, so the periodic /
    /// scene-phase refreshes that would otherwise reset `plan` and the
    /// settlement fields to their real (empty) runner state leave the
    /// sample in place for the capture. Never set outside the shot path.
    private var screenshotSampleActive = false

    /// Drive the Storage tab to the deposit-0 top-up state for a CI
    /// screenshot.
    ///
    /// The settlement-deposit card is the migration surface for every
    /// pre-funding install: a chequebook that is deployed but holds no
    /// xBZZ. It renders only when a plan is connected *and* the on-chain
    /// deposit read comes back under target — and a fresh simulator
    /// account has neither a plan nor a chequebook, so that state can't be
    /// reached from real state on the runner. This publishes representative
    /// sample values so `antstream-visual` can capture the card.
    ///
    /// Sample only, and only ever called behind `-antstream-shot-deposit`:
    /// it moves no funds and is never reached in normal use. The account
    /// address the card copies is left as the node's real one (set by
    /// `refreshAccount`), so only the settlement figures are synthetic.
    func installDepositTopUpSample() {
        screenshotSampleActive = true
        // A modest connected plan so `plan.enabled` gates the card in, and
        // the storage meter shows a populated bar rather than "no plan".
        plan = StoragePlan(
            enabled: true,
            batchId: "0x0000000000000000000000000000000000000000000000000000000000000000",
            batchDepth: 22,
            immutable: false,
            totalCapacityChunks: 1_250_000,   // ~5 GB at 4 KiB/chunk
            issuedChunks: 40_000,             // ~160 MB used
            worstCaseRemainingChunks: 1_210_000
        )
        // A deployed chequebook (settlement enabled) …
        settlement = SettlementInfo(
            enabled: true,
            chequebook: "0x370e6965000000000000000000000000000000e1"
        )
        // … that is empty: the exact deposit-0 branch #73 fixes, and the
        // figures the read returns for it (target 0.0010 xBZZ, all of it
        // missing, ~0.0150 xDAI to fund it — matching the PR's verified
        // on-chain probe of a real unfunded chequebook).
        settlementDeposit = SettlementDeposit(
            enabled: true,
            chequebook: "0x370e6965000000000000000000000000000000e1",
            depositBzz: "0.0000",
            targetBzz: "0.0010",
            shortfallBzz: "0.0010",
            needsTopUp: true,
            xdaiToSendDisplay: "0.0150",
            sufficientFunds: false
        )
    }

    // MARK: - Polling

    private func startPollingPeers() {
        peerPollTask?.cancel()
        guard handle != nil else { return }
        peerPollTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                // Re-read the handle every tick rather than capturing it
                // once: a shutdown clears it, and `ant_peer_count` is a
                // cheap non-blocking read, so doing it inline on the main
                // actor keeps it in the same atomic step as that check —
                // no suspension in between for a shutdown to slip into.
                guard let self, let h = self.handle else { return }
                // Publish only real changes: an unconditional re-assign
                // fires objectWillChange every second and re-renders
                // every observing view.
                let count = max(0, Int(ant_peer_count(h)))
                if self.peerCount != count { self.peerCount = count }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    // MARK: - FFI string helper

    /// Run an FFI call that returns an owned C string + writes an error
    /// string into its `out_err` slot. Frees both. Hops onto a detached
    /// task so the (potentially blocking) Rust call never stalls the UI.
    private static func string(
        name: String,
        _ body: @escaping (UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) async throws -> String {
        try await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if let raw = body(&errPtr) {
                defer { ant_free_string(raw) }
                return String(cString: raw)
            }
            let msg = errPtr.flatMap { String(cString: $0) } ?? "\(name) failed"
            if let errPtr { ant_free_string(errPtr) }
            throw AntError.op(msg)
        }.value
    }

    // MARK: - Data dir / peer seed

    private static func resolveDataDir() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("antstream", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    /// Has an account already run against this data dir? `ant-ffi` writes
    /// `account.json` (the public address only) on every start, so its
    /// presence means an earlier account's postage / chequebook / SWAP
    /// state is sitting at the canonical paths. A reinstall takes the
    /// container with it, so this is `false` on a real first launch.
    private static func hasPriorAccount(in dataDir: URL) -> Bool {
        FileManager.default.fileExists(
            atPath: dataDir.appendingPathComponent("account.json").path)
    }

    private static func seedPeerstoreIfNeeded(in dataDir: URL) {
        let dest = dataDir.appendingPathComponent("peers.json")
        if FileManager.default.fileExists(atPath: dest.path),
           let data = try? Data(contentsOf: dest),
           let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           let peers = obj["peers"] as? [Any], !peers.isEmpty {
            return
        }
        guard let bundled = Bundle.main.url(forResource: "peers.seed", withExtension: "json") else { return }
        try? FileManager.default.removeItem(at: dest)
        try? FileManager.default.copyItem(at: bundled, to: dest)
    }
}

enum AntError: LocalizedError {
    case notReady
    case op(String)

    var errorDescription: String? {
        switch self {
        case .notReady: return "AntStream is still starting up"
        case .op(let m): return m
        }
    }
}
