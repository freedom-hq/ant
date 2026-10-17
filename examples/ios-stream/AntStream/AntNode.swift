import Foundation
import Network
import SwiftUI
import UIKit

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
///   (`ant_start_gateway`, `light_mode`) on localhost, falling back
///   through ports 1634…1643 when 1633 is taken. That is the surface the
///   publisher (#67) pushes segments and feed updates through, and the
///   Watch tab plays channels from.
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
    /// The live chequebook view, refreshed every 2 s (`refreshCredit`).
    @Published private(set) var credit: Credit?
    /// The account wallet's xBZZ and xDAI, read from chain
    /// (`refreshWallet`); `nil` until then.
    @Published private(set) var wallet: WalletBalance?
    /// Why the last wallet read failed; `nil` once one succeeds.
    @Published private(set) var walletError: String?
    private var sessionIssued: Decimal?
    private var issuedSamples: [(at: Date, plur: Decimal)] = []
    private var lastSent: [String: Decimal]?
    private var recentPayments: [Payment] = []
    private var creditRefreshing = false
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
    /// Why the gateway is down, when binding failed (another app holds
    /// every port in the fallback range). Shown in the checklist; the
    /// peer poll retries every 10 s while it is set.
    @Published private(set) var gatewayError: String?
    /// Where the in-process bee gateway listens: 127.0.0.1:1633, or the
    /// next free port up to 1643 when another app (another Swarm node, a
    /// Freedom install) already holds it. Remembered, so a restart lands
    /// on the port that worked last time.
    @Published private(set) var gatewayAddress: String =
        UserDefaults.standard.string(forKey: AntNode.gatewayAddressKey) ?? AntNode.defaultGatewayAddress
    /// The account's permanent channel; `nil` until the account is known.
    @Published private(set) var channel: Channel?
    /// A broadcast this account was in the middle of when the app was
    /// killed or crashed (#175), read from its recovery entry; `nil` when
    /// there is none, or while this launch is broadcasting.
    @Published private(set) var unfinishedBroadcast: UnfinishedBroadcast?
    /// A broadcast started on this node and its stop has not returned:
    /// the recovery entry on disk is its own, not an unfinished one.
    private var publisherActive = false
    /// The account's stream info (#220), remembered per account for its
    /// next broadcast: what its catalog entry says about the channel.
    @Published private(set) var streamInfo = StreamInfo()
    /// The thumbnail image on this device (a JPEG), chosen or captured;
    /// `nil` without one.
    @Published private(set) var thumbnailJPEG: Data?
    /// The thumbnail is going up.
    @Published private(set) var thumbnailUploading = false
    /// Why the last thumbnail upload or catalog update failed; `nil` once
    /// one succeeds. Never a broadcast failure.
    @Published private(set) var streamInfoError: String?
    /// ``announceBroadcast()`` is writing the catalog entry. Lives here,
    /// not in the editor that started it: the write outlives that sheet,
    /// and the library refuses `ant_publisher_start` until it returns, so
    /// every Go live (the Broadcast tab's and the live screen's) is held
    /// on it and says why.
    @Published private(set) var announcingBroadcast = false
    /// ``finishUnfinishedBroadcast()`` is running: the publisher is that
    /// broadcast, so ``unfinishedBroadcast`` stays up (its card shows the
    /// finalize in progress, and Go live stays held) until the stop
    /// returns and the entry on disk says how it went.
    private var finalizingUnfinished = false

    /// The preferred gateway address and the start of the fallback range
    /// (the same range Freedom desktop uses).
    static let defaultGatewayPort = 1633
    static let defaultGatewayAddress = "127.0.0.1:\(defaultGatewayPort)"
    private static let gatewayAddressKey = "antstream.gatewayAddress"
    /// The gateway's base URL. Follows ``gatewayAddress``, so everything
    /// that talks to the gateway (publisher, bench, player) must read it
    /// here rather than assume port 1633.
    var gatewayURL: URL? { URL(string: "http://\(gatewayAddress)") }
    /// Set while ``startGateway()`` walks the port range, so the periodic
    /// retry never runs two binds at once.
    private var gatewayBinding = false

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
        settlementDeposit = nil
        wallet = nil
        walletError = nil
        credit = nil
        sessionIssued = nil
        issuedSamples = []
        lastSent = nil
        recentPayments = []
        validity = nil
        channel = nil
        streamInfo = StreamInfo()
        thumbnailJPEG = nil
        streamInfoError = nil
        await Task.detached(priority: .userInitiated) {
            _ = ant_stop_gateway(h)
            ant_shutdown(h)
        }.value
        status = .idle
    }

    /// Bring up the in-process bee-shaped HTTP gateway in `light_mode`
    /// (publish / feed / SOC writes allowed — exactly what broadcasting
    /// needs; ultra-light is read-only). Idempotent node-side.
    ///
    /// Tries the remembered address first, then 127.0.0.1:1633…1643:
    /// another app holding 1633 must not leave the device unable to
    /// broadcast. The address that worked is published as
    /// ``gatewayAddress`` and remembered for the next start; when every
    /// port fails the last bind error is kept in ``gatewayError``.
    @discardableResult
    func startGateway() async -> Bool {
        guard !gatewayBinding else { return gatewayUp }
        gatewayBinding = true
        defer { gatewayBinding = false }
        let rpc = gnosisRpc
        var candidates = [gatewayAddress]
        for port in Self.defaultGatewayPort...(Self.defaultGatewayPort + 10) {
            let addr = "127.0.0.1:\(port)"
            if !candidates.contains(addr) { candidates.append(addr) }
        }
        var lastError: String?
        for addr in candidates {
            // `nil` from `withHandle` means there is no node; a non-nil
            // inner value is the bind failure (`nil` inside = listening).
            let outcome: String?? = await withHandle { h in
                await Task.detached(priority: .userInitiated) { () -> String? in
                    var errPtr: UnsafeMutablePointer<CChar>? = nil
                    let ok = addr.withCString { caddr in
                        rpc.withCString { crpc in
                            ant_start_gateway(h, caddr, true, crpc, &errPtr)
                        }
                    }
                    let detail = ok ? nil : (errPtr.map { String(cString: $0) } ?? "could not start")
                    if let errPtr { ant_free_string(errPtr) }
                    return detail
                }.value
            }
            guard let result = outcome else { break }
            if let failure = result {
                lastError = failure
                continue
            }
            if gatewayAddress != addr {
                gatewayAddress = addr
                UserDefaults.standard.set(addr, forKey: Self.gatewayAddressKey)
            }
            gatewayError = nil
            gatewayUp = true
            return true
        }
        gatewayError = handle == nil ? nil : (lastError ?? "could not start")
        gatewayUp = false
        return false
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
        guard handle != nil, !gatewayBinding else { return }
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
                // A broadcast in progress pauses rather than ends (#172):
                // silent while the network is gone, and carried on in
                // the same feed and recording behind one seam when it
                // returns. Resumed after `wake`, so the swarm is warm
                // again before the reap window restarts, or before a
                // reap that fell due offline uploads the closing
                // playlist and the recording.
                if offline {
                    await self.pausePublisher()
                    await self.suspend()
                } else {
                    await self.wake()
                    // `wake` suspends: the path may have dropped again
                    // meanwhile, and that offline pass has already
                    // paused the run. Resuming now would unpause it
                    // against a dead uplink (and reap there, if a reap
                    // fell due) — leave it for the next return.
                    if !self.isOffline { await self.resumePublisher() }
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
    ///
    /// The count is `minPeersToBroadcast`, not one: a broadcast reads
    /// its channel's feed head before its first playlist (#173), and the
    /// node only confirms a miss there (a new channel's empty feed, or
    /// the slot after the head) when more than two peers answer it, so
    /// with one or two peers it would upload segments but publish no
    /// playlist.
    var isOnNetwork: Bool {
        !isOffline && status.isReady && peerCount >= Self.minPeersToBroadcast
    }

    /// Peers the node needs before going live; see `isOnNetwork`.
    static let minPeersToBroadcast = 3

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
            "gateway": "http://\(gatewayAddress)",
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

    // MARK: - Live publisher (#67 stage 2)

    /// Start a live broadcast on the node. `batchId` is the connected
    /// plan's postage batch — every segment, playlist and feed update is
    /// stamped with it, so a broadcast without a plan is refused here
    /// rather than failing per segment on the gateway.
    ///
    /// The publish window is left at the node's default (4): stage 1
    /// measured 4 as the stable point and 8 as a connection-layer
    /// collapse, so it is not a knob this screen should offer.
    ///
    /// `feed` is ``BroadcastFeed/current`` for every new broadcast: the
    /// Solar Punk format on the account's `antstream` topic, so the
    /// channel is the account's address and that topic, the same every
    /// time. No `start_index` is passed: the node reads the feed head and
    /// continues after it (#173), with the media sequence numbered on
    /// from the previous broadcast and the recording opening with it, so
    /// an index this app lost track of (a reinstall, another device on
    /// the same account) can't rewrite earlier broadcasts. Ending an
    /// unfinished broadcast passes the feed its recovery entry names
    /// instead (``finishUnfinishedBroadcast()``).
    ///
    /// Every broadcast keeps a recovery entry (`recovery_path`, #175) in
    /// the account's file under `Application Support/antstream/`, so a
    /// kill or crash leaves what the next launch needs to carry it on
    /// (`resume`) or end it into its recording
    /// (``finishUnfinishedBroadcast()``). The node removes it once the
    /// recording is the feed's head.
    func startPublisher(
        channel: String,
        feed: BroadcastFeed = .current,
        owner: String,
        batchId: String,
        bitrateKbps: UInt32,
        segmentMs: UInt32,
        notes: String,
        resume: Bool = false,
        info: StreamInfo? = nil,
        width: Int = 0,
        height: Int = 0
    ) async throws {
        // `LiveBroadcast` notices a reap's `reaped` flag and finishes the
        // stop.
        let config = feed.publisherConfig(
            channel: channel,
            gateway: "http://\(gatewayAddress)",
            batchId: batchId,
            bitrateKbps: bitrateKbps,
            segmentMs: segmentMs,
            notes: notes,
            recoveryPath: Self.recoveryURL(owner: owner).path,
            resume: resume,
            reapMs: Self.reapMsForThisLaunch,
            info: info,
            width: width,
            height: height
        )
        guard let data = try? JSONSerialization.data(withJSONObject: config),
              let json = String(data: data, encoding: .utf8) else {
            throw AntError.op("could not encode the broadcast configuration")
        }
        // `nil` from `withHandle` means there is no node; a non-nil
        // inner value is the failure detail (`nil` inside = started).
        let failure: String?? = await withHandle { h in
            await Task.detached(priority: .userInitiated) { () -> String? in
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let ok = json.withCString { ant_publisher_start(h, $0, &errPtr) }
                let detail = ok ? nil : errPtr.map { String(cString: $0) }
                if let errPtr { ant_free_string(errPtr) }
                return ok ? nil : (detail ?? "could not start the broadcast")
            }.value
        }
        guard let started = failure else { throw AntError.notReady }
        if let message = started { throw AntError.op(message) }
        publisherActive = true
        if !finalizingUnfinished { unfinishedBroadcast = nil }
    }

    // MARK: - Stream info (#220)

    /// What the stream info editor does to the thumbnail.
    enum ThumbnailEdit {
        case keep
        case remove
        /// A photo the broadcaster chose, already scaled down to a JPEG.
        case picked(Data)
    }

    /// A broadcast started on this node is running (its stop has not
    /// returned): stream info edits go to it at once.
    var broadcastRunning: Bool { publisherActive }

    /// `UserDefaults` key of `owner`'s stream info.
    private static func streamInfoKey(owner: String) -> String {
        "antstream.streamInfo." + owner.lowercased().replacingOccurrences(of: "0x", with: "")
    }

    /// Where `owner`'s thumbnail image is kept, next to its recovery entry.
    static func thumbnailURL(owner: String) -> URL {
        let hex = owner.lowercased().replacingOccurrences(of: "0x", with: "")
        return resolveDataDir().appendingPathComponent("stream-thumbnail-\(hex).jpg")
    }

    /// Read the account's stream info and thumbnail (a new account starts
    /// with none). Nothing while a screenshot sample is showing.
    private func loadStreamInfo() {
        if streamInfoSampleActive { return }
        guard let owner = channel?.owner else { return }
        let saved = UserDefaults.standard.data(forKey: Self.streamInfoKey(owner: owner))
            .flatMap { try? JSONDecoder().decode(StreamInfo.self, from: $0) }
        streamInfo = saved ?? StreamInfo()
        thumbnailJPEG = try? Data(contentsOf: Self.thumbnailURL(owner: owner))
        streamInfoError = nil
    }

    /// Publish `info` and remember it for the account.
    private func storeStreamInfo(_ info: StreamInfo) {
        if info != streamInfo { streamInfo = info }
        guard !streamInfoSampleActive, let owner = channel?.owner,
              let data = try? JSONEncoder().encode(info)
        else { return }
        UserDefaults.standard.set(data, forKey: Self.streamInfoKey(owner: owner))
    }

    /// Replace (or with `nil`, remove) the thumbnail image on the device.
    private func storeThumbnail(_ jpeg: Data?) {
        thumbnailJPEG = jpeg
        guard !streamInfoSampleActive, let owner = channel?.owner else { return }
        let url = Self.thumbnailURL(owner: owner)
        if let jpeg {
            try? jpeg.write(to: url, options: .atomic)
        } else {
            try? FileManager.default.removeItem(at: url)
        }
    }

    /// Save what the stream info editor says. A chosen photo goes up
    /// straight away when there is a storage plan to stamp it with, else
    /// at the next Go live; a running broadcast gets the new info at once
    /// (`ant_publisher_set_info`). Both run in the background (`sync`),
    /// so the editor can close at once.
    func saveStreamInfo(
        title: String,
        description: String,
        scheduledStart: Date?,
        thumbnail: ThumbnailEdit,
        sync: Bool = true
    ) {
        var info = streamInfo
        info.title = title.trimmingCharacters(in: .whitespacesAndNewlines)
        info.description = description.trimmingCharacters(in: .whitespacesAndNewlines)
        info.scheduledStartMs = scheduledStart.map { UInt64(max(0, $0.timeIntervalSince1970) * 1000) }
        switch thumbnail {
        case .keep:
            break
        case .remove:
            storeThumbnail(nil)
            info.thumbnail = ""
            info.thumbnailPicked = false
            info.thumbnailPending = false
        case .picked(let jpeg):
            storeThumbnail(jpeg)
            info.thumbnail = ""
            info.thumbnailPicked = true
            info.thumbnailPending = true
        }
        storeStreamInfo(info)
        guard sync else { return }
        Task { await self.syncStreamInfo(pushToBroadcast: true) }
    }

    /// A frame the broadcast captured from the camera shortly after Go
    /// live: the thumbnail, unless the broadcaster has chosen one (checked
    /// again here, as a photo may have been chosen meanwhile). Uploaded
    /// and handed to the running broadcast.
    func useCapturedThumbnail(_ jpeg: Data) async {
        guard !streamInfo.thumbnailPicked, channel != nil else { return }
        storeThumbnail(jpeg)
        var info = streamInfo
        info.thumbnail = ""
        info.thumbnailPending = true
        storeStreamInfo(info)
        await syncStreamInfo(pushToBroadcast: true)
    }

    /// Bring Swarm up to date with the stream info: upload a thumbnail
    /// image that is not up yet (when there is a plan to stamp it with),
    /// then, with `pushToBroadcast` or after an upload, hand the info to
    /// a running broadcast. Best effort: a failure shows in
    /// ``streamInfoError`` and the thumbnail stays pending for the next
    /// try (the next save or Go live).
    func syncStreamInfo(pushToBroadcast: Bool) async {
        var uploaded = false
        if !thumbnailUploading {
            thumbnailUploading = true
            // Again while the image changed under an upload (a new photo,
            // a new frame): the reference must be the image on the device.
            while streamInfo.thumbnailPending, let jpeg = thumbnailJPEG, hasStorage {
                let owner = channel?.owner
                do {
                    let reference = try await uploadThumbnail(jpeg)
                    guard channel?.owner == owner else { break }
                    if thumbnailJPEG == jpeg, streamInfo.thumbnailPending {
                        var info = streamInfo
                        info.thumbnail = reference
                        info.thumbnailPending = false
                        storeStreamInfo(info)
                        streamInfoError = nil
                        uploaded = true
                    }
                } catch {
                    streamInfoError = "Thumbnail not uploaded: \(error.localizedDescription)"
                    break
                }
            }
            thumbnailUploading = false
        }
        // Not while an unfinished broadcast is being ended: that run is
        // the old broadcast, and its closing entry keeps its own info.
        guard (pushToBroadcast || uploaded), publisherActive, !finalizingUnfinished else { return }
        do {
            try await setPublisherInfo(streamInfo.forBroadcast)
        } catch {
            streamInfoError = "Stream info not updated: \(error.localizedDescription)"
        }
    }

    /// Hand the running broadcast new stream info
    /// (`ant_publisher_set_info`): its catalog entry is rewritten at once.
    /// Non-blocking node-side.
    func setPublisherInfo(_ info: StreamInfo) async throws {
        guard let json = info.infoJSON() else {
            throw AntError.op("could not encode the stream info")
        }
        // `nil` from `withHandle` means there is no node; a non-nil
        // inner value is the failure detail (`nil` inside = updated).
        let failure: String?? = await withHandle { h in
            await Task.detached(priority: .userInitiated) { () -> String? in
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let code = json.withCString { ant_publisher_set_info(h, $0, &errPtr) }
                let detail = code == 0 ? nil : errPtr.map { String(cString: $0) }
                if let errPtr { ant_free_string(errPtr) }
                return code == 0 ? nil : (detail ?? "could not update the stream info")
            }.value
        }
        guard let outcome = failure else { throw AntError.notReady }
        if let message = outcome { throw AntError.op(message) }
    }

    /// Upload a thumbnail image (`ant_stream_thumbnail_upload`, one
    /// `POST /bzz` through the in-process gateway, stamped with the
    /// connected plan's batch) and return its reference for
    /// ``StreamInfo/thumbnail``. Blocks for the upload, off the main
    /// thread (``ffiString``).
    func uploadThumbnail(_ jpeg: Data) async throws -> String {
        guard let batchId = plan?.batchId, plan?.enabled == true, !batchId.isEmpty else {
            throw AntError.op("Connect a storage plan to upload a thumbnail.")
        }
        let gateway = "http://\(gatewayAddress)"
        let reference = try await ffiString(name: "upload thumbnail") { h, errPtr in
            jpeg.withUnsafeBytes { raw -> UnsafeMutablePointer<CChar>? in
                guard let base = raw.bindMemory(to: UInt8.self).baseAddress else { return nil }
                return gateway.withCString { cgw in
                    batchId.withCString { cbatch in
                        ant_stream_thumbnail_upload(h, cgw, cbatch, base, raw.count, errPtr)
                    }
                }
            }
        }
        let trimmed = reference.trimmingCharacters(in: .whitespacesAndNewlines)
        guard StreamInfo.isReference(trimmed) else {
            throw AntError.op("the thumbnail upload returned no reference")
        }
        return trimmed.lowercased()
    }

    /// Why Go live is held while ``announcingBroadcast``: the Broadcast
    /// tab's and the live screen's note.
    static let announcingHoldNote =
        "An announcement is still being written to the catalog. Go live once it's done; it can take a few minutes on a slow network."

    /// Announce the scheduled broadcast (`ant_stream_catalog_publish`):
    /// the channel's catalog entry becomes a "scheduled" one with the
    /// stream info and its start, so players list it before it goes live.
    /// Not while a broadcast runs (that one owns the catalog). Uploads a
    /// thumbnail that is not up yet first. Blocks for the catalog write,
    /// off the main thread; throws the library's message.
    func announceBroadcast() async throws {
        guard !publisherActive else {
            throw AntError.op("A broadcast is running: its catalog entry already says it is live.")
        }
        guard streamInfo.scheduledStartMs != nil else {
            throw AntError.op("Pick a start time to announce.")
        }
        guard let channel else { throw AntError.notReady }
        guard let batchId = plan?.batchId, plan?.enabled == true, !batchId.isEmpty else {
            throw AntError.op("Connect a storage plan to announce a broadcast: the announcement is an upload.")
        }
        guard !announcingBroadcast else {
            throw AntError.op("An announcement is already being written.")
        }
        announcingBroadcast = true
        defer { announcingBroadcast = false }
        await syncStreamInfo(pushToBroadcast: false)
        let info = streamInfo
        var settings = CaptureEngine.Settings(quality: LiveBroadcast.storedQuality)
        settings.portrait = LiveBroadcast.storedPortrait(owner: channel.owner)
        let config = info.catalogPublishConfig(
            gateway: "http://\(gatewayAddress)",
            batchId: batchId,
            channel: channel.name ?? UIDevice.current.name,
            width: settings.encodedWidth,
            height: settings.encodedHeight,
            // The video bitrate, as the broadcast's own config gives it
            // (and Android's announcement): the rung an announcement
            // promises is the one the live entry then starts with.
            bitrateKbps: settings.bitrateKbps
        )
        guard let data = try? JSONSerialization.data(withJSONObject: config),
              let json = String(data: data, encoding: .utf8) else {
            throw AntError.op("could not encode the announcement")
        }
        let result = try await ffiString(name: "announce broadcast") { h, errPtr in
            json.withCString { ant_stream_catalog_publish(h, $0, errPtr) }
        }
        guard let object = (result.data(using: .utf8))
                .flatMap({ try? JSONSerialization.jsonObject(with: $0) as? [String: Any] }),
              object["entry"] is [String: Any]
        else {
            throw AntError.op("could not read the announcement's result")
        }
        // The account may have changed while the write was out, or the
        // start been edited: remember only what was announced.
        guard self.channel?.owner == channel.owner else { return }
        var announced = streamInfo
        if announced.scheduledStartMs == info.scheduledStartMs {
            announced.announcedStartMs = info.scheduledStartMs
            storeStreamInfo(announced)
        }
    }

    /// A broadcast went live with the stream info: the announcement it
    /// fulfilled is done with, and a thumbnail chosen while there was no
    /// plan (or whose upload failed) goes up now.
    func streamInfoWentLive() {
        if streamInfo.scheduledStartMs != nil || streamInfo.announcedStartMs != nil {
            storeStreamInfo(streamInfo.forBroadcast)
        }
        if streamInfo.thumbnailPending {
            Task { await self.syncStreamInfo(pushToBroadcast: false) }
        }
    }

    // MARK: - Storage room for a broadcast (#176)

    /// Estimates already worked out for ``capacityCachePlan``, by
    /// quality: the views ask on every redraw. Emptied whenever the plan
    /// changes (mid-broadcast its room shrinks with every refresh), so it
    /// never holds more than one plan's few qualities. Recomputing after
    /// a change is cheap: the library remembers the broadcast's cost per
    /// config, so only the room changes.
    private var capacityCache: [CaptureEngine.Quality: CapacityEstimate] = [:]
    private var capacityCachePlan: StoragePlan?

    /// How long a broadcast at `quality` fits on the connected plan
    /// (#176), from the library's `ant_publisher_capacity`: what a minute
    /// of it stamps (segments at the video and audio rate, the playlist
    /// and feed update after each, the ladder markers, the recording)
    /// against the room left in the plan's fullest bucket. `nil` with no
    /// plan. Needs no node; the same format and ladder setting
    /// ``startPublisher`` sends for a new broadcast.
    func capacity(for quality: CaptureEngine.Quality) -> CapacityEstimate? {
        guard let plan, plan.enabled, !plan.batchId.isEmpty else { return nil }
        if capacityCachePlan != plan {
            capacityCache = [:]
            capacityCachePlan = plan
        }
        if let cached = capacityCache[quality] { return cached }
        let settings = CaptureEngine.Settings(quality: quality)
        let config: [String: Any] = [
            "channel": "",
            "batch_id": plan.batchId,
            "bitrate_kbps": settings.bitrateKbps + CaptureEngine.audioBitrateKbps,
            "segment_ms": Int(settings.segmentSeconds * 1000),
            "format": BroadcastFeed.current.format,
            "ladder": BroadcastFeed.ladder,
        ]
        guard let configData = try? JSONSerialization.data(withJSONObject: config),
              let configJSON = String(data: configData, encoding: .utf8),
              let planData = try? JSONEncoder().encode(plan),
              let planJSON = String(data: planData, encoding: .utf8)
        else { return nil }
        var errPtr: UnsafeMutablePointer<CChar>? = nil
        let raw = configJSON.withCString { configPtr in
            planJSON.withCString { planPtr in
                ant_publisher_capacity(configPtr, planPtr, &errPtr)
            }
        }
        if let errPtr { ant_free_string(errPtr) }
        guard let raw else { return nil }
        defer { ant_free_string(raw) }
        guard let estimate = StreamDecoder.capacity(from: String(cString: raw)) else { return nil }
        capacityCache[quality] = estimate
        return estimate
    }

    // MARK: - Unfinished broadcast (#175)

    /// Where the publisher keeps `owner`'s recovery entry: one file per
    /// account, so a restored or switched account never offers another
    /// account's broadcast (the node refuses to resume one anyway).
    static func recoveryURL(owner: String) -> URL {
        let hex = owner.lowercased().replacingOccurrences(of: "0x", with: "")
        return resolveDataDir().appendingPathComponent("broadcast-recovery-\(hex).json")
    }

    /// Re-read this account's recovery entry. Nothing while this launch
    /// is broadcasting (the entry is the running broadcast's own) or the
    /// account is not known yet. An entry that can't be read is left on
    /// disk and not offered: the node could not resume it either.
    func refreshUnfinishedBroadcast() {
        if unfinishedSampleActive || finalizingUnfinished { return }
        guard !publisherActive, let owner = channel?.owner else {
            if unfinishedBroadcast != nil { unfinishedBroadcast = nil }
            return
        }
        let url = Self.recoveryURL(owner: owner)
        let hex = owner.lowercased().replacingOccurrences(of: "0x", with: "")
        let entry = (try? Data(contentsOf: url)).flatMap(UnfinishedBroadcast.decode)
        let found = entry?.owner == hex ? entry : nil
        if found != unfinishedBroadcast { unfinishedBroadcast = found }
    }

    /// End the unfinished broadcast without going live again: the node
    /// resumes it and stops straight away, so its closing window and
    /// recording go up as on any stop and the channel's head plays every
    /// segment that landed before the app went. Blocks like a stop.
    func finishUnfinishedBroadcast() async throws -> PublisherReport {
        guard let unfinished = unfinishedBroadcast, let channel else {
            throw AntError.op("There is no unfinished broadcast to end.")
        }
        guard let batchId = plan?.batchId, plan?.enabled == true else {
            throw AntError.op("Connect a storage plan to end the broadcast: its recording is an upload.")
        }
        let settings = CaptureEngine.Settings(quality: .low)
        finalizingUnfinished = true
        defer {
            // Once the stop returned (or the start failed), the entry on
            // disk decides: gone once the recording landed, kept to retry.
            finalizingUnfinished = false
            refreshUnfinishedBroadcast()
        }
        // The entry's own feed and format: one left by a build from before
        // the switch to "solar-punk" (#217) is still ended where it went
        // out, as the node requires.
        try await startPublisher(
            channel: unfinished.channel,
            feed: unfinished.feed,
            owner: channel.owner,
            batchId: batchId,
            bitrateKbps: UInt32(settings.bitrateKbps),
            segmentMs: UInt32(settings.segmentSeconds * 1000),
            notes: "ending an unfinished broadcast",
            resume: true,
            // No info: the node takes the info the recovery entry saved,
            // so the closing `vod` entry over the killed run's `live` one
            // (#220) names that broadcast, not whatever the editor holds
            // for the next one. The rung's size comes from there too.
            info: nil
        )
        return try await stopPublisher()
    }

    /// Forget the unfinished broadcast: its segments stay on Swarm and
    /// the channel keeps its last live window, but it is never closed or
    /// recorded. For an entry the node refuses (the channel moved on).
    func discardUnfinishedBroadcast() {
        guard let owner = channel?.owner else { return }
        try? FileManager.default.removeItem(at: Self.recoveryURL(owner: owner))
        unfinishedSampleActive = false
        unfinishedBroadcast = nil
    }

    /// How long a broadcast outlives its media before it ends by itself
    /// (`reap_ms`, issue #172): Solar Punk's `ORPHAN_REAP_MS`.
    static let broadcastReapMs: UInt64 = 60_000

    /// ``broadcastReapMs``, except under `antstream-visual`'s plain
    /// `-antstream-shot-live`: the runner's sample batch is rejected, so
    /// nothing is ever listed, and a minute's reap would end the
    /// broadcast before shots 11/12 capture it live. The paused run
    /// (`-antstream-shot-live-paused`) keeps the real window so shot 19
    /// shows the reap.
    static var reapMsForThisLaunch: UInt64 {
        let args = RootView.shotArgs
        if args.contains("-antstream-shot-live"), !args.contains("-antstream-shot-live-paused") {
            return 600_000
        }
        return broadcastReapMs
    }

    /// The network went: pause the running broadcast (`ant_publisher_pause`)
    /// so it goes silent instead of filling with failed uploads. A no-op
    /// when nothing is broadcasting.
    func pausePublisher() async {
        _ = await withHandle { h in
            await Task.detached(priority: .userInitiated) {
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                if ant_publisher_pause(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
            }.value
        }
    }

    /// The network (or the app) is back: carry the paused broadcast on
    /// (`ant_publisher_resume`). Also called on every return to the
    /// foreground, which restarts the reap window once. A no-op when
    /// nothing is broadcasting.
    func resumePublisher() async {
        _ = await withHandle { h in
            await Task.detached(priority: .userInitiated) {
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                if ant_publisher_resume(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
            }.value
        }
    }

    /// Hand one captured segment to the publisher. Returns the FFI's
    /// disposition: `0` queued, `1` queued after dropping the oldest
    /// pending segment, `2` refused (stopping, or ended by itself),
    /// `3` withheld (paused: the network is gone), `-1` error.
    ///
    /// Non-blocking node-side, so the capture pipeline is never stalled
    /// by the uplink — which is the whole point of the drop-oldest
    /// backlog behind it.
    @discardableResult
    func pushSegment(_ segment: CaptureEngine.Segment) async -> Int32 {
        let outcome: Int32? = await withHandle { h in
            await Task.detached(priority: .userInitiated) { () -> Int32 in
                segment.data.withUnsafeBytes { raw -> Int32 in
                    guard let base = raw.bindMemory(to: UInt8.self).baseAddress else { return -1 }
                    var errPtr: UnsafeMutablePointer<CChar>? = nil
                    let code = ant_publisher_push_segment(
                        h,
                        segment.isInitialization,
                        base,
                        raw.count,
                        segment.durationMs,
                        segment.discontinuity,
                        &errPtr
                    )
                    if let errPtr { ant_free_string(errPtr) }
                    return code
                }
            }.value
        }
        return outcome ?? -1
    }

    /// Live progress of the broadcast, or `nil` when there is none.
    func publisherProgress() async -> PublisherSnapshot? {
        guard let json = try? await ffiString(name: "broadcast progress", { h, errPtr in
            ant_publisher_progress(h, errPtr)
        }) else { return nil }
        return StreamDecoder.publisherSnapshot(from: json)
    }

    /// End the broadcast and take its final report.
    ///
    /// `ant_publisher_stop` blocks while the already-captured segments
    /// are published, the playlist is closed with `#EXT-X-ENDLIST` and
    /// the whole broadcast goes up as the recording (up to ~130 s, its stop grace, ~195 s worst case), so this must not run on the main
    /// actor's thread — ``ffiString`` already hops onto a detached task
    /// for that reason.
    func stopPublisher() async throws -> PublisherReport {
        defer {
            // The stop has returned, so whatever entry is left is an
            // unfinished broadcast (a recording that did not land, or a
            // resume the node refused): offer it again.
            publisherActive = false
            refreshUnfinishedBroadcast()
        }
        let json = try await ffiString(name: "stop broadcast") { h, errPtr in
            ant_publisher_stop(h, errPtr)
        }
        guard let report = StreamDecoder.publisherReport(from: json) else {
            throw AntError.op("could not read the broadcast report")
        }
        return report
    }

    // MARK: - Refresh

    func refreshAll() async {
        await refreshPlan()
        await refreshAccount()
        refreshUnfinishedBroadcast()
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
            // The channel is the account address (and the `antstream`
            // topic): a restored account gets its own.
            if let owner = decoded?.ethAddress, !owner.isEmpty, channel?.owner != owner {
                channel = Channel(owner: owner)
                loadStreamInfo()
            }
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

    /// Read the account wallet's balances from chain. Best-effort: a
    /// failed read keeps the last figures and says why in `walletError`.
    func refreshWallet(rpc: String) async {
        if screenshotSampleActive { return }
        guard !rpc.isEmpty else { return }
        do {
            let json = try await ffiString(name: "wallet balance") { h, errPtr in
                rpc.withCString { ant_wallet_balance(h, $0, errPtr) }
            }
            // The account may have been switched while the read was out:
            // never show one account's wallet under another.
            guard !screenshotSampleActive, handle != nil, let w = WalletBalance.parse(json),
                  account.map({ $0.ethAddress.lowercased() == w.address.lowercased() }) ?? true
            else { return }
            if w != wallet { wallet = w }
            walletError = nil
        } catch {
            if !screenshotSampleActive, handle != nil { walletError = error.localizedDescription }
        }
    }

    /// Move `plur` xBZZ from the account wallet into the chequebook
    /// (`ant_storage_settlement_topup_amount`). Sends a real transaction
    /// and blocks until it confirms.
    ///
    /// That call swaps xDAI for any xBZZ the wallet lacks, but the card
    /// caps this at a wallet read up to 30 s old and promises a plain
    /// transfer. So the wallet is read again right before sending, and a
    /// wallet that no longer holds `plur` stops here with nothing sent
    /// instead of swapping.
    ///
    /// The re-read and the top-up run inside one handle borrow, so both go
    /// to the same node: an account switch can't land between them and
    /// top up (or swap from) another account's wallet than the one just
    /// checked. The wallet read must also be the account the card showed
    /// when the user confirmed.
    func addCredit(rpc: String, plur: Decimal) async throws {
        let amount = plurString(plur)
        let confirmedAddress = account?.ethAddress.lowercased()
        guard let json = try await withHandle({ h -> String in
            let walletJson = try await Self.string(name: "wallet balance") { errPtr in
                rpc.withCString { ant_wallet_balance(h, $0, errPtr) }
            }
            guard let w = WalletBalance.parse(walletJson) else {
                throw AntError.op("could not read the wallet; nothing was sent")
            }
            guard let confirmedAddress, w.address.lowercased() == confirmedAddress else {
                throw AntError.op("the account changed (or isn't loaded yet); nothing was sent")
            }
            if w.bzzPlur < plur {
                throw AntError.op(
                    "your wallet now holds \(formatBzz(w.bzzPlur)), less than \(formatBzz(plur)); nothing was sent"
                )
            }
            return try await Self.string(name: "add to credit") { errPtr in
                rpc.withCString { r in
                    amount.withCString { ant_storage_settlement_topup_amount(h, r, $0, errPtr) }
                }
            }
        }) else {
            throw AntError.notReady
        }
        await afterChequebookChange(json, rpc: rpc)
    }

    /// Move `plur` xBZZ from the chequebook back to the account wallet
    /// (`ant_storage_settlement_withdraw_amount`); the node refuses more
    /// than its issued cheques leave free. Sends a real transaction and
    /// blocks until it confirms. A confirmed withdrawal whose deposit
    /// re-read failed returns `null` JSON, which keeps the deposit card as
    /// it was (re-read by `refreshAll`) rather than reporting a failure.
    func withdrawCredit(rpc: String, plur: Decimal) async throws {
        let amount = plurString(plur)
        let json = try await ffiString(name: "withdraw to account") { h, errPtr in
            rpc.withCString { r in
                amount.withCString { ant_storage_settlement_withdraw_amount(h, r, $0, errPtr) }
            }
        }
        await afterChequebookChange(json, rpc: rpc)
    }

    /// After a deposit or withdrawal: show its result and re-read the rest.
    private func afterChequebookChange(_ depositJson: String, rpc: String) async {
        if let d = StreamDecoder.settlementDeposit(from: depositJson) { settlementDeposit = d }
        await refreshCredit()
        await refreshWallet(rpc: rpc)
        await refreshAll()
    }

    // MARK: - Screenshot hooks (antstream-visual CI)

    /// Set once a screenshot sample has been installed, so the periodic /
    /// scene-phase refreshes that would otherwise reset `plan` and the
    /// settlement fields to their real (empty) runner state leave the
    /// sample in place for the capture. Never set outside the shot path.
    private var screenshotSampleActive = false

    /// A modest connected plan: `plan.enabled` is what gates both the
    /// deposit card and the going-live screen, and the storage meter
    /// shows a populated bar rather than "no plan".
    private static func sampleConnectedPlan(batchId: String) -> StoragePlan {
        // `-antstream-shot-low-capacity`: an immutable plan whose fullest
        // bucket leaves about twenty minutes of 360p (#176), so the
        // checklist and the going-live screen show their warning.
        // `-antstream-shot-unusable-plan`: a roomy immutable plan the
        // node won't stamp with at all (expired or unknown to it, and not
        // merely propagating), so the checklist and the going-live screen
        // show "Swarm won't accept this plan's stamps" (#176, R1-F1).
        if RootView.shotArgs.contains("-antstream-shot-unusable-plan") {
            return StoragePlan(
                enabled: true,
                batchId: batchId,
                batchDepth: 22,
                immutable: true,
                totalCapacityChunks: 4_194_304,
                issuedChunks: 40_000,
                worstCaseRemainingChunks: 4_000_000,
                usable: false,
                propagating: false
            )
        }
        if RootView.shotArgs.contains("-antstream-shot-low-capacity") {
            return StoragePlan(
                enabled: true,
                batchId: batchId,
                batchDepth: 22,
                immutable: true,
                totalCapacityChunks: 4_194_304,
                issuedChunks: 3_900_000,
                worstCaseRemainingChunks: 46_000
            )
        }
        return StoragePlan(
            enabled: true,
            batchId: batchId,
            batchDepth: 22,
            immutable: false,
            totalCapacityChunks: 1_250_000,   // ~5 GB at 4 KiB/chunk
            issuedChunks: 40_000,             // ~160 MB used
            worstCaseRemainingChunks: 1_210_000
        )
    }

    /// Let `-antstream-shot-live` reach the going-live screen on a
    /// runner. A fresh simulator account has no storage plan, and
    /// broadcasting is refused without one, so a sample plan is
    /// published to get past that gate.
    ///
    /// The batch id is deliberately **not** a real one: the segments are
    /// captured, encoded, segmented and handed to the publisher for
    /// real, and the gateway then rejects the stamp — the same "no
    /// usable batch" wall the stage-1 publish rows hit. Nothing here
    /// fakes a successful upload.
    ///
    /// Sample only, and only ever called behind `-antstream-shot-live`.
    func installBroadcastSample() {
        screenshotSampleActive = true
        plan = Self.sampleConnectedPlan(
            batchId: "0x" + String(repeating: "a1", count: 32)
        )
    }

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
        plan = Self.sampleConnectedPlan(
            batchId: "0x0000000000000000000000000000000000000000000000000000000000000000"
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

    /// Set while ``installUnfinishedSample()``'s entry is showing, so a
    /// periodic refresh can't replace it with the runner's real (empty)
    /// state before the shot fires.
    private var unfinishedSampleActive = false

    /// The Broadcast tab's "Unfinished broadcast" card for a CI
    /// screenshot: a runner never has a broadcast killed under it, so
    /// this shows a sample one saved 20 s ago, recent enough to resume.
    /// Nothing is written to disk. Sample only, and only ever called
    /// behind `-antstream-shot-unfinished`.
    func installUnfinishedSample() {
        installChannelSample()
        unfinishedSampleActive = true
        unfinishedBroadcast = UnfinishedBroadcast(
            channel: "Kitchen",
            owner: channel?.owner.lowercased() ?? "",
            broadcastId: "19a3c5e2f10",
            savedAt: Date().addingTimeInterval(-20),
            segments: 214,
            seconds: 428,
            ended: false,
            feed: .current
        )
    }

    /// Set while ``installStreamInfoSample()``'s info is showing, so the
    /// account read can't replace it with the runner's real (empty) info
    /// before the shot fires, and nothing of it is saved.
    private var streamInfoSampleActive = false

    /// The Broadcast tab's stream info card, and with
    /// `-antstream-shot-info-edit` its editor, filled in (#220): a title,
    /// a description, a thumbnail and an announced start tomorrow at
    /// 08:00. A fresh runner account has none, so sample info is shown
    /// (with the channel sample's plan, so Announce reads as available).
    /// Nothing is saved or uploaded. Sample only, and only ever called
    /// behind `-antstream-shot-info`.
    func installStreamInfoSample() {
        installChannelSample()
        streamInfoSampleActive = true
        let calendar = Calendar.current
        let tomorrow = calendar.date(byAdding: .day, value: 1, to: Date()) ?? Date()
        let start = calendar.date(bySettingHour: 8, minute: 0, second: 0, of: tomorrow) ?? tomorrow
        let startMs = UInt64(start.timeIntervalSince1970 * 1000)
        thumbnailJPEG = StreamThumbnail.sampleJPEG()
        streamInfo = StreamInfo(
            title: "Morning walk",
            description: "Along the river to the old bridge, with the swans if they're up.",
            thumbnail: String(repeating: "ab", count: 32),
            thumbnailPicked: true,
            scheduledStartMs: startMs,
            announcedStartMs: startMs
        )
        streamInfoError = nil
        // `-antstream-shot-info-announcing`: an announcement still being
        // written, so Go live is held and the editor can't be cancelled.
        // Nothing is written.
        if RootView.shotArgs.contains("-antstream-shot-info-announcing") {
            announcingBroadcast = true
        }
    }

    /// Drive the Broadcast tab to its ready state, for the "Your channel"
    /// card's CI screenshot (`-antstream-shot-channel`): a fresh runner
    /// account has no plan, so a sample plan and a deployed chequebook are
    /// published. The card itself shows the account's real channel (its
    /// address and the `antstream` topic). Nothing is persisted.
    ///
    /// Sample only, and only ever called behind `-antstream-shot-channel`.
    func installChannelSample() {
        screenshotSampleActive = true
        plan = Self.sampleConnectedPlan(
            batchId: "0x" + String(repeating: "a1", count: 32)
        )
        settlement = SettlementInfo(
            enabled: true,
            chequebook: "0x370e6965000000000000000000000000000000e1"
        )
    }

    /// Drive the Storage tab's Credit card to the state issue #165 was
    /// filed from, for its CI screenshots (`-antstream-shot-credit`): a
    /// wallet holding 5.76 xBZZ next to a chequebook with almost nothing
    /// left. A fresh runner account has neither, so sample figures are
    /// published and latched (`refreshCredit`/`refreshWallet` leave them
    /// alone). Moves no funds; the card's buttons are never pressed.
    ///
    /// Sample only, and only ever called behind `-antstream-shot-credit`.
    func installCreditSample() {
        screenshotSampleActive = true
        let chequebook = "0x370e6965000000000000000000000000000000e1"
        plan = Self.sampleConnectedPlan(
            batchId: "0x" + String(repeating: "a1", count: 32)
        )
        settlement = SettlementInfo(enabled: true, chequebook: chequebook)
        let swap = SwapStatus(
            paying: true,
            swapEnabled: true,
            chequebook: chequebook,
            depositedPlur: Decimal(10_000_000_000_000),  // 0.001 xBZZ
            issuedPlur: Decimal(9_970_000_000_000),
            availablePlur: Decimal(30_000_000_000)      // 0.000003 xBZZ
        )
        let now = Date()
        credit = Credit(
            swap: swap,
            sessionPlur: Decimal(1_240_000_000_000),
            perMinutePlur: Decimal(41_000_000_000),
            recent: [
                Payment(peer: "8e2c51a0f6b34d7d9a1e", plur: Decimal(18_000_000_000), at: now.addingTimeInterval(-3)),
                Payment(peer: "c47d09e1b25f6a3e44b0", plur: Decimal(9_000_000_000), at: now.addingTimeInterval(-41)),
            ]
        )
        wallet = WalletBalance(
            address: account?.ethAddress ?? "",
            bzzPlur: Decimal(string: "57600000000000000")!,     // 5.76 xBZZ
            xdaiWei: Decimal(string: "421300000000000000")!     // 0.4213 xDAI
        )
        walletError = nil
    }

    // MARK: - Polling

    private func startPollingPeers() {
        peerPollTask?.cancel()
        guard handle != nil else { return }
        peerPollTask = Task { @MainActor [weak self] in
            var tick = 0
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
                tick += 1
                // A gateway that failed to bind is retried every 10 s,
                // off this loop so a slow bind never delays the peer count.
                if tick % 10 == 0, !self.gatewayUp {
                    Task { await self.startGateway() }
                }
                if tick % 2 == 0 {
                    Task { await self.refreshCredit() }
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    // MARK: - FFI string helper

    // MARK: - Chequebook

    /// Read the chequebook figures (`ant_swap_status`, from memory) and the
    /// per-peer cheque totals (the gateway's `GET /settlements`, also from
    /// memory), and publish `credit`. A peer whose total grew since the
    /// last poll was just paid the difference.
    func refreshCredit() async {
        guard let h = handle, !creditRefreshing, !screenshotSampleActive else { return }
        creditRefreshing = true
        defer { creditRefreshing = false }
        guard let json = try? await Self.string(name: "swap status", { ant_swap_status(h, $0) }),
              let swap = SwapStatus.parse(json)
        else { return }
        let now = Date()
        if let issued = swap.issuedPlur {
            if sessionIssued == nil { sessionIssued = issued }
            issuedSamples.append((now, issued))
            issuedSamples.removeAll { now.timeIntervalSince($0.at) > 60 }
        }
        var perMinute: Decimal?
        if let first = issuedSamples.first, let last = issuedSamples.last {
            let span = last.at.timeIntervalSince(first.at)
            if span >= 10 {
                perMinute = (last.plur - first.plur) * Decimal(60) / Decimal(span)
            }
        }
        if gatewayUp, let sent = await readSettlements() {
            if let before = lastSent {
                let paid = sent.compactMap { peer, total -> Payment? in
                    let delta = total - (before[peer] ?? 0)
                    return delta > 0 ? Payment(peer: peer, plur: delta, at: now) : nil
                }
                if !paid.isEmpty {
                    recentPayments = Array((paid.sorted { $0.plur > $1.plur } + recentPayments).prefix(8))
                }
            }
            lastSent = sent
        }
        let updated = Credit(
            swap: swap,
            sessionPlur: swap.issuedPlur.map { $0 - (sessionIssued ?? $0) } ?? 0,
            perMinutePlur: perMinute,
            recent: recentPayments
        )
        // A shutdown (account switch) or a screenshot sample may have
        // landed while the reads were out.
        guard !screenshotSampleActive, handle == h else { return }
        if updated != credit { credit = updated }
    }

    /// `GET /settlements`: each peer's cheque total sent, PLUR.
    private func readSettlements() async -> [String: Decimal]? {
        guard let url = gatewayURL?.appendingPathComponent("settlements") else { return nil }
        var request = URLRequest(url: url)
        request.timeoutInterval = 1.5
        guard let (data, response) = try? await URLSession.shared.data(for: request),
              (response as? HTTPURLResponse)?.statusCode == 200,
              let o = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { return nil }
        var sent: [String: Decimal] = [:]
        for row in o["settlements"] as? [[String: Any]] ?? [] {
            guard let peer = row["peer"] as? String,
                  let total = (row["sent"] as? String).flatMap({ Decimal(string: $0) }),
                  total > 0
            else { continue }
            sent[peer] = total
        }
        return sent
    }

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

/// The account's permanent channel: the account's address (the feed
/// owner) and the topic text ``topicText``, `antstream`, the same for every
/// broadcast. One channel per account; the owner is what tells channels
/// apart. Ported from the Android app's `Channel.kt`.
///
/// Broadcasts go out in the publisher's `"solar-punk"` format, so a
/// channel is shown and shared as owner + topic (``ChannelAddress``),
/// with nothing to create first. Every broadcast continues the feed where
/// the last one stopped: the node reads the feed head at the start of
/// each broadcast and writes after it (#173), so nothing here tracks the
/// index.
///
/// The ``name`` is only a label (the broadcast's notes and the UI), never
/// part of the topic, so renaming the channel never forks it. Kept in
/// `UserDefaults` per account.
struct Channel: Equatable {
    let owner: String
    /// The topic text every broadcast goes out on.
    static let topicText = ChannelAddress.antstreamTopic
    /// Where viewers find it.
    let address: ChannelAddress

    private let prefix: String
    private var defaults: UserDefaults { .standard }

    init(owner: String) {
        self.owner = owner
        self.prefix = "antstream.channel.\(owner.lowercased())."
        self.address = ChannelAddress(owner: owner, text: Self.topicText)
    }

    /// The name the last broadcast used; seeds the live screen's field.
    var name: String? {
        get { defaults.string(forKey: prefix + "name") }
        nonmutating set { defaults.set(newValue, forKey: prefix + "name") }
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
