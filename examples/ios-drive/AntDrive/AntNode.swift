import Foundation
import Network
import SwiftUI

/// Swift wrapper around the hand-written C API in `crates/ant-ffi/`.
///
/// Holds a single opaque `AntHandle*` for the app's lifetime. `start()`
/// spins up the embedded node; the upload / storage / account methods
/// drive the FFI on detached tasks (the Rust side blocks on its own
/// Tokio runtime) and publish decoded models back on the main actor for
/// SwiftUI to observe.
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
    @Published private(set) var jobs: [UploadJob] = []
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
    /// Read-back propagation results, keyed by upload job id. Populated only
    /// by an explicit check from the file detail page (`reverify`); `nil`
    /// for a job means "not checked yet" (there is no automatic check).
    @Published private(set) var propagation: [String: PropagationInfo] = [:]
    /// Job ids with an in-flight propagation check, so the UI can show a
    /// spinner and avoid firing duplicate probes.
    @Published private(set) var verifying: Set<String> = []
    /// Live progress for an in-flight verification, keyed by job id. Set
    /// while `verifying` contains the id and cleared when it finishes;
    /// drives the determinate progress bar on the file detail page.
    @Published private(set) var verifyProgress: [String: VerifyProgress] = [:]
    /// `true` while the device has no usable network path
    /// (`NWPathMonitor`). The node is suspended for the duration —
    /// uploads auto-pause and resume on their own — so the UI shows
    /// "Waiting for network…" instead of stuck progress bars.
    @Published private(set) var isOffline: Bool = false
    /// Job ids with an in-flight "Push again" re-push/heal, so the detail
    /// page can show a re-pushing indicator until the follow-up re-verify.
    @Published private(set) var repushing: Set<String> = []
    /// Live progress for an in-flight re-push, keyed by job id (phases
    /// "checking" / "repushing"); drives the determinate bar while a file
    /// is being pushed again.
    @Published private(set) var repushProgress: [String: VerifyProgress] = [:]

    private var handle: OpaquePointer?
    private var peerPollTask: Task<Void, Never>?
    private var refreshTask: Task<Void, Never>?
    private var pathMonitor: NWPathMonitor?

    // MARK: - Lifecycle

    func start() async {
        guard case .idle = status else { return }
        status = .starting

        let dataDir = Self.resolveDataDir()
        Self.seedPeerstoreIfNeeded(in: dataDir)
        // Restore persisted verify verdicts before anything renders: a
        // user-run check is the richest durability signal a row has, and
        // without this a re-checked (green) file fell back to the
        // daemon's stale degraded heal flag on every relaunch.
        loadPersistedVerdicts()
        let path = dataDir.path
        // Register the imports dir as the upload source root so the node
        // can re-anchor persisted uploads when iOS relocates the app
        // container (new UUID on update/reinstall) — without it, old
        // jobs' absolute source paths dangle and they can't resume or
        // self-heal from the source.
        let importsPath = (try? antdriveImportsDir())?.path

        let raw: OpaquePointer? = await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            let h = path.withCString { cpath in
                Self.withOptCString(importsPath) { croot in
                    ant_init_with_options(cpath, croot, &errPtr)
                }
            }
            if h == nil, let errPtr { ant_free_string(errPtr) }
            return h
        }.value

        guard let raw else {
            status = .failed("could not start")
            return
        }
        handle = raw
        status = .ready
        startPollingPeers()
        startNetworkMonitor()
        await refreshAll()
        startAutoRefresh()
    }

    func shutdown() async {
        guard let h = handle else { return }
        handle = nil
        peerPollTask?.cancel()
        refreshTask?.cancel()
        pathMonitor?.cancel()
        pathMonitor = nil
        peerCount = 0
        await Task.detached(priority: .userInitiated) { ant_shutdown(h) }.value
        status = .idle
    }

    /// System-suspend the upload subsystem before the app loses execution
    /// (backgrounding) or the network disappears. In-flight uploads pause
    /// with the "resumes automatically" marker; the call returns once
    /// upload state is checkpointed on disk (bounded node-side at ~5 s),
    /// so run it inside a `beginBackgroundTask` window.
    func suspend() async {
        guard let h = handle else { return }
        await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if ant_suspend(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
        }.value
    }

    /// Undo `suspend()` on foreground / network-restored transitions:
    /// restarts the uploads it paused (a user pause stays paused) and
    /// re-queues securing for completed-but-unverified files. Also
    /// re-warms the peer connections (`ant_resume`) — a suspension long
    /// enough to pause uploads has usually also reaped the sockets.
    func wake() async {
        guard let h = handle else { return }
        await Task.detached(priority: .userInitiated) {
            var errPtr: UnsafeMutablePointer<CChar>? = nil
            if ant_resume(h, &errPtr) != 0, let errPtr { ant_free_string(errPtr) }
            var wakeErr: UnsafeMutablePointer<CChar>? = nil
            if ant_wake(h, &wakeErr) != 0, let wakeErr { ant_free_string(wakeErr) }
        }.value
        await refreshJobs()
    }

    /// Any upload that still needs network work to become durable — an
    /// in-flight or auto-paused transfer, or a completed file whose
    /// securing pass hasn't finished. Drives whether backgrounding
    /// schedules the `antdrive.secure` background task.
    var hasUnsettledUploads: Bool {
        jobs.contains { $0.isActive || $0.isAutoPaused || $0.isSecuring }
    }

    /// Watch the device's network path. Offline ⇒ suspend the node's
    /// uploads (they'd only burn retries against a dead network) and
    /// surface "Waiting for network…"; back online ⇒ wake them. The
    /// node-side `stalled` flag still covers degraded-but-connected.
    private func startNetworkMonitor() {
        guard pathMonitor == nil else { return }
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] netPath in
            let offline = netPath.status != .satisfied
            Task { @MainActor [weak self] in
                guard let self, self.isOffline != offline else { return }
                self.isOffline = offline
                if offline {
                    await self.suspend()
                } else {
                    await self.wake()
                }
            }
        }
        monitor.start(queue: DispatchQueue(label: "antdrive.netpath"))
        pathMonitor = monitor
    }

    // MARK: - Uploads

    /// Upload a file. Stamps against the connected storage plan (its
    /// batch id), so a plan must exist first.
    @discardableResult
    func startUpload(path: URL, name: String?, contentType: String?) async throws -> UploadJob? {
        guard let h = handle else { throw AntError.notReady }
        let batch = plan?.enabled == true ? plan?.batchId : nil
        let p = path.path
        let json = try await Self.string(name: "upload start") { errPtr in
            p.withCString { cpath in
                Self.withOptCString(batch) { cbatch in
                    Self.withOptCString(name) { cname in
                        Self.withOptCString(contentType) { cct in
                            ant_upload_start(h, cpath, cbatch, cname, cct, errPtr)
                        }
                    }
                }
            }
        }
        // `json` here is just the job id; refresh to pull the full row.
        _ = json
        await refreshJobs()
        return jobs.first
    }

    func pauseUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_pause) }
    func resumeUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_resume) }
    func cancelUpload(_ jobId: String) async { await jobControl(jobId, ant_upload_cancel) }

    /// "Push again" for a completed file whose chunks didn't fully
    /// propagate. Runs a self-heal on the *same* job — re-pushing only the
    /// missing chunks (from the local chunk store, falling back to the
    /// source) — and streams progress into `repushProgress` so the detail
    /// page shows a determinate bar. No new upload row is created. When the
    /// heal settles, re-verifies so the row reflects the new verdict.
    func repushUpload(_ job: UploadJob) async {
        guard let h = handle, job.isDone else { return }
        if repushing.contains(job.id) { return }
        repushing.insert(job.id)
        // Drop the stale "not verified" verdict so the card shows the
        // re-pushing state, not the old failure, while the heal runs.
        setPropagation(nil, for: job.id)
        repushProgress[job.id] = VerifyProgress(phase: "checking", checked: nil, total: nil)
        defer { repushing.remove(job.id); repushProgress[job.id] = nil }

        let jobId = job.id
        _ = await withCheckedContinuation { (cont: CheckedContinuation<String?, Never>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let box = VerifyProgressBox { line in
                    guard let p = VerifyProgress(jsonLine: line) else { return }
                    Task { @MainActor [weak self] in self?.repushProgress[jobId] = p }
                }
                let ctx = Unmanaged.passRetained(box).toOpaque()
                let cb: @convention(c) (UnsafePointer<CChar>?, UnsafeMutableRawPointer?) -> Void = { cstr, ctxp in
                    guard let cstr, let ctxp else { return }
                    Unmanaged<VerifyProgressBox>.fromOpaque(ctxp)
                        .takeUnretainedValue()
                        .handler(String(cString: cstr))
                }
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let raw = jobId.withCString {
                    ant_upload_repush_progress(h, $0, cb, ctx, &errPtr)
                }
                Unmanaged<VerifyProgressBox>.fromOpaque(ctx).release()
                if let raw {
                    let s = String(cString: raw)
                    ant_free_string(raw)
                    cont.resume(returning: s)
                } else {
                    if let errPtr { ant_free_string(errPtr) }
                    cont.resume(returning: nil)
                }
            }
        }

        await refreshJobs()
        // Confirm the outcome on the same row.
        await reverify(job)
    }

    private func jobControl(
        _ jobId: String,
        _ fn: @escaping (OpaquePointer?, UnsafePointer<CChar>?, UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) async {
        guard let h = handle else { return }
        _ = try? await Self.string(name: "job control") { errPtr in
            jobId.withCString { fn(h, $0, errPtr) }
        }
        await refreshJobs()
    }

    // MARK: - Storage

    /// Connect a storage plan the account already owns on Gnosis, by id.
    func connectStorage(rpc: String, batchId: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "connect storage") { errPtr in
            rpc.withCString { crpc in
                batchId.withCString { cbatch in
                    ant_storage_connect_batch(h, crpc, cbatch, errPtr)
                }
            }
        }
        if let p = DriveDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// True once a storage plan is connected — gates uploads.
    var hasStorage: Bool { plan?.enabled == true }

    /// Price a plan (no transaction). Returns the payment information.
    func quoteStorage(rpc: String, depth: UInt8, days: UInt64) async throws -> StorageQuote {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "price plan") { errPtr in
            rpc.withCString { ant_storage_quote(h, $0, depth, days, errPtr) }
        }
        guard let q = DriveDecoder.quote(from: json) else {
            throw AntError.op("could not read plan price")
        }
        return q
    }

    /// Buy + activate a plan funding only with xDAI: the node swaps the
    /// xBZZ shortfall on-chain itself, then buys the plan. Spends real funds.
    func buyStorage(rpc: String, depth: UInt8, amountPerChunk: String, immutable: Bool) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "activate storage") { errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_buy_xdai(h, crpc, depth, camt, immutable ? 1 : 0, errPtr)
                }
            }
        }
        if let p = DriveDecoder.plan(from: json) { plan = p }
        await refreshAll()
    }

    /// Price extending the connected plan's lifetime by `days` (no
    /// transaction). Returns the same payment information as a new-plan
    /// quote, sized to the connected plan's depth.
    func quoteTopUp(rpc: String, days: UInt64) async throws -> StorageQuote {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "price extension") { errPtr in
            rpc.withCString { ant_storage_topup_quote(h, $0, days, errPtr) }
        }
        guard let q = DriveDecoder.quote(from: json) else {
            throw AntError.op("could not read extension price")
        }
        return q
    }

    /// Extend the connected plan's lifetime, funding only with xDAI: the
    /// node swaps the xBZZ shortfall itself, then tops the batch up
    /// on-chain. Spends real funds. Publishes the refreshed validity.
    func topUpStorage(rpc: String, amountPerChunk: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "extend storage") { errPtr in
            rpc.withCString { crpc in
                amountPerChunk.withCString { camt in
                    ant_storage_topup_xdai(h, crpc, camt, errPtr)
                }
            }
        }
        if let v = DriveDecoder.validity(from: json) { validity = v }
        await refreshAll()
    }

    /// Auto-discover every storage plan the account owns on Gnosis.
    func discoverStorage(rpc: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        _ = try await Self.string(name: "find storage") { errPtr in
            rpc.withCString { ant_storage_discover(h, $0, errPtr) }
        }
        await refreshAll()
    }

    // MARK: - Propagation verification

    /// Streaming verification: checks that a completed upload actually
    /// propagated by resolving its manifest, enumerating the chunk tree, and
    /// checking *every* data leaf over the network (bypassing our local
    /// copy), publishing incremental `verifyProgress` so the UI can draw a
    /// determinate bar. Only ever run on explicit user action from the file
    /// detail page — there is no automatic verification. The FFI call blocks
    /// on a background queue; its C progress callback hops each update back
    /// to the main actor. `probes` is how many distinct closest peers to ask
    /// per chunk (replication depth, clamped 1…8).
    @discardableResult
    func verifyPropagationStreaming(_ job: UploadJob, samples: UInt8 = 0,
                                    probes: UInt8 = 2) async -> PropagationInfo? {
        guard let h = handle, let reference = job.reference, !reference.isEmpty else { return nil }
        if verifying.contains(job.id) { return propagation[job.id] }
        verifying.insert(job.id)
        verifyProgress[job.id] = VerifyProgress(phase: "resolving", checked: nil, total: nil)
        defer { verifying.remove(job.id); verifyProgress[job.id] = nil }

        let jobId = job.id
        let json: String? = await withCheckedContinuation { (cont: CheckedContinuation<String?, Never>) in
            DispatchQueue.global(qos: .userInitiated).async {
                let box = VerifyProgressBox { line in
                    guard let p = VerifyProgress(jsonLine: line) else { return }
                    Task { @MainActor [weak self] in self?.verifyProgress[jobId] = p }
                }
                let ctx = Unmanaged.passRetained(box).toOpaque()
                let cb: @convention(c) (UnsafePointer<CChar>?, UnsafeMutableRawPointer?) -> Void = { cstr, ctxp in
                    guard let cstr, let ctxp else { return }
                    Unmanaged<VerifyProgressBox>.fromOpaque(ctxp)
                        .takeUnretainedValue()
                        .handler(String(cString: cstr))
                }
                var errPtr: UnsafeMutablePointer<CChar>? = nil
                let raw = reference.withCString {
                    ant_storage_verify_propagation_progress(h, $0, samples, probes, cb, ctx, &errPtr)
                }
                // Balance the passRetained above now the C call (and its
                // callbacks) are done.
                Unmanaged<VerifyProgressBox>.fromOpaque(ctx).release()
                if let raw {
                    let s = String(cString: raw)
                    ant_free_string(raw)
                    cont.resume(returning: s)
                } else {
                    if let errPtr { ant_free_string(errPtr) }
                    cont.resume(returning: nil)
                }
            }
        }

        guard let json, let info = DriveDecoder.propagation(from: json) else { return nil }
        // A user-cancelled check returns a cancelled body — leave the file
        // unchecked (no verdict) rather than recording a bogus "missing".
        if info.error == "cancelled" { return nil }
        setPropagation(info, for: jobId)
        return info
    }

    /// Cancel the in-flight verification (the detail page's Cancel button).
    /// Cooperative: the node aborts at the next phase/leaf boundary and the
    /// streaming call returns, clearing `verifying`.
    func cancelVerify() {
        guard let h = handle else { return }
        ant_verify_cancel(h)
    }

    /// Run a fresh network check of a single completed file from the detail
    /// page's "Check again" / "Deep verify" buttons. Every chunk is checked
    /// (`samples == 0`, uncapped); `probes` raises how many distinct closest
    /// peers each chunk is fetched from — a replication-depth knob (1…8).
    /// Deep verify passes the max so the reported `sources` floor reflects
    /// real redundancy, not just "at least one copy exists".
    func reverify(_ job: UploadJob, probes: UInt8 = 2) async {
        guard job.reference?.isEmpty == false else { return }
        // Keep the previous verdict on screen until the new one replaces
        // it: clearing it here made a previously-green file fall back to
        // the warning-toned "Propagating" state for the whole duration
        // of the re-check. The `verifying` set already communicates the
        // in-flight check; a cancelled/failed check keeps the old
        // verdict.
        await verifyPropagationStreaming(job, samples: 0, probes: probes)
    }

    // MARK: - Verdict persistence

    /// Single mutation point for `propagation` so every change lands on
    /// disk: verdicts must survive an app restart, or a re-checked
    /// (green) file flip-flops back to the daemon's degraded heal label
    /// on relaunch. Stored as one JSON dictionary in the app data dir.
    private func setPropagation(_ info: PropagationInfo?, for jobId: String) {
        if let info {
            propagation[jobId] = info
        } else {
            propagation.removeValue(forKey: jobId)
        }
        let snapshot = propagation
        let url = Self.verdictsFileURL()
        Task.detached(priority: .utility) {
            if let data = try? JSONEncoder().encode(snapshot) {
                try? data.write(to: url, options: .atomic)
            }
        }
    }

    private func loadPersistedVerdicts() {
        guard let data = try? Data(contentsOf: Self.verdictsFileURL()),
              let decoded = try? JSONDecoder().decode([String: PropagationInfo].self, from: data)
        else { return }
        propagation = decoded
    }

    private static func verdictsFileURL() -> URL {
        resolveDataDir().appendingPathComponent("propagation.json")
    }

    /// Probe depth for a thorough, user-requested "Deep verify": fetch
    /// every chunk from as many distinct closest peers as the FFI allows.
    static let deepVerifyProbes: UInt8 = 8

    private static func normalizedRef(_ r: String) -> String {
        (r.hasPrefix("0x") ? String(r.dropFirst(2)) : r).lowercased()
    }

    // MARK: - Account

    func exportKey() async throws -> String {
        guard let h = handle else { throw AntError.notReady }
        return try await Self.string(name: "export key") { errPtr in
            ant_account_export_key(h, errPtr)
        }
    }

    // MARK: - Refresh

    func refreshAll() async {
        await refreshJobs()
        await refreshPlan()
        await refreshAccount()
        await refreshSettlement()
    }

    func refreshJobs() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "list uploads", { errPtr in
            ant_upload_list(h, errPtr)
        }) {
            let decoded = Self.dedupedByContent(DriveDecoder.jobs(from: json))
            // Same dedup as the peer poll: the 1.5 s auto-refresh must
            // not republish an unchanged list.
            if decoded != jobs { jobs = decoded }
        }
    }

    /// Collapse jobs that point at the same content so a file never shows
    /// twice. A Swarm reference is a content hash, so two completed jobs
    /// with the same reference are the same file (e.g. a file uploaded — or,
    /// before the in-place fix, "pushed again" — twice). Within a reference
    /// group the most useful row wins: a completed one over a non-completed
    /// one, then the most recent. Jobs without a reference yet
    /// (in-progress / failed) are content-unknown, so each is kept. The
    /// result is ordered newest-first for display.
    private static func dedupedByContent(_ jobs: [UploadJob]) -> [UploadJob] {
        let ranked = jobs.sorted {
            if $0.isDone != $1.isDone { return $0.isDone && !$1.isDone }
            return $0.createdAtUnix > $1.createdAtUnix
        }
        var seen = Set<String>()
        var kept: [UploadJob] = []
        for job in ranked {
            if let ref = job.reference, !ref.isEmpty {
                if !seen.insert(normalizedRef(ref)).inserted { continue }
            }
            kept.append(job)
        }
        return kept.sorted { $0.createdAtUnix > $1.createdAtUnix }
    }

    func refreshPlan() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "storage status", { errPtr in
            ant_storage_status(h, errPtr)
        }) {
            let decoded = DriveDecoder.plan(from: json)
            if decoded != plan { plan = decoded }
        }
    }

    func refreshAccount() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "account info", { errPtr in
            ant_account_info(h, errPtr)
        }) {
            let decoded = DriveDecoder.account(from: json)
            if decoded != account { account = decoded }
        }
    }

    /// Fetch the connected plan's remaining lifetime from chain. Needs a
    /// Gnosis RPC URL; a no-op (leaving the last value) when none is set or
    /// no plan is connected. Best-effort: a flaky RPC just leaves the
    /// previous value in place rather than clearing the card.
    func refreshValidity(rpc: String) async {
        guard let h = handle, !rpc.isEmpty, hasStorage else { return }
        if let json = try? await Self.string(name: "storage validity", { errPtr in
            rpc.withCString { ant_storage_validity(h, $0, errPtr) }
        }), let v = DriveDecoder.validity(from: json) {
            validity = v
        }
    }

    func refreshSettlement() async {
        guard let h = handle else { return }
        if let json = try? await Self.string(name: "settlement status", { errPtr in
            ant_storage_settlement_status(h, errPtr)
        }) {
            let decoded = DriveDecoder.settlement(from: json)
            if decoded != settlement { settlement = decoded }
        }
    }

    /// Read from chain how much xBZZ stands behind the chequebook. Needs a
    /// Gnosis RPC URL and a deployed chequebook; best-effort, so a flaky
    /// RPC leaves the previous value rather than clearing the card.
    func refreshSettlementDeposit(rpc: String) async {
        guard let h = handle, !rpc.isEmpty, settlement?.enabled == true else { return }
        if let json = try? await Self.string(name: "settlement deposit", { errPtr in
            rpc.withCString { ant_storage_settlement_deposit(h, $0, errPtr) }
        }), let d = DriveDecoder.settlementDeposit(from: json) {
            settlementDeposit = d
        }
    }

    /// Fund the chequebook up to the settlement deposit, paying with xDAI
    /// (the node swaps for the xBZZ it needs). Spends real funds. Publishes
    /// the refreshed deposit so the card updates in place.
    func topUpSettlementDeposit(rpc: String) async throws {
        guard let h = handle else { throw AntError.notReady }
        let json = try await Self.string(name: "top up settlement deposit") { errPtr in
            rpc.withCString { ant_storage_settlement_topup(h, $0, errPtr) }
        }
        if let d = DriveDecoder.settlementDeposit(from: json) { settlementDeposit = d }
        await refreshAll()
    }

    // MARK: - Polling

    private func startPollingPeers() {
        peerPollTask?.cancel()
        guard let h = handle else { return }
        peerPollTask = Task { [weak self] in
            while !Task.isCancelled {
                let count = Int(ant_peer_count(h))
                await MainActor.run {
                    guard let self, self.handle == h else { return }
                    // Publish only real changes: an unconditional
                    // re-assign fires objectWillChange every second,
                    // re-rendering every observing view (a visible
                    // glass shimmer) and tearing down open menus
                    // mid-tap.
                    let clamped = max(0, count)
                    if self.peerCount != clamped { self.peerCount = clamped }
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    /// Keep the job list current. Two cadences:
    ///
    /// * **Fast (1.5 s)** while a transfer or securing pass is visibly
    ///   live, so bars move.
    /// * **Slow (10 s)** while any completed file is still unverified —
    ///   the node's retry ticker starts securing passes on its *own*
    ///   schedule, so a state change can happen while the app believes
    ///   everything is idle. Gating all polling on the *last known*
    ///   state deadlocked here: the UI could never notice the pass that
    ///   would have made it look busy again (a degraded file sat frozen
    ///   on "next check: any moment now" while the node re-checked
    ///   underneath).
    ///
    /// No polling at all when nothing is watchable (all files verified
    /// or terminal — nothing changes node-side on its own), and the
    /// publish dedup means a no-change poll never re-renders.
    private func startAutoRefresh() {
        refreshTask?.cancel()
        refreshTask = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                let (busy, watchable) = await MainActor.run {
                    (
                        self.jobs.contains {
                            $0.isActive || $0.isSecuring || $0.healPhase != nil
                        },
                        self.jobs.contains { $0.isDone && !$0.healVerified }
                    )
                }
                try? await Task.sleep(nanoseconds: busy ? 1_500_000_000 : 10_000_000_000)
                if busy || watchable { await self.refreshJobs() }
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

    // `nonisolated`: a pure pointer helper, also used inside detached
    // tasks (the init call passes the optional source root from one).
    nonisolated private static func withOptCString<R>(_ s: String?, _ body: (UnsafePointer<CChar>?) -> R) -> R {
        if let s { return s.withCString { body($0) } }
        return body(nil)
    }

    // MARK: - Data dir / peer seed

    private static func resolveDataDir() -> URL {
        let fm = FileManager.default
        let base = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
        let dir = base.appendingPathComponent("antdrive", isDirectory: true)
        try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
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

/// Boxes a Swift progress handler so it can be carried through the FFI's
/// `void *ctx` and recovered inside the C function-pointer callback (which
/// itself can't capture state). Retained for the duration of one verify
/// call and released once it returns.
private final class VerifyProgressBox {
    let handler: (String) -> Void
    init(_ handler: @escaping (String) -> Void) { self.handler = handler }
}

enum AntError: LocalizedError {
    case notReady
    case op(String)

    var errorDescription: String? {
        switch self {
        case .notReady: return "AntDrive is still starting up"
        case .op(let m): return m
        }
    }
}
