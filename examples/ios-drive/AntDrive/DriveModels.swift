import Foundation

/// Swift mirrors of the JSON the FFI returns. Field names match the
/// `serde`-serialised Rust structs (`UploadJobView`, `PostageStatusView`,
/// and the account info object) exactly, so plain `Codable` decoding
/// works without custom keys.

/// One upload job, as returned by `ant_upload_list` / `ant_upload_status`.
struct UploadJob: Codable, Identifiable, Equatable {
    let jobId: String
    let sourcePath: String
    let sourceSize: UInt64
    let name: String?
    let contentType: String?
    let status: String
    let bytesPushed: UInt64
    let chunksPushed: UInt64
    let chunksTotal: UInt64?
    let createdAtUnix: UInt64
    let lastUpdateUnix: UInt64
    let lastError: String?
    let reference: String?
    /// `true` while the driver keeps retrying but nothing has acked for a
    /// sustained window — the network is effectively down for us. The job
    /// isn't failed; the UI shows "Waiting for network…" instead of a
    /// frozen progress bar.
    let stalled: Bool
    /// `true` when the pause was imposed by the system (app suspension /
    /// offline) rather than the user. Such uploads resume on their own,
    /// so the row says "Paused — resumes automatically".
    let autoPaused: Bool
    /// `true` once the daemon's automatic post-upload self-heal confirmed
    /// every chunk is *deep-reachable* (held by its true neighbourhood, not
    /// merely reachable via our privileged link to a shallow storer). This is
    /// the real "safe / backed up" signal — `status == "completed"` only means
    /// every chunk was pushed once. Defaults to `false` so an older daemon
    /// that doesn't emit the field still decodes.
    let healVerified: Bool
    /// `true` once the post-upload heal finished its rounds for this job,
    /// whether or not it could confirm `healVerified`. Lets the UI stop
    /// showing "Securing…" once heal is done even on the degraded path.
    let healFinished: Bool
    /// Parts the last completed securing pass could not confirm — the
    /// honest residual behind "Propagating…", shrinking across the
    /// node's automatic retries. `nil` when never measured.
    let healMissing: UInt64?
    /// When the node's next automatic securing retry becomes eligible
    /// (unix seconds), for a "next check in ~M min" label. `nil` while a
    /// pass runs, once verified, or on an older daemon.
    let healRetryUnix: UInt64?
    /// Completed securing passes that ended without verifying —
    /// "Checked N times" evidence that the retry loop is active.
    let healAttempts: UInt64
    /// Unix seconds of the most recent completed-but-unverified pass
    /// ("last checked 3 min ago").
    let healLastCheckUnix: UInt64?
    /// Live "Securing…" progress emitted by the daemon's automatic heal.
    /// `healPhase` is the current step (`"checking"` while reading chunks
    /// back, `"repushing"` while re-storing the ones that didn't land);
    /// `healChecked` of `healTotal` is that step's determinate count. All
    /// `nil` for an older daemon, for a step with no count, or once heal
    /// finished and the daemon cleared them — so the UI falls back to an
    /// indeterminate spinner.
    let healPhase: String?
    let healChecked: UInt64?
    let healTotal: UInt64?

    var id: String { jobId }

    enum CodingKeys: String, CodingKey {
        case jobId = "job_id"
        case sourcePath = "source_path"
        case sourceSize = "source_size"
        case name
        case contentType = "content_type"
        case status
        case bytesPushed = "bytes_pushed"
        case chunksPushed = "chunks_pushed"
        case chunksTotal = "chunks_total"
        case createdAtUnix = "created_at_unix"
        case lastUpdateUnix = "last_update_unix"
        case lastError = "last_error"
        case reference
        case stalled
        case autoPaused = "auto_paused"
        case healVerified = "heal_verified"
        case healFinished = "heal_finished"
        case healMissing = "heal_missing"
        case healRetryUnix = "heal_retry_unix"
        case healAttempts = "heal_attempts"
        case healLastCheckUnix = "heal_last_check_unix"
        case healPhase = "heal_phase"
        case healChecked = "heal_checked"
        case healTotal = "heal_total"
    }

    /// Custom decode so the two heal fields default to `false` against an
    /// older daemon that doesn't emit them, instead of failing the whole
    /// job list. Every other field decodes as before.
    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        jobId = try c.decode(String.self, forKey: .jobId)
        sourcePath = try c.decode(String.self, forKey: .sourcePath)
        sourceSize = try c.decode(UInt64.self, forKey: .sourceSize)
        name = try c.decodeIfPresent(String.self, forKey: .name)
        contentType = try c.decodeIfPresent(String.self, forKey: .contentType)
        status = try c.decode(String.self, forKey: .status)
        bytesPushed = try c.decode(UInt64.self, forKey: .bytesPushed)
        chunksPushed = try c.decode(UInt64.self, forKey: .chunksPushed)
        chunksTotal = try c.decodeIfPresent(UInt64.self, forKey: .chunksTotal)
        createdAtUnix = try c.decode(UInt64.self, forKey: .createdAtUnix)
        lastUpdateUnix = try c.decode(UInt64.self, forKey: .lastUpdateUnix)
        lastError = try c.decodeIfPresent(String.self, forKey: .lastError)
        reference = try c.decodeIfPresent(String.self, forKey: .reference)
        stalled = try c.decodeIfPresent(Bool.self, forKey: .stalled) ?? false
        autoPaused = try c.decodeIfPresent(Bool.self, forKey: .autoPaused) ?? false
        healVerified = try c.decodeIfPresent(Bool.self, forKey: .healVerified) ?? false
        healFinished = try c.decodeIfPresent(Bool.self, forKey: .healFinished) ?? false
        healMissing = try c.decodeIfPresent(UInt64.self, forKey: .healMissing)
        healRetryUnix = try c.decodeIfPresent(UInt64.self, forKey: .healRetryUnix)
        healAttempts = try c.decodeIfPresent(UInt64.self, forKey: .healAttempts) ?? 0
        healLastCheckUnix = try c.decodeIfPresent(UInt64.self, forKey: .healLastCheckUnix)
        healPhase = try c.decodeIfPresent(String.self, forKey: .healPhase)
        healChecked = try c.decodeIfPresent(UInt64.self, forKey: .healChecked)
        healTotal = try c.decodeIfPresent(UInt64.self, forKey: .healTotal)
    }

    /// Best display name: the manifest name, else the source file's
    /// basename, else the job id.
    var displayName: String {
        if let name, !name.isEmpty { return name }
        let base = (sourcePath as NSString).lastPathComponent
        return base.isEmpty ? jobId : base
    }

    /// 0…1 upload progress. `nil` until the total chunk count is known.
    var fraction: Double? {
        guard let total = chunksTotal, total > 0 else { return nil }
        return min(1.0, Double(chunksPushed) / Double(total))
    }

    /// 0…1 progress of the post-upload "Securing…" heal, when the daemon is
    /// reporting determinate counts for the current step; `nil` for a step
    /// with no count (or an older daemon) so the UI shows a spinner instead.
    var securingFraction: Double? {
        guard let total = healTotal, total > 0, let checked = healChecked else { return nil }
        return min(1.0, Double(checked) / Double(total))
    }

    /// `true` while this file is waiting its turn in the bounded heal queue
    /// (the daemon caps how many secure at once). Distinct from actively
    /// securing — the bar isn't moving because it hasn't started yet.
    var isQueuedToSecure: Bool { isSecuring && healPhase == "queued" }

    /// Human label for the current securing step.
    var securingPhaseLabel: String {
        switch healPhase {
        case "queued": return "Waiting to secure…"
        case "checking": return "Checking the network…"
        case "repushing": return "Re-storing missing parts…"
        default: return "Securing…"
        }
    }

    /// Row label for a degraded (still-propagating) file: lead with the
    /// honest residual when the node measured one, so the user sees the
    /// number shrink across automatic retries.
    var propagatingLabel: String {
        if let n = healMissing, n > 0 {
            return "Propagating — \(n) \(n == 1 ? "part" : "parts") left"
        }
        return "Propagating…"
    }

    /// "Checked 4x — last 2 min ago" for the degraded card: proof the
    /// automatic retry loop is working the file between visible passes.
    var checkHistoryLabel: String? {
        guard healAttempts > 0 else { return nil }
        var label = "Checked \(healAttempts)\u{00D7}"
        if let at = healLastCheckUnix {
            let secs = max(0, Int64(Date().timeIntervalSince1970) - Int64(at))
            if secs < 90 {
                label += " — just now"
            } else if secs < 3600 {
                label += " — last \(secs / 60) min ago"
            } else {
                label += " — last \(secs / 3600) h ago"
            }
        }
        return label
    }

    /// "Next check in ~M min" for the degraded card, from the node's
    /// retry schedule. `nil` when unknown or already due.
    var nextCheckLabel: String? {
        guard let at = healRetryUnix else { return nil }
        let secs = Int64(at) - Int64(Date().timeIntervalSince1970)
        if secs <= 30 { return "Next check: any moment now" }
        let mins = (secs + 59) / 60
        return "Next check in ~\(mins) min"
    }

    var isActive: Bool { status == "running" || status == "pending" }
    var isPaused: Bool { status == "paused" }
    /// Paused by the system (suspension / offline), not the user — it
    /// resumes on its own at the next wake.
    var isAutoPaused: Bool { isPaused && autoPaused }
    /// Every chunk was pushed once. NOT the same as durable — the file may
    /// still be settling into its neighbourhoods (see `isDurable`).
    var isDone: Bool { status == "completed" }
    var isFailed: Bool { status == "failed" }

    /// Completed AND the daemon's automatic heal confirmed deep reachability:
    /// the real "safe / backed up" state.
    var isDurable: Bool { isDone && healVerified }
    /// Completed but the background heal hasn't finished its rounds yet — the
    /// file is still being secured. Drive a spinner / "Securing…" off this.
    var isSecuring: Bool { isDone && !healFinished }
    /// Completed, the last securing pass ran to the end, but couldn't
    /// confirm deep reachability yet: some chunks are still settling into
    /// place. The node re-queues the pass automatically on every
    /// launch/wake until the file verifies; "Push again" is the manual
    /// accelerator.
    var isDegraded: Bool { isDone && healFinished && !healVerified }

    /// A friendly, non-Swarm status line for the row. Completed uploads
    /// return an empty string: the network-verified "Verified" badge below
    /// the status line is the real completion signal, so the row doesn't
    /// also carry a weaker local "Online" word.
    var statusLabel: String {
        switch status {
        case "completed":
            // Heal still running ⇒ tell the user it's being secured rather
            // than implying it's already safe. Heal finished but couldn't
            // confirm deep reachability ⇒ "Propagating…": the node keeps
            // re-queuing the securing pass on every launch/wake until the
            // file verifies, so this is an in-progress state, not a dead
            // end. Verified ⇒ stay silent (the "Verified" badge is the
            // completion signal).
            if isSecuring {
                if isQueuedToSecure { return "Queued" }
                return securingFraction.map { "Securing \(Int($0 * 100))%" } ?? "Securing…"
            }
            if isDegraded { return propagatingLabel }
            return ""
        // `pending` is the daemon's pre-dispatch state (before the driver
        // pushes its first chunk); from the user's view that's still part
        // of uploading, so it shares the "Uploading…" label. A stalled
        // driver (nothing acked for minutes; still retrying) reads as a
        // network problem, not slow progress.
        case "running", "pending":
            if stalled { return "Waiting for network…" }
            return fraction.map { "Uploading \(Int($0 * 100))%" } ?? "Uploading…"
        case "paused": return autoPaused ? "Paused — resumes automatically" : "Paused"
        case "failed": return "Upload failed"
        case "cancelled": return "Cancelled"
        default: return status.capitalized
        }
    }
}

private struct UploadJobList: Codable {
    let jobs: [UploadJob]
}

/// The local storage plan, as returned by `ant_storage_status`.
struct StoragePlan: Codable, Equatable {
    let enabled: Bool
    let batchId: String
    let batchDepth: UInt8
    let immutable: Bool
    let totalCapacityChunks: UInt64
    let issuedChunks: UInt64
    let worstCaseRemainingChunks: UInt64

    enum CodingKeys: String, CodingKey {
        case enabled
        case batchId = "batch_id"
        case batchDepth = "batch_depth"
        case immutable
        case totalCapacityChunks = "total_capacity_chunks"
        case issuedChunks = "issued_chunks"
        case worstCaseRemainingChunks = "worst_case_remaining_chunks"
    }

    /// Swarm chunks are 4 KiB. Translate the chunk-denominated capacity
    /// figures into bytes so the UI can show a familiar "GB" meter.
    static let bytesPerChunk: UInt64 = 4096

    var totalBytes: UInt64 { totalCapacityChunks * Self.bytesPerChunk }
    var usedBytes: UInt64 { issuedChunks * Self.bytesPerChunk }
    /// Free space the user actually has: total capacity minus what's
    /// been issued. This is the batch's true `remaining_total`
    /// (`Σ free indices`), not `worstCaseRemainingChunks` — the latter
    /// is a pessimistic pre-flight budget (room left in the *fullest*
    /// bucket, scaled) that collapses to 0 as soon as a single bucket
    /// saturates, which is misleading as a free-space meter.
    var freeBytes: UInt64 { totalBytes - usedBytes }

    var usedFraction: Double {
        guard totalBytes > 0 else { return 0 }
        return min(1.0, Double(usedBytes) / Double(totalBytes))
    }

    /// "Running low" heuristic: less than 10% of the plan's capacity
    /// left, consistent with the free-space figure and meter bar.
    var isLow: Bool {
        guard totalCapacityChunks > 0 else { return false }
        return Double(issuedChunks) / Double(totalCapacityChunks) > 0.90
    }
}

/// Outbound-settlement status, as returned by
/// `ant_storage_settlement_status`. `enabled` is what lets uploads
/// actually reach the network: without a deployed chequebook the node
/// can't pay peers for pushsync and they stop accepting its chunks, so
/// files look "uploaded" locally but never propagate.
struct SettlementInfo: Codable, Equatable {
    let enabled: Bool
    let chequebook: String?
}

/// What actually stands behind this account's cheques, as returned by
/// `ant_storage_settlement_deposit`. `SettlementInfo` says a chequebook
/// exists; this says whether it is funded. A chequebook deployed with no
/// deposit signs cheques nobody can cash: uploads run clean until the
/// peers' payment tolerance is used up, then collapse into pushsync
/// timeouts — so the Storage tab detects that here and offers a top-up.
struct SettlementDeposit: Codable, Equatable {
    /// False when this account has no chequebook yet — nothing to top up
    /// (buying or connecting a plan deploys one, funded).
    let enabled: Bool
    let chequebook: String?
    /// xBZZ behind the chequebook right now, and the deposit we aim for.
    let depositBzz: String
    let targetBzz: String
    /// What is still missing, and the single predicate ("is it short?")
    /// the card and its button both read.
    let shortfallBzz: String
    let needsTopUp: Bool
    /// Extra xDAI the account must receive before the top-up can run.
    let xdaiToSendDisplay: String
    let sufficientFunds: Bool

    enum CodingKeys: String, CodingKey {
        case enabled
        case chequebook
        case depositBzz = "deposit_bzz"
        case targetBzz = "target_bzz"
        case shortfallBzz = "shortfall_bzz"
        case needsTopUp = "needs_top_up"
        case xdaiToSendDisplay = "xdai_to_send_display"
        case sufficientFunds = "sufficient_funds"
    }
}

/// Deep read-back propagation result, as returned by
/// `ant_storage_verify_propagation`. The daemon resolves the manifest,
/// enumerates the file's chunk tree, fetches every interior node
/// network-only, then probes a sample of the real data leaves across
/// distinct closest peers. This verifies the *actual data chunks* are
/// retrievable from the network — a much stronger signal than a job's
/// "Online" status (which only means the upload was *attempted*) or a
/// root-only reachability check.
struct PropagationInfo: Codable, Equatable {
    let reference: String
    /// The primary verdict: every checked chunk came back over the
    /// network's cache-free routed path — the *same* closest-first,
    /// forwarder-routed path a public gateway uses. So this is real
    /// "anyone can load it" retrievability, not a local-store shortcut.
    let retrievable: Bool
    /// The stricter neighbourhood-floor signal: every checked leaf is
    /// reachable from its *own* closest peers AND the sampled replication
    /// floor is at least one. Kept as a secondary detail only — a
    /// constrained mobile node frequently can't probe a chunk's closest
    /// peers even for a file well-connected nodes fetch instantly, so this
    /// is unreliable as the "is it safe?" verdict and must not gate it.
    /// Optional so a verdict from an older daemon still decodes.
    let fullyReplicated: Bool?
    /// Total chunks in the file's data tree (leaves + interior nodes).
    let totalChunks: UInt32
    let leafChunks: UInt32
    let intermediateChunks: UInt32
    /// Interior nodes (all) + sampled leaves actually probed.
    let checkedChunks: UInt32
    let retrievableChunks: UInt32
    let sampledLeaves: UInt32
    /// Leaves that exist but only landed shallow — reachable via the
    /// uploader's route, not from their own closest peers. Optional for
    /// backward compatibility with older daemon payloads.
    let shallowLeaves: UInt32?
    /// Minimum distinct-route count across sampled leaves — a
    /// replication floor.
    let sources: UInt32
    /// Present only when verification couldn't complete (e.g. the
    /// manifest or data root wasn't retrievable).
    let error: String?

    enum CodingKeys: String, CodingKey {
        case reference
        case retrievable
        case fullyReplicated = "fully_replicated"
        case totalChunks = "total_chunks"
        case leafChunks = "leaf_chunks"
        case intermediateChunks = "intermediate_chunks"
        case checkedChunks = "checked_chunks"
        case retrievableChunks = "retrievable_chunks"
        case sampledLeaves = "sampled_leaves"
        case shallowLeaves = "shallow_leaves"
        case sources
        case error
    }

    /// The user-facing verdict: is every part of the file actually
    /// retrievable from the network (by anyone, via the same forwarder
    /// routing a public gateway uses)? This — not the local neighbourhood
    /// replication floor — is what "safe" means. Gating on the floor cried
    /// wolf on files that are genuinely fine: a phone often can't reach a
    /// chunk's own closest peers even when the gateway fetches it in 0.6s.
    var isStoredSafely: Bool { retrievable }

    /// Stricter neighbourhood verdict, retained only as an informational
    /// detail. Falls back to `retrievable` for older daemon payloads.
    var isFullyReplicated: Bool { fullyReplicated ?? retrievable }

    /// Count of leaves that exist but only landed shallow.
    var shallowCount: UInt32 { shallowLeaves ?? 0 }

    /// Short, plain-language verdict for a file row badge. Two clear
    /// states only — the file is either safely stored or it isn't — so the
    /// user never has to interpret jargon like "at risk". Deliberately
    /// free of chunk counts (the check samples a bounded subset on large
    /// files, so a raw count reads as if most of the file went unchecked).
    var label: String {
        isStoredSafely ? "Stored safely" : "Not fully stored"
    }
}

/// A streaming progress update during a propagation verification, decoded
/// from the JSON lines `ant_storage_verify_propagation_progress` emits. The
/// `checking` phase carries `checked`/`total` leaf counts (a determinate
/// bar); the earlier `resolving`/`enumerating` phases carry neither.
struct VerifyProgress: Equatable {
    let phase: String
    let checked: Int?
    let total: Int?

    /// 0…1 once the checking phase reports counts; `nil` for the
    /// indeterminate early phases.
    var fraction: Double? {
        guard let checked, let total, total > 0 else { return nil }
        return min(1.0, Double(checked) / Double(total))
    }

    /// Friendly one-line status for the verification card. Covers both the
    /// verify phases (`resolving`/`enumerating`/`checking`) and the re-push
    /// phase (`repushing`).
    var label: String {
        switch phase {
        case "resolving": return "Resolving file…"
        case "enumerating": return "Mapping chunks…"
        case "checking":
            if let checked, let total { return "Checking chunks \(checked)/\(total)" }
            return "Checking the network…"
        case "sources":
            if let checked, let total { return "Checking replication \(checked)/\(total)" }
            return "Measuring replication…"
        case "repushing":
            if let checked, let total { return "Re-pushing chunks \(checked)/\(total)" }
            return "Re-pushing missing chunks…"
        default: return "Checking the network…"
        }
    }

    init(phase: String, checked: Int?, total: Int?) {
        self.phase = phase
        self.checked = checked
        self.total = total
    }

    /// Decode one progress line; `nil` if it isn't a recognisable update.
    init?(jsonLine: String) {
        guard let data = jsonLine.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let phase = obj["phase"] as? String else { return nil }
        self.phase = phase
        self.checked = (obj["checked"] as? NSNumber)?.intValue
        self.total = (obj["total"] as? NSNumber)?.intValue
    }
}

/// Remaining lifetime of the connected storage plan, as returned by
/// `ant_storage_validity`. Computed from the batch's on-chain remaining
/// balance and the current postage price, so it needs a chain RPC and is
/// fetched on demand (not on every status poll).
struct StorageValidity: Codable, Equatable {
    let enabled: Bool
    let remainingSeconds: UInt64
    let expiresUnix: UInt64

    enum CodingKeys: String, CodingKey {
        case enabled
        case remainingSeconds = "remaining_seconds"
        case expiresUnix = "expires_unix"
    }

    /// Coarse human duration for the storage card: "1 year" / "3 months" /
    /// "12 days" / "5 hours" / "Expired".
    var durationLabel: String {
        guard remainingSeconds > 0 else { return "Expired" }
        let days = remainingSeconds / 86_400
        switch days {
        case 0:
            let hours = remainingSeconds / 3_600
            return hours <= 1 ? "less than an hour" : "\(hours) hours"
        case 1: return "1 day"
        case 2...30: return "\(days) days"
        case 31...364:
            let months = days / 30
            return months == 1 ? "1 month" : "\(months) months"
        default:
            let years = days / 365
            return years == 1 ? "1 year" : "\(years) years"
        }
    }

    /// Date-only expiry label (e.g. "19 Sep 2026").
    var expiryDateLabel: String {
        guard expiresUnix > 0 else { return "—" }
        let f = DateFormatter()
        f.dateStyle = .medium
        f.timeStyle = .none
        return f.string(from: Date(timeIntervalSince1970: TimeInterval(expiresUnix)))
    }
}

/// Account identity, as returned by `ant_account_info`.
struct AccountInfo: Codable, Equatable {
    let ethAddress: String
    let overlay: String
    let peerId: String
    let agent: String

    enum CodingKeys: String, CodingKey {
        case ethAddress = "eth_address"
        case overlay
        case peerId = "peer_id"
        case agent
    }

    /// `0x1234… abcd` short form for display.
    var shortAddress: String {
        let a = ethAddress
        guard a.count > 12 else { return a }
        return "\(a.prefix(8))…\(a.suffix(4))"
    }
}

/// A storage plan the user can pick in Get Started. Maps a friendly
/// size/duration to the on-chain `depth` + `days` the FFI prices.
struct StoragePlanTier: Identifiable, Equatable {
    let id: String
    let title: String
    let depth: UInt8
    let days: UInt64
    /// Advertised SAFE upload limit — the amount of data the batch can
    /// reliably store without risking chunk eviction. This is Swarm's
    /// "effective volume" for the batch depth (the volume storable at a
    /// ≤0.1% failure rate), which is far below the theoretical
    /// `2^depth × 4 KiB`. We show this to the user instead of the
    /// misleading theoretical figure so a plan never promises more than
    /// it can durably hold. Effective volume per depth (unencrypted):
    /// d19 ≈ 112 MB, d20 ≈ 688 MB, d21 ≈ 2.60 GB, d22 ≈ 7.73 GB.
    let safeLimitBytes: UInt64

    /// Theoretical capacity (2^depth chunks × 4 KiB). Retained for
    /// reference; NOT shown to the user (see `safeLimitBytes`).
    var capacityBytes: UInt64 { (UInt64(1) << depth) &* StoragePlan.bytesPerChunk }

    /// Friendly duration label ("1 day" / "1 year").
    var durationLabel: String {
        switch days {
        case 1: return "1 day"
        case 2...30: return "\(days) days"
        case 31...364: return "\(days / 30) months"
        default: return days == 365 ? "1 year" : "\(days / 365) years"
        }
    }

    // Depths are chosen as the SMALLEST batch depth whose Swarm
    // "effective volume" (≤0.1% failure) covers the advertised safe
    // limit, so uploads up to the limit land durably without evicting
    // earlier chunks. Never advertise more than the effective volume of
    // the chosen depth, and never go below depth 19 (depth 17/18 have an
    // effective volume of only ~45 kB / ~6.7 MB).
    static let all: [StoragePlanTier] = [
        // depth 20 → effective ≈ 688 MB (comfortable margin over 100 MB).
        StoragePlanTier(id: "starter", title: "Starter",
                        depth: 20, days: 30,
                        safeLimitBytes: 100 * 1_000_000),
        // depth 21 → effective ≈ 2.60 GB (comfortably covers 1 GB).
        StoragePlanTier(id: "advanced", title: "Advanced",
                        depth: 21, days: 180,
                        safeLimitBytes: 1_000_000_000),
        // depth 22 → effective ≈ 7.73 GB (covers a 5 GB safe limit).
        StoragePlanTier(id: "plus", title: "Plus",
                        depth: 22, days: 365,
                        safeLimitBytes: 5 * 1_000_000_000),
    ]
}

/// Payment information for a plan, as returned by `ant_storage_quote`.
struct StorageQuote: Codable, Equatable {
    let depth: UInt8
    let days: UInt64
    let amountPerChunk: String
    let totalCostBzz: String
    /// One-time xBZZ this purchase also puts behind the node's chequebook
    /// so its cheques are backed ("0" once it is funded). Part of the
    /// all-in `xdaiToSendDisplay` the user is asked for, not of
    /// `totalCostBzz`.
    let settlementDepositPlur: String
    let settlementDepositBzz: String
    let capacityBytes: UInt64
    let accountBzzDisplay: String
    let accountXdai: String
    let accountXdaiDisplay: String
    let neededBzzDisplay: String
    let xdaiRequiredDisplay: String
    let xdaiToSendDisplay: String
    let sufficientFunds: Bool

    enum CodingKeys: String, CodingKey {
        case depth
        case days
        case amountPerChunk = "amount_per_chunk"
        case totalCostBzz = "total_cost_bzz"
        case settlementDepositPlur = "settlement_deposit_plur"
        case settlementDepositBzz = "settlement_deposit_bzz"
        case capacityBytes = "capacity_bytes"
        case accountBzzDisplay = "account_bzz_display"
        case accountXdai = "account_xdai"
        case accountXdaiDisplay = "account_xdai_display"
        case neededBzzDisplay = "needed_bzz_display"
        case xdaiRequiredDisplay = "xdai_required_display"
        case xdaiToSendDisplay = "xdai_to_send_display"
        case sufficientFunds = "sufficient_funds"
    }
}

enum DriveDecoder {
    static func jobs(from json: String) -> [UploadJob] {
        guard let data = json.data(using: .utf8),
              let list = try? JSONDecoder().decode(UploadJobList.self, from: data)
        else { return [] }
        return list.jobs
    }

    static func job(from json: String) -> UploadJob? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(UploadJob.self, from: data)
    }

    static func plan(from json: String) -> StoragePlan? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StoragePlan.self, from: data)
    }

    static func account(from json: String) -> AccountInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(AccountInfo.self, from: data)
    }

    static func settlement(from json: String) -> SettlementInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(SettlementInfo.self, from: data)
    }

    static func settlementDeposit(from json: String) -> SettlementDeposit? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(SettlementDeposit.self, from: data)
    }

    static func propagation(from json: String) -> PropagationInfo? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(PropagationInfo.self, from: data)
    }

    static func quote(from json: String) -> StorageQuote? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StorageQuote.self, from: data)
    }

    static func validity(from json: String) -> StorageValidity? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StorageValidity.self, from: data)
    }
}

/// Fetches the live xBZZ→USD spot price so plan costs can be shown in a
/// familiar currency. Best-effort: if the lookup fails the UI just omits
/// the USD figure rather than blocking.
enum PriceOracle {
    private struct CoinGecko: Decodable {
        let swarm: Price?
        enum CodingKeys: String, CodingKey { case swarm = "swarm-bzz" }
        struct Price: Decodable { let usd: Double }
    }

    static func bzzUsd() async -> Double? {
        let url = URL(string: "https://api.coingecko.com/api/v3/simple/price?ids=swarm-bzz&vs_currencies=usd")!
        guard let (data, _) = try? await URLSession.shared.data(from: url),
              let decoded = try? JSONDecoder().decode(CoinGecko.self, from: data)
        else { return nil }
        return decoded.swarm?.usd
    }

    /// Format a USD amount, scaling precision for tiny values so a
    /// fraction-of-a-cent test plan still shows something meaningful.
    static func formatUsd(_ value: Double) -> String {
        if value > 0, value < 0.01 { return String(format: "$%.4f", value) }
        return String(format: "$%.2f", value)
    }
}
