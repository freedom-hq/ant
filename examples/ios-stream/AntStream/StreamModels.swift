import Foundation

/// Swift mirrors of the JSON the FFI returns. Field names match the
/// `serde`-serialised Rust structs exactly, so plain `Codable` decoding
/// works without custom keys.
///
/// This is the storage/account subset of `examples/ios-drive`'s
/// `DriveModels.swift` — AntStream has no upload-job list, so the job /
/// propagation models are deliberately left out rather than ported dead.

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
    /// Free space the user actually has: total capacity minus what's been
    /// issued. This is the batch's true `remaining_total`, not
    /// `worstCaseRemainingChunks` — the latter is a pessimistic
    /// pre-flight budget that collapses to 0 as soon as a single bucket
    /// saturates, which is misleading as a free-space meter.
    var freeBytes: UInt64 { totalBytes - usedBytes }

    var usedFraction: Double {
        guard totalBytes > 0 else { return 0 }
        return min(1.0, Double(usedBytes) / Double(totalBytes))
    }

    /// "Running low" heuristic: less than 10% of the plan's capacity left.
    var isLow: Bool {
        guard totalCapacityChunks > 0 else { return false }
        return Double(issuedChunks) / Double(totalCapacityChunks) > 0.90
    }
}

/// How long a broadcast fits on the storage plan, from
/// `ant_publisher_capacity` (#176): what a minute of the broadcast
/// stamps against the room left in the plan's **fullest** bucket.
struct CapacityEstimate: Codable, Equatable {
    let batchId: String
    /// An immutable plan refuses uploads once full; a mutable one
    /// overwrites the oldest video instead, with no error at all.
    let immutable: Bool
    /// The node will stamp with the plan at all.
    let usable: Bool
    let chunksPerMinute: UInt64
    let chunksLeft: UInt64
    let minutesLeft: UInt64
    /// Under an hour left at this quality: warn before going live.
    let low: Bool

    enum CodingKeys: String, CodingKey {
        case immutable, usable, low
        case batchId = "batch_id"
        case chunksPerMinute = "chunks_per_minute"
        case chunksLeft = "chunks_left"
        case minutesLeft = "minutes_left"
    }

    /// Anything a broadcaster should hear about before going live. The
    /// one test both the Broadcast checklist and the going-live screen
    /// use, so they never disagree.
    var needsWarning: Bool { low || !immutable || !usable }

    /// "About 3 h 20 min", "About 12 min", "No room".
    var timeLabel: String {
        if minutesLeft == 0 { return "No room" }
        if minutesLeft < 60 { return "About \(minutesLeft) min" }
        let hours = minutesLeft / 60
        if hours >= 48 { return "About \(hours / 24) days" }
        let minutes = minutesLeft % 60
        return minutes == 0 ? "About \(hours) h" : "About \(hours) h \(minutes) min"
    }

    /// The "about N minutes left on this plan" line, for `quality`.
    func summary(quality: String) -> String {
        minutesLeft == 0
            ? "No room left on this plan for \(quality) video"
            : "\(timeLabel) of \(quality) left on this plan"
    }

    /// Why ``needsWarning``, in a sentence, or `nil`.
    var warning: String? {
        if !usable {
            return "Swarm won't accept this plan's stamps: every upload would be refused."
        }
        if !immutable {
            return low
                ? "This plan overwrites its oldest video once full: past that point "
                    + "a broadcast eats its own beginning, with no error."
                : "This plan overwrites its oldest video once full instead of refusing, "
                    + "so a broadcast longer than this would eat its own beginning."
        }
        if low {
            return "Past that, Swarm refuses the broadcast's uploads and its recording."
        }
        return nil
    }
}

/// Outbound-settlement status, as returned by
/// `ant_storage_settlement_status`. `enabled` is what lets published
/// segments actually reach the network: without a deployed chequebook the
/// node can't pay peers for pushsync and they stop accepting its chunks,
/// so a broadcast looks live locally but nobody can play it back.
struct SettlementInfo: Codable, Equatable {
    let enabled: Bool
    let chequebook: String?
}

/// `ant_swap_status`: whether the node pays peers with cheques right now
/// (otherwise the slower free tier), and the chequebook's figures in
/// PLUR: the deposit as last read from the chain, every cheque issued
/// from it, and what it can still pay out. Answered from memory, so it
/// can be polled every couple of seconds.
struct SwapStatus: Equatable {
    let paying: Bool
    let swapEnabled: Bool
    let chequebook: String?
    let depositedPlur: Decimal?
    let issuedPlur: Decimal?
    let availablePlur: Decimal?

    static func parse(_ json: String) -> SwapStatus? {
        guard let data = json.data(using: .utf8),
              let o = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { return nil }
        func plur(_ key: String) -> Decimal? {
            (o[key] as? String).flatMap { Decimal(string: $0) }
        }
        return SwapStatus(
            paying: o["paying"] as? Bool ?? false,
            swapEnabled: o["swap_enabled"] as? Bool ?? true,
            chequebook: o["chequebook"] as? String,
            depositedPlur: plur("deposited_plur"),
            issuedPlur: plur("issued_plur"),
            availablePlur: plur("available_plur")
        )
    }
}

/// One cheque payment to a peer, as seen between two polls.
struct Payment: Equatable, Identifiable {
    let id = UUID()
    let peer: String
    let plur: Decimal
    let at: Date
}

/// The live chequebook view: `swap`, plus what this app run has seen paid
/// (`sessionPlur`), the pace over the last minute (`perMinutePlur`, nil
/// until there is enough to measure) and the latest payments.
struct Credit: Equatable {
    let swap: SwapStatus
    let sessionPlur: Decimal
    let perMinutePlur: Decimal?
    let recent: [Payment]
}

/// PLUR as xBZZ with 6 decimals, rounded down; "< 0.000001" for dust.
func formatBzz(_ plur: Decimal) -> String {
    guard plur > 0 else { return "0 xBZZ" }
    var micro = plur / Decimal(sign: .plus, exponent: 10, significand: 1)
    var floored = Decimal()
    NSDecimalRound(&floored, &micro, 0, .down)
    guard floored > 0 else { return "< 0.000001 xBZZ" }
    var digits = NSDecimalNumber(decimal: floored).stringValue
    while digits.count < 7 { digits = "0" + digits }
    return "\(digits.dropLast(6)).\(digits.suffix(6)) xBZZ"
}

/// What actually stands behind this account's cheques, as returned by
/// `ant_storage_settlement_deposit`. `SettlementInfo` says a chequebook
/// exists; this says whether it is funded. A chequebook deployed with no
/// deposit signs cheques nobody can cash: publishing runs clean until the
/// peers' payment tolerance is used up, then collapses into pushsync
/// timeouts and unbounded live-edge lag — so the Storage tab detects that
/// here and offers a top-up.
struct SettlementDeposit: Codable, Equatable {
    /// False when this account has no chequebook yet — nothing to top up
    /// (buying or connecting a plan deploys one, funded) — or when its
    /// chequebook failed the chain check and settlement is off for it.
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

    /// Coarse human duration: "1 year" / "3 months" / "12 days" /
    /// "5 hours" / "Expired".
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
///
/// The tiers are sized for *video*, not documents: a 720p HLS broadcast
/// costs roughly 1 GB/hour, so the labels are quoted in hours of live
/// video rather than gigabytes. Depths are the smallest whose Swarm
/// "effective volume" (the volume storable at a ≤0.1% failure rate)
/// covers the advertised limit — d21 ≈ 2.60 GB, d22 ≈ 7.73 GB,
/// d23 ≈ 19.94 GB — so a plan never promises more than it can durably
/// hold.
struct StoragePlanTier: Identifiable, Equatable {
    let id: String
    let title: String
    let depth: UInt8
    let days: UInt64
    /// Advertised SAFE limit — see the note above; never the theoretical
    /// `2^depth × 4 KiB`.
    let safeLimitBytes: UInt64

    /// Rough hours of 720p live video the plan holds, at ~1 GB/hour.
    static let bytesPerVideoHour: UInt64 = 1_000_000_000

    var videoHours: UInt64 { max(1, safeLimitBytes / Self.bytesPerVideoHour) }

    var capacityLabel: String {
        let h = videoHours
        return "~\(h) \(h == 1 ? "hour" : "hours") of video"
    }

    /// Friendly duration label ("30 days" / "1 year").
    var durationLabel: String {
        switch days {
        case 1: return "1 day"
        case 2...30: return "\(days) days"
        case 31...364: return "\(days / 30) months"
        default: return days == 365 ? "1 year" : "\(days / 365) years"
        }
    }

    static let all: [StoragePlanTier] = [
        // depth 21 → effective ≈ 2.60 GB (comfortably covers 2 GB).
        StoragePlanTier(id: "starter", title: "Starter",
                        depth: 21, days: 30,
                        safeLimitBytes: 2 * 1_000_000_000),
        // depth 22 → effective ≈ 7.73 GB (covers 5 GB).
        StoragePlanTier(id: "creator", title: "Creator",
                        depth: 22, days: 180,
                        safeLimitBytes: 5 * 1_000_000_000),
        // depth 23 → effective ≈ 19.94 GB (covers 15 GB). 20 GB would
        // sit *above* the effective volume: a broadcaster who filled the
        // advertised hours would saturate a bucket and evict the start
        // of their own stream.
        StoragePlanTier(id: "studio", title: "Studio",
                        depth: 23, days: 365,
                        safeLimitBytes: 15 * 1_000_000_000),
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

/// Live progress of a publisher throughput benchmark, as returned by
/// `ant_bench_progress` (issue #67 stage 1).
struct BenchSnapshot: Codable, Equatable {
    let running: Bool
    let elapsedS: Double
    let configuredDurationS: UInt64
    let segmentsTotal: UInt64
    let segmentsOk: UInt64
    let segmentsFailed: UInt64
    let sustainedMbitS: Double
    let sustainedChunksS: Double
    let lagMsLast: UInt64
    let peers: UInt32

    enum CodingKeys: String, CodingKey {
        case running
        case elapsedS = "elapsed_s"
        case configuredDurationS = "configured_duration_s"
        case segmentsTotal = "segments_total"
        case segmentsOk = "segments_ok"
        case segmentsFailed = "segments_failed"
        case sustainedMbitS = "sustained_mbit_s"
        case sustainedChunksS = "sustained_chunks_s"
        case lagMsLast = "lag_ms_last"
        case peers
    }

    var progressFraction: Double {
        guard configuredDurationS > 0 else { return 0 }
        return min(1.0, elapsedS / Double(configuredDurationS))
    }
}

/// Final result of a benchmark run, as returned by `ant_bench_stop`.
/// Field-for-field the Rust `BenchReport`; `markdownRow` mirrors the
/// Rust renderer so a row copied off the phone drops straight into
/// `crates/ant-ffi/ANTSTREAM_BENCH.md` next to the CI rows.
struct BenchReport: Codable, Equatable {
    let label: String
    let mode: String
    let notes: String
    let targetBitrateKbps: UInt32
    let segmentMs: UInt32
    let segmentBytes: UInt64
    let maxInFlight: Int
    let configuredDurationS: UInt64
    let warmupS: UInt64
    let measuredS: Double
    let segmentsTotal: UInt64
    let segmentsOk: UInt64
    let segmentsFailed: UInt64
    /// Segments captured inside the measured (post-warm-up) window —
    /// the sample every figure below is computed from. Zero means the
    /// run was stopped before it left warm-up, so there is no verdict.
    let measuredSegmentsTotal: UInt64
    let measuredSegmentsOk: UInt64
    let sustainedMbitS: Double
    let sustainedChunksS: Double
    let publishMsP50: UInt64
    let publishMsP95: UInt64
    let publishMsMax: UInt64
    let lagMsP50: UInt64
    let lagMsP95: UInt64
    let lagMsMax: UInt64
    let lagMsFinal: UInt64
    let sustained: Bool
    let peersMin: UInt32
    let peersMax: UInt32
    let errors: [String]

    enum CodingKeys: String, CodingKey {
        case label, mode, notes, sustained, errors
        case targetBitrateKbps = "target_bitrate_kbps"
        case segmentMs = "segment_ms"
        case segmentBytes = "segment_bytes"
        case maxInFlight = "max_in_flight"
        case configuredDurationS = "configured_duration_s"
        case warmupS = "warmup_s"
        case measuredS = "measured_s"
        case segmentsTotal = "segments_total"
        case segmentsOk = "segments_ok"
        case segmentsFailed = "segments_failed"
        case measuredSegmentsTotal = "measured_segments_total"
        case measuredSegmentsOk = "measured_segments_ok"
        case sustainedMbitS = "sustained_mbit_s"
        case sustainedChunksS = "sustained_chunks_s"
        case publishMsP50 = "publish_ms_p50"
        case publishMsP95 = "publish_ms_p95"
        case publishMsMax = "publish_ms_max"
        case lagMsP50 = "lag_ms_p50"
        case lagMsP95 = "lag_ms_p95"
        case lagMsMax = "lag_ms_max"
        case lagMsFinal = "lag_ms_final"
        case peersMin = "peers_min"
        case peersMax = "peers_max"
    }

    static let markdownHeader = """
        | environment | mode | target | sustained | chunks/s | publish p50/p95/max ms | final lag ms | kept up |
        |---|---|---|---|---|---|---|---|
        """

    /// Whether the run produced a measured (post-warm-up) sample at
    /// all. Stopping inside `warmup_s` leaves none — every figure is
    /// then the zero an empty window folds to, and the run has no
    /// verdict to report either way. Mirrors Rust's `has_measurement`.
    var hasMeasurement: Bool { measuredSegmentsTotal > 0 }

    /// The **kept up** cell — three-state, matching Rust's `verdict()`:
    /// a run that measured nothing must not enter the go/no-go table as
    /// a pass *or* a failure.
    var verdict: String {
        guard hasMeasurement else { return "n/a (no measured window)" }
        return sustained ? "yes" : "**no**"
    }

    var markdownRow: String {
        let sustainedMbit = String(format: "%.2f", sustainedMbitS)
        let chunks = String(format: "%.1f", sustainedChunksS)
        return "| \(label) | \(mode) | \(targetBitrateKbps) kbit/s | \(sustainedMbit) Mbit/s "
            + "| \(chunks) | \(publishMsP50) / \(publishMsP95) / \(publishMsMax) "
            + "| \(lagMsFinal) | \(verdict) |"
    }

    /// The whole run, ready to paste into the issue: the table row plus
    /// the context a reader needs to trust it.
    var markdownBlock: String {
        var out = "\(Self.markdownHeader)\n\(markdownRow)\n"
        out += "\nRun: \(segmentsOk)/\(segmentsTotal) segments published, "
        out += "\(measuredSegmentsOk) inside the \(Int(measuredS)) s measured window "
        out += "(\(warmupS) s warm-up excluded), "
        out += "\(segmentBytes) B segments every \(segmentMs) ms, "
        out += "window \(maxInFlight), peers \(peersMin)–\(peersMax).\n"
        if !hasMeasurement {
            out += "Stopped before the warm-up ended: nothing was measured, "
            out += "so this run has no throughput number and no verdict.\n"
        }
        if !notes.isEmpty { out += "Notes: \(notes)\n" }
        if !errors.isEmpty { out += "Errors: \(errors.joined(separator: "; "))\n" }
        return out
    }
}

/// Live progress of a broadcast, as returned by
/// `ant_publisher_progress` (issue #67 stage 2).
struct PublisherSnapshot: Codable, Equatable {
    let running: Bool
    let elapsedS: Double
    let channel: String
    let topic: String
    let owner: String
    /// The feed manifest a viewer resolves the channel from — the one
    /// reference worth sharing.
    let channelReference: String
    let playlistReference: String
    /// `nil` until the node has read the channel's feed head.
    let feedIndex: UInt64?
    let segmentsPushed: UInt64
    let segmentsPublished: UInt64
    /// Of those, the ones that reached a published playlist — what a
    /// viewer could actually play.
    let segmentsListed: UInt64
    let segmentsFailed: UInt64
    /// Segments the live-edge discipline dropped rather than falling
    /// further behind. Visible on screen: a broadcast that is shedding
    /// is a broadcast the uplink can't carry.
    let segmentsDropped: UInt64
    let bytesPublished: UInt64
    let playlistsPublished: UInt64
    let publishMsP50: UInt64
    let publishMsP95: UInt64
    /// Live-edge lag: how far behind live a viewer is right now — the
    /// age of the newest segment a landed feed update made playable.
    /// The number the on-screen indicator shows. An age, not the
    /// latency of the last update that landed, so it keeps climbing
    /// while the feed is stuck rather than freezing at its last good
    /// value.
    let lagMs: UInt64
    /// Highest capture → playable latency any one update recorded.
    let lagMsMax: UInt64
    /// `lagMs` inside three segment durations — the same budget the
    /// stage-1 bench verdict uses.
    let keepingUp: Bool
    let sustainedMbitS: Double
    let peers: UInt32
    let lastError: String
    let errorCount: UInt64
    /// The network is gone and the broadcast is paused, not over
    /// (#172). Optional so a snapshot from a node without it decodes.
    let paused: Bool?
    /// Segments the camera shot while paused, never uploaded.
    let segmentsWithheld: UInt64?
    /// The broadcast is ending by itself: a minute passed with nothing
    /// reaching the playlist (`reap_ms`).
    let reaped: Bool?
    /// Why the crash-recovery entry (#175) could not be saved, empty
    /// while it saves fine. Optional so a node without it decodes.
    let recoveryError: String?
    /// Uploads the gateway refused because the storage plan's batch is
    /// full (bee's 402), and because the batch itself is unusable (#176).
    /// Not network failures: counted apart so the live screen can name
    /// the plan as the problem. Optional so a node without them decodes.
    let batchFullRefusals: UInt64?
    let batchUnusableRefusals: UInt64?
    /// The newest of those refusals, empty while there has been none.
    let batchError: String?

    enum CodingKeys: String, CodingKey {
        case running, channel, topic, owner, peers, paused, reaped
        case recoveryError = "recovery_error"
        case batchFullRefusals = "batch_full_refusals"
        case batchUnusableRefusals = "batch_unusable_refusals"
        case batchError = "batch_error"
        case segmentsWithheld = "segments_withheld"
        case elapsedS = "elapsed_s"
        case channelReference = "channel_reference"
        case playlistReference = "playlist_reference"
        case feedIndex = "feed_index"
        case segmentsPushed = "segments_pushed"
        case segmentsPublished = "segments_published"
        case segmentsListed = "segments_listed"
        case segmentsFailed = "segments_failed"
        case segmentsDropped = "segments_dropped"
        case bytesPublished = "bytes_published"
        case playlistsPublished = "playlists_published"
        case publishMsP50 = "publish_ms_p50"
        case publishMsP95 = "publish_ms_p95"
        case lagMs = "lag_ms"
        case lagMsMax = "lag_ms_max"
        case keepingUp = "keeping_up"
        case sustainedMbitS = "sustained_mbit_s"
        case lastError = "last_error"
        case errorCount = "error_count"
    }

    /// "Live", "2.4 s behind", or "Connecting…" before the first feed
    /// update lands. One place, so the badge and the detail row can
    /// never disagree about what the lag means.
    var lagLabel: String {
        guard playlistsPublished > 0 else { return "Connecting…" }
        return String(format: "%.1f s behind", Double(lagMs) / 1000.0)
    }

    /// What the storage plan is doing to this broadcast, if anything.
    var batchProblem: BatchProblem? {
        BatchProblem(full: batchFullRefusals ?? 0, unusable: batchUnusableRefusals ?? 0)
    }
}

/// A broadcast's uploads refused because of its storage plan rather
/// than the network (#176). One type for the live screen and the
/// summary, so both say the same thing about the same counts.
struct BatchProblem: Equatable {
    /// Refused because the plan is full (bee's 402).
    let full: UInt64
    /// Refused because the plan's batch is unusable (unknown, expired,
    /// not on chain).
    let unusable: UInt64

    init?(full: UInt64, unusable: UInt64) {
        guard full > 0 || unusable > 0 else { return nil }
        self.full = full
        self.unusable = unusable
    }

    /// Full outranks unusable: it is the one a long broadcast walks into.
    var title: String {
        full > 0 ? "Storage plan full" : "Storage plan refused"
    }

    var detail: String {
        let count = full > 0 ? full : unusable
        let uploads = count == 1 ? "1 upload" : "\(count) uploads"
        if full > 0 {
            return "Swarm refused \(uploads) because the plan has no room left. "
                + "Viewers miss what is refused, and the recording may not be "
                + "kept. Extend or buy a bigger plan before the next broadcast."
        }
        return "Swarm won't accept this plan's stamps (\(uploads) refused): "
            + "it is unknown to the network or has expired. Check the plan "
            + "on the Storage tab."
    }
}

/// A broadcast the app was killed or crashed in the middle of (#175):
/// the publisher's recovery entry (`recovery_path`), read back at
/// launch. Only what the Broadcast tab shows; resuming hands the node
/// the file itself, and the node checks it is this account's feed.
struct UnfinishedBroadcast: Equatable {
    let channel: String
    /// The feed's owner, 40 lowercase hex.
    let owner: String
    let broadcastId: String
    /// When the entry was last written: ahead of the broadcast's last
    /// feed update before the app went.
    let savedAt: Date
    /// Playable segments it holds (gaps left out), and their media.
    let segments: Int
    let seconds: Double
    /// A stop had begun closing it (the entry's `shape` is past "live"):
    /// its `#EXT-X-ENDLIST` window or recording may already be the head,
    /// and live viewers have stopped there, so it can only be ended.
    let ended: Bool

    /// Decode a recovery entry, or `nil` for one this build can't read
    /// (left on disk all the same: it is the only record of the
    /// broadcast).
    static func decode(_ data: Data) -> UnfinishedBroadcast? {
        guard let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              obj["version"] as? Int == 1,
              let channel = obj["channel"] as? String,
              let owner = obj["owner"] as? String,
              let id = obj["broadcast_id"] as? String,
              let savedMs = (obj["saved_at_ms"] as? NSNumber)?.doubleValue,
              let recording = obj["recording"] as? [[String: Any]]
        else { return nil }
        let media = recording.filter { ($0["gap"] as? Bool) != true }
        let ms = media.reduce(0.0) { $0 + ((($1["duration_ms"] as? NSNumber)?.doubleValue) ?? 0) }
        return UnfinishedBroadcast(
            channel: channel,
            owner: owner.lowercased(),
            broadcastId: id,
            savedAt: Date(timeIntervalSince1970: savedMs / 1000),
            segments: media.count,
            seconds: ms / 1000,
            ended: (obj["shape"] as? String ?? "live") != "live"
        )
    }

    /// Recent enough to carry on as the same broadcast: the app came back
    /// inside the reap window (a minute), the time a viewer would have
    /// waited for it anyway. Older, it can only be ended into its
    /// recording. An entry a stop had begun closing is never resumed
    /// (the node only finalizes it).
    func canResume(now: Date = Date(), reapMs: UInt64) -> Bool {
        !ended && now.timeIntervalSince(savedAt) * 1000 < Double(reapMs)
    }

    var durationLabel: String {
        let total = Int(seconds.rounded())
        return String(format: "%d:%02d", total / 60, total % 60)
    }
}

/// Final result of a broadcast, as returned by `ant_publisher_stop`.
struct PublisherReport: Codable, Equatable {
    let channel: String
    let topic: String
    let owner: String
    let channelReference: String
    let playlistReference: String
    /// The whole broadcast as one VOD playlist, the channel's final feed
    /// update (issue #168), playable by itself without the feed. Empty
    /// when its upload failed; optional so a report from a node without
    /// it still decodes.
    let recordingReference: String?
    let notes: String
    let targetBitrateKbps: UInt32
    let segmentMs: UInt32
    let maxInFlight: Int
    let durationS: Double
    let segmentsPushed: UInt64
    let segmentsPublished: UInt64
    /// Of those, the ones that reached a published playlist. This — not
    /// `segmentsPublished` — is what `keptUp` counts.
    let segmentsListed: UInt64
    let segmentsFailed: UInt64
    let segmentsDropped: UInt64
    let bytesPublished: UInt64
    let chunksPublished: UInt64
    let playlistsPublished: UInt64
    let feedUpdates: UInt64
    /// The next feed update's index: where the permanent channel's next
    /// broadcast starts (the node finds it by reading the feed head).
    /// `nil` when the broadcast ended before it read the head, and so a
    /// report from a node without it still decodes.
    let feedIndex: UInt64?
    let sustainedMbitS: Double
    let sustainedChunksS: Double
    let publishMsP50: UInt64
    let publishMsP95: UInt64
    let publishMsMax: UInt64
    let lagMsP50: UInt64
    let lagMsP95: UInt64
    let lagMsMax: UInt64
    /// The live edge where the broadcast ended: the age of the newest
    /// playable segment at stop, frozen there.
    let lagMsFinal: UInt64
    let keptUp: Bool
    let errors: [String]
    let errorCount: UInt64
    /// Segments shot while the network was gone, never uploaded (#172).
    let segmentsWithheld: UInt64?
    /// It ended by itself after a minute with nothing reaching the
    /// playlist, not on the stop button.
    let reaped: Bool?
    /// See ``PublisherSnapshot/batchFullRefusals`` (#176).
    let batchFullRefusals: UInt64?
    let batchUnusableRefusals: UInt64?
    let batchError: String?

    enum CodingKeys: String, CodingKey {
        case channel, topic, owner, notes, errors, reaped
        case batchFullRefusals = "batch_full_refusals"
        case batchUnusableRefusals = "batch_unusable_refusals"
        case batchError = "batch_error"
        case segmentsWithheld = "segments_withheld"
        case channelReference = "channel_reference"
        case playlistReference = "playlist_reference"
        case recordingReference = "recording_reference"
        case targetBitrateKbps = "target_bitrate_kbps"
        case segmentMs = "segment_ms"
        case maxInFlight = "max_in_flight"
        case durationS = "duration_s"
        case segmentsPushed = "segments_pushed"
        case segmentsPublished = "segments_published"
        case segmentsListed = "segments_listed"
        case segmentsFailed = "segments_failed"
        case segmentsDropped = "segments_dropped"
        case bytesPublished = "bytes_published"
        case chunksPublished = "chunks_published"
        case playlistsPublished = "playlists_published"
        case feedUpdates = "feed_updates"
        case feedIndex = "feed_index"
        case sustainedMbitS = "sustained_mbit_s"
        case sustainedChunksS = "sustained_chunks_s"
        case publishMsP50 = "publish_ms_p50"
        case publishMsP95 = "publish_ms_p95"
        case publishMsMax = "publish_ms_max"
        case lagMsP50 = "lag_ms_p50"
        case lagMsP95 = "lag_ms_p95"
        case lagMsMax = "lag_ms_max"
        case lagMsFinal = "lag_ms_final"
        case keptUp = "kept_up"
        case errorCount = "error_count"
    }

    var durationLabel: String {
        let total = Int(durationS.rounded())
        return String(format: "%d:%02d", total / 60, total % 60)
    }

    /// Whether `keptUp` is a verdict about anything. It is a conjunction
    /// whose first clause is "at least one segment published", so a
    /// broadcast that played nothing reports `keptUp == false` — and
    /// rendering that as "fell behind" would blame the uplink for a
    /// broadcast that never started. Mirrors the first clause of Rust's
    /// `PublisherReport::kept_up` **verbatim**, including its use of
    /// `segments_listed` rather than `segments_published`; the same
    /// three-state treatment `BenchReport.hasMeasurement` gives the
    /// throughput verdict.
    var hasVerdict: Bool { segmentsListed > 0 }

    var batchProblem: BatchProblem? {
        BatchProblem(full: batchFullRefusals ?? 0, unusable: batchUnusableRefusals ?? 0)
    }

    var verdictLabel: String {
        if reaped == true { return "Broadcast ended: nothing reached viewers for a minute" }
        guard hasVerdict else { return "Broadcast ended without publishing" }
        return keptUp ? "Broadcast kept up" : "Broadcast fell behind"
    }
}

enum StreamDecoder {
    static func publisherSnapshot(from json: String) -> PublisherSnapshot? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(PublisherSnapshot.self, from: data)
    }

    static func capacity(from json: String) -> CapacityEstimate? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(CapacityEstimate.self, from: data)
    }

    static func publisherReport(from json: String) -> PublisherReport? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(PublisherReport.self, from: data)
    }

    static func benchSnapshot(from json: String) -> BenchSnapshot? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(BenchSnapshot.self, from: data)
    }

    static func benchReport(from json: String) -> BenchReport? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(BenchReport.self, from: data)
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

    static func quote(from json: String) -> StorageQuote? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StorageQuote.self, from: data)
    }

    static func validity(from json: String) -> StorageValidity? {
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(StorageValidity.self, from: data)
    }
}

/// xDAI is a USD stablecoin (~$1), so plan prices are shown in dollars
/// straight from the xDAI figure. Kept as a named helper so every call
/// site formats identically.
enum PriceOracle {
    /// Format a USD amount, scaling precision for tiny values so a
    /// fraction-of-a-cent test plan still shows something meaningful.
    static func formatUsd(_ value: Double) -> String {
        if value > 0, value < 0.01 { return String(format: "$%.4f", value) }
        return String(format: "$%.2f", value)
    }

    static func usdFromXdai(_ xdai: String) -> String? {
        guard let amount = Double(xdai) else { return nil }
        return formatUsd(amount)
    }

    /// Round a funding amount up to the next whole cent (0.01 xDAI) so
    /// the user sends a clean figure with a hair of headroom — never less
    /// than the quote requires. The small epsilon keeps a value already
    /// on a cent boundary from being bumped a cent by float error.
    static func roundedUpXdai(_ xdai: String) -> String {
        guard let amount = Double(xdai) else { return xdai }
        let cents = (amount * 100 - 1e-6).rounded(.up)
        return String(format: "%.2f", max(0, cents) / 100)
    }
}
