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

/// Outbound-settlement status, as returned by
/// `ant_storage_settlement_status`. `enabled` is what lets published
/// segments actually reach the network: without a deployed chequebook the
/// node can't pay peers for pushsync and they stop accepting its chunks,
/// so a broadcast looks live locally but nobody can play it back.
struct SettlementInfo: Codable, Equatable {
    let enabled: Bool
    let chequebook: String?
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

enum StreamDecoder {
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
