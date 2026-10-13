package at.vibing.ant.stream

import java.math.BigInteger
import org.json.JSONObject

// JSON documents returned by the FFI, mirroring
// `examples/ios-stream/AntStream/StreamModels.swift`.

data class StoragePlan(
    val enabled: Boolean,
    val batchId: String,
    val batchDepth: Int,
    val totalCapacityChunks: Long,
    val issuedChunks: Long,
) {
    val totalBytes get() = totalCapacityChunks * BYTES_PER_CHUNK
    val usedBytes get() = issuedChunks * BYTES_PER_CHUNK
    val usedFraction get() = if (totalBytes > 0) (usedBytes.toDouble() / totalBytes).coerceAtMost(1.0) else 0.0

    companion object {
        const val BYTES_PER_CHUNK = 4096L

        fun parse(json: String): StoragePlan? = runCatching {
            val o = JSONObject(json)
            StoragePlan(
                enabled = o.optBoolean("enabled"),
                batchId = o.optString("batch_id"),
                batchDepth = o.optInt("batch_depth"),
                totalCapacityChunks = o.optLong("total_capacity_chunks"),
                issuedChunks = o.optLong("issued_chunks"),
            )
        }.getOrNull()
    }
}

data class SettlementInfo(val enabled: Boolean, val chequebook: String?) {
    companion object {
        fun parse(json: String): SettlementInfo? = runCatching {
            val o = JSONObject(json)
            SettlementInfo(o.optBoolean("enabled"), o.optString("chequebook").ifEmpty { null })
        }.getOrNull()
    }
}

/**
 * `ant_swap_status`: whether the node pays peers with cheques right now
 * (otherwise the slower free tier), and the chequebook's figures in PLUR:
 * the deposit as last read from the chain, every cheque issued from it,
 * and what it can still pay out. Answered from memory, so it can be
 * polled every couple of seconds.
 */
data class SwapStatus(
    val paying: Boolean,
    val swapEnabled: Boolean,
    val chequebook: String?,
    val depositedPlur: BigInteger?,
    val issuedPlur: BigInteger?,
    val availablePlur: BigInteger?,
) {
    companion object {
        fun parse(json: String): SwapStatus? = runCatching {
            val o = JSONObject(json)
            fun plur(key: String) = if (o.isNull(key)) null else o.optString(key).toBigIntegerOrNull()
            SwapStatus(
                paying = o.optBoolean("paying"),
                swapEnabled = o.optBoolean("swap_enabled", true),
                chequebook = if (o.isNull("chequebook")) null else o.optString("chequebook").ifEmpty { null },
                depositedPlur = plur("deposited_plur"),
                issuedPlur = plur("issued_plur"),
                availablePlur = plur("available_plur"),
            )
        }.getOrNull()
    }
}

/** One cheque payment to a peer, as seen between two polls. */
data class Payment(val peer: String, val plur: BigInteger, val atMillis: Long)

/**
 * The live chequebook view: [swap], plus what this app run has seen paid
 * ([sessionPlur]), the pace over the last minute ([perMinutePlur], null
 * until there is enough to measure) and the latest payments.
 */
data class Credit(
    val swap: SwapStatus,
    val sessionPlur: BigInteger,
    val perMinutePlur: BigInteger?,
    val recent: List<Payment>,
)

private val PLUR_PER_BZZ = BigInteger.TEN.pow(16)

/** PLUR as xBZZ with 6 decimals, rounded down; "< 0.000001" for dust. */
fun formatBzz(plur: BigInteger): String {
    if (plur.signum() <= 0) return "0 xBZZ"
    val micro = plur.multiply(BigInteger.valueOf(1_000_000)).divide(PLUR_PER_BZZ)
    if (micro.signum() == 0) return "< 0.000001 xBZZ"
    val s = micro.toString().padStart(7, '0')
    return "${s.dropLast(6)}.${s.takeLast(6)} xBZZ"
}

data class StorageValidity(val enabled: Boolean, val remainingSeconds: Long, val expiresUnix: Long) {
    val durationLabel: String
        get() {
            if (remainingSeconds <= 0) return "Expired"
            val days = remainingSeconds / 86_400
            return when {
                days == 0L -> (remainingSeconds / 3_600).let { if (it <= 1) "less than an hour" else "$it hours" }
                days == 1L -> "1 day"
                days <= 30 -> "$days days"
                days < 365 -> (days / 30).let { if (it == 1L) "1 month" else "$it months" }
                else -> (days / 365).let { if (it == 1L) "1 year" else "$it years" }
            }
        }

    companion object {
        fun parse(json: String): StorageValidity? = runCatching {
            val o = JSONObject(json)
            StorageValidity(o.optBoolean("enabled"), o.optLong("remaining_seconds"), o.optLong("expires_unix"))
        }.getOrNull()
    }
}

data class AccountInfo(val ethAddress: String, val overlay: String, val peerId: String, val agent: String) {
    companion object {
        fun parse(json: String): AccountInfo? = runCatching {
            val o = JSONObject(json)
            AccountInfo(o.optString("eth_address"), o.optString("overlay"), o.optString("peer_id"), o.optString("agent"))
        }.getOrNull()
    }
}

data class StorageQuote(
    val depth: Int,
    val days: Long,
    val amountPerChunk: String,
    val totalCostBzz: String,
    val settlementDepositPlur: String,
    val settlementDepositBzz: String,
    val accountXdaiDisplay: String,
    val xdaiToSendDisplay: String,
    val sufficientFunds: Boolean,
) {
    companion object {
        fun parse(json: String): StorageQuote? = runCatching {
            val o = JSONObject(json)
            StorageQuote(
                depth = o.optInt("depth"),
                days = o.optLong("days"),
                amountPerChunk = o.getString("amount_per_chunk"),
                totalCostBzz = o.optString("total_cost_bzz"),
                settlementDepositPlur = o.optString("settlement_deposit_plur", "0"),
                settlementDepositBzz = o.optString("settlement_deposit_bzz"),
                accountXdaiDisplay = o.optString("account_xdai_display"),
                xdaiToSendDisplay = o.optString("xdai_to_send_display"),
                sufficientFunds = o.optBoolean("sufficient_funds"),
            )
        }.getOrNull()
    }
}

/** A plan on the Get-started screen; same tiers as the iOS app. */
data class StoragePlanTier(val id: String, val title: String, val depth: Int, val days: Long, val safeLimitBytes: Long) {
    val capacityLabel: String
        get() {
            val h = maxOf(1L, safeLimitBytes / BYTES_PER_VIDEO_HOUR)
            return "~$h ${if (h == 1L) "hour" else "hours"} of video"
        }
    val durationLabel: String
        get() = when {
            days == 1L -> "1 day"
            days <= 30 -> "$days days"
            days < 365 -> "${days / 30} months"
            days == 365L -> "1 year"
            else -> "${days / 365} years"
        }

    companion object {
        /** Planning figure: ~1 GB per hour of 720p video. */
        const val BYTES_PER_VIDEO_HOUR = 1_000_000_000L

        val all = listOf(
            StoragePlanTier("starter", "Starter", 21, 30, 2 * 1_000_000_000L),
            StoragePlanTier("creator", "Creator", 22, 180, 5 * 1_000_000_000L),
            StoragePlanTier("studio", "Studio", 23, 365, 15 * 1_000_000_000L),
        )
    }
}

data class PublisherSnapshot(
    val running: Boolean,
    val elapsedS: Double,
    val channelReference: String,
    val playlistReference: String,
    val feedIndex: Long,
    val segmentsPushed: Long,
    val segmentsPublished: Long,
    val segmentsFailed: Long,
    val segmentsDropped: Long,
    val bytesPublished: Long,
    val playlistsPublished: Long,
    val lagMs: Long,
    val keepingUp: Boolean,
    val sustainedMbitS: Double,
    val peers: Int,
    val lastError: String,
    val errorCount: Long,
) {
    val lagLabel: String
        get() = if (playlistsPublished <= 0) "Connecting…" else String.format("%.1f s behind", lagMs / 1000.0)

    companion object {
        fun parse(json: String): PublisherSnapshot? = runCatching {
            val o = JSONObject(json)
            PublisherSnapshot(
                running = o.optBoolean("running"),
                elapsedS = o.optDouble("elapsed_s", 0.0),
                channelReference = o.optString("channel_reference"),
                playlistReference = o.optString("playlist_reference"),
                feedIndex = o.optLong("feed_index"),
                segmentsPushed = o.optLong("segments_pushed"),
                segmentsPublished = o.optLong("segments_published"),
                segmentsFailed = o.optLong("segments_failed"),
                segmentsDropped = o.optLong("segments_dropped"),
                bytesPublished = o.optLong("bytes_published"),
                playlistsPublished = o.optLong("playlists_published"),
                lagMs = o.optLong("lag_ms"),
                keepingUp = o.optBoolean("keeping_up"),
                sustainedMbitS = o.optDouble("sustained_mbit_s", 0.0),
                peers = o.optInt("peers"),
                lastError = o.optString("last_error"),
                errorCount = o.optLong("error_count"),
            )
        }.getOrNull()
    }
}

data class PublisherReport(
    val channelReference: String,
    val feedIndex: Long,
    val durationS: Double,
    val segmentsPushed: Long,
    val segmentsListed: Long,
    val segmentsFailed: Long,
    val segmentsDropped: Long,
    val bytesPublished: Long,
    val sustainedMbitS: Double,
    val lagMsP50: Long,
    val lagMsMax: Long,
    val keptUp: Boolean,
    val errors: List<String>,
) {
    companion object {
        fun parse(json: String): PublisherReport? = runCatching {
            val o = JSONObject(json)
            val errs = o.optJSONArray("errors")
            PublisherReport(
                channelReference = o.optString("channel_reference"),
                feedIndex = o.optLong("feed_index"),
                durationS = o.optDouble("duration_s", 0.0),
                segmentsPushed = o.optLong("segments_pushed"),
                segmentsListed = o.optLong("segments_listed"),
                segmentsFailed = o.optLong("segments_failed"),
                segmentsDropped = o.optLong("segments_dropped"),
                bytesPublished = o.optLong("bytes_published"),
                sustainedMbitS = o.optDouble("sustained_mbit_s", 0.0),
                lagMsP50 = o.optLong("lag_ms_p50"),
                lagMsMax = o.optLong("lag_ms_max"),
                keptUp = o.optBoolean("kept_up"),
                errors = (0 until (errs?.length() ?: 0)).map { errs!!.optString(it) },
            )
        }.getOrNull()
    }
}

fun formatBytes(bytes: Long): String {
    val units = listOf("B", "KB", "MB", "GB", "TB")
    var v = bytes.toDouble()
    var i = 0
    while (v >= 1000 && i < units.lastIndex) {
        v /= 1000
        i++
    }
    return if (i == 0) "$bytes B" else String.format("%.1f %s", v, units[i])
}
