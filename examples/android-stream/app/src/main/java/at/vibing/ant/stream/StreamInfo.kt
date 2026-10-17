package at.vibing.ant.stream

import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.json.JSONArray
import org.json.JSONObject

/**
 * The broadcaster's stream info (issue #220): what the publisher's
 * `"info"` object carries and the catalog entry shows. Every field is
 * optional for the library; the app requires a title and fills it with
 * the channel's name. [thumbnail] is the 64-hex reference
 * `ant_stream_thumbnail_upload` returned.
 */
data class StreamInfo(
    val title: String = "",
    val description: String = "",
    val thumbnail: String? = null,
    val scheduledStartMs: Long? = null,
) {
    /**
     * The `"info"` object for the publisher config, `ant_publisher_set_info`
     * and `ant_stream_catalog_publish`. Blank fields are left out (an empty
     * title is the channel's name), and a thumbnail that is not a 64-hex
     * reference too, since the library refuses one.
     */
    fun toJson(): JSONObject = JSONObject().apply {
        title.trim().takeIf { it.isNotEmpty() }?.let { put("title", it) }
        description.trim().takeIf { it.isNotEmpty() }?.let { put("description", it) }
        thumbnail?.takeIf(::isReference)?.let { put("thumbnail", it.lowercase()) }
        scheduledStartMs?.let { put("scheduled_start_ms", it) }
    }

    companion object {
        private val REFERENCE = Regex("^[0-9a-fA-F]{64}$")

        fun isReference(value: String): Boolean = REFERENCE.matches(value)
    }
}

/**
 * [StreamInfo] as the app remembers it per account (in the channel's
 * preferences, [Channel.prefsName]), with what the editor needs besides:
 *
 * - [thumbnailPicked]: the broadcaster chose the photo, so no frame is
 *   captured from the camera to replace it;
 * - [thumbnailPending]: that photo is saved on the phone
 *   ([Channel.thumbnailFile]) but not uploaded yet (no storage plan when
 *   it was picked); it goes up at Go live;
 * - [announcedStartMs]: the scheduled start last announced with
 *   `ant_stream_catalog_publish`, so the Broadcast screen can tell an
 *   announced time from one only set in the editor.
 */
data class SavedStreamInfo(
    val info: StreamInfo = StreamInfo(),
    val thumbnailPicked: Boolean = false,
    val thumbnailPending: Boolean = false,
    val announcedStartMs: Long? = null,
) {
    /** Whether the scheduled start has been announced as it is now. */
    val announced: Boolean get() = info.scheduledStartMs != null && announcedStartMs == info.scheduledStartMs

    /**
     * The info a broadcast or an announcement goes out with: the title
     * falls back to the channel's name, and a photo still waiting for its
     * upload is no thumbnail yet.
     */
    fun published(channelName: String): StreamInfo = info.copy(
        title = info.title.trim().ifEmpty { channelName.trim() },
        thumbnail = if (thumbnailPending) null else info.thumbnail,
    )

    /** After a broadcast: the announcement it fulfilled is spent, the rest stays for the next one. */
    fun afterBroadcast(): SavedStreamInfo = copy(info = info.copy(scheduledStartMs = null), announcedStartMs = null)

    fun toPrefsJson(): String = JSONObject().apply {
        put("title", info.title)
        put("description", info.description)
        info.thumbnail?.let { put("thumbnail", it) }
        info.scheduledStartMs?.let { put("scheduled_start_ms", it) }
        put("thumbnail_picked", thumbnailPicked)
        put("thumbnail_pending", thumbnailPending)
        announcedStartMs?.let { put("announced_start_ms", it) }
    }.toString()

    companion object {
        /** What [toPrefsJson] wrote; anything unreadable is the empty info. */
        fun fromPrefsJson(json: String?): SavedStreamInfo {
            if (json.isNullOrEmpty()) return SavedStreamInfo()
            val o = runCatching { JSONObject(json) }.getOrNull() ?: return SavedStreamInfo()
            return SavedStreamInfo(
                info = StreamInfo(
                    title = o.optString("title"),
                    description = o.optString("description"),
                    thumbnail = o.optString("thumbnail").takeIf(StreamInfo::isReference),
                    scheduledStartMs = longOrNull(o, "scheduled_start_ms"),
                ),
                thumbnailPicked = o.optBoolean("thumbnail_picked"),
                thumbnailPending = o.optBoolean("thumbnail_pending"),
                announcedStartMs = longOrNull(o, "announced_start_ms"),
            )
        }

        private fun longOrNull(o: JSONObject, key: String): Long? =
            (o.opt(key) as? Number)?.toLong()
    }
}

/**
 * The config `ant_stream_catalog_publish` takes to announce a scheduled
 * broadcast: the publisher config's gateway, batch, channel name and
 * topic text, the info (its `scheduled_start_ms` required), and the rung
 * the broadcast will have.
 */
fun catalogPublishConfig(
    gateway: String,
    batchId: String,
    channel: String,
    info: StreamInfo,
    width: Int,
    height: Int,
    bitrateKbps: Int,
): JSONObject = JSONObject()
    .put("gateway", gateway)
    .put("batch_id", batchId)
    .put("channel", channel)
    .put("topic_text", ChannelAddress.TOPIC)
    .put("info", info.toJson())
    .put("width", width)
    .put("height", height)
    .put("bitrate_kbps", bitrateKbps)

/** The size to scale a `width`×`height` image to so its long side is at most [maxLong]. */
fun thumbnailSize(width: Int, height: Int, maxLong: Int = THUMBNAIL_LONG_SIDE): Pair<Int, Int> {
    if (width <= 0 || height <= 0) return 0 to 0
    val long = maxOf(width, height)
    if (long <= maxLong) return width to height
    val scale = maxLong.toDouble() / long
    return maxOf(1, Math.round(width * scale).toInt()) to maxOf(1, Math.round(height * scale).toInt())
}

const val THUMBNAIL_LONG_SIDE = 640
const val THUMBNAIL_JPEG_QUALITY = 70

/**
 * One entry of an account's catalog feed, as the Watch tab shows it. The
 * catalog is untrusted JSON: every field may be missing or of another
 * type, and is then null (or empty for [title]).
 */
data class CatalogEntry(
    val owner: String,
    val topic: String,
    val title: String,
    val description: String?,
    val state: String?,
    val thumbnail: String?,
    val scheduledStartTime: String?,
) {
    /** The scheduled start in Unix ms, when the entry names a readable one. */
    val scheduledStartMs: Long? get() = scheduledStartTime?.let(::parseIsoMillis)

    /** "Starts at …" for a scheduled entry, null for any other. */
    fun startsAt(zone: ZoneId = ZoneId.systemDefault(), locale: Locale = Locale.getDefault(), nowMs: Long = System.currentTimeMillis()): String? {
        if (state != "scheduled") return null
        return formatStartsAt(scheduledStartMs ?: return null, zone, locale, nowMs)
    }
}

/**
 * Reading an account's catalog feed (topic text [TOPIC], owner the
 * account), the feed the publisher keeps one entry per channel in.
 */
object Catalog {
    const val TOPIC = "antstream-catalog"

    /** keccak256("antstream-catalog"), hashed like the channel's topic text. */
    val topicHex: String by lazy { Keccak.topicFromString(TOPIC) }

    /** Where the in-process gateway serves the head of `owner`'s catalog. */
    fun url(gateway: String, owner: String): String =
        "${gateway.trimEnd('/')}/feeds/${normalizeOwner(owner)}/$topicHex"

    /** Every readable entry of a catalog body; empty for anything that is not a JSON array. */
    fun parse(body: String): List<CatalogEntry> {
        val array = runCatching { JSONArray(body.trim()) }.getOrNull() ?: return emptyList()
        return (0 until array.length()).mapNotNull { i -> entryOf(array.opt(i) as? JSONObject ?: return@mapNotNull null) }
    }

    /** The entry for channel `owner` / topic text `topic`, or null. */
    fun select(entries: List<CatalogEntry>, owner: String, topic: String): CatalogEntry? {
        val o = normalizeOwner(owner)
        return entries.lastOrNull { normalizeOwner(it.owner) == o && it.topic == topic }
    }

    /** [select] over [parse]. */
    fun entryFor(body: String, owner: String, topic: String): CatalogEntry? = select(parse(body), owner, topic)

    /**
     * Read the channel's entry from the owner's catalog feed. Null when
     * the catalog is missing (404), unreadable or has no entry for it;
     * throws [IOException] when the gateway could not be asked at all, so
     * the caller can keep what it showed. Blocking.
     */
    fun fetch(gateway: String, channel: ChannelAddress, timeoutMs: Int = 30_000): CatalogEntry? {
        if (channel.rawTopic) return null
        val conn = URL(url(gateway, channel.owner)).openConnection() as HttpURLConnection
        return try {
            conn.connectTimeout = 10_000
            conn.readTimeout = timeoutMs
            conn.useCaches = false
            when (val code = conn.responseCode) {
                200 -> {
                    // A catalog is a few KB; never read a large body.
                    val body = conn.inputStream.use { it.readNBytesCompat(MAX_BODY) }
                    entryFor(String(body, Charsets.UTF_8), channel.owner, channel.topic)
                }
                in 400..499 -> null
                else -> throw IOException("catalog read: $code")
            }
        } finally {
            conn.disconnect()
        }
    }

    private const val MAX_BODY = 256 * 1024

    private fun java.io.InputStream.readNBytesCompat(max: Int): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        val buf = ByteArray(8192)
        while (out.size() < max) {
            val n = read(buf, 0, minOf(buf.size, max - out.size()))
            if (n < 0) break
            out.write(buf, 0, n)
        }
        return out.toByteArray()
    }

    private fun entryOf(o: JSONObject): CatalogEntry? {
        val owner = o.opt("owner") as? String ?: return null
        val topic = o.opt("topic") as? String ?: return null
        return CatalogEntry(
            owner = owner,
            topic = topic,
            title = (o.opt("title") as? String)?.trim().orEmpty(),
            description = (o.opt("description") as? String)?.trim()?.takeIf { it.isNotEmpty() },
            state = o.opt("state") as? String,
            thumbnail = (o.opt("thumbnail") as? String)?.takeIf(StreamInfo::isReference)?.lowercase(),
            scheduledStartTime = o.opt("scheduledStartTime") as? String,
        )
    }

    private fun normalizeOwner(owner: String): String = owner.trim().removePrefix("0x").removePrefix("0X").lowercase()
}

/** An ISO 8601 date-time with an offset (`2026-10-17T08:00:00.000Z`) in Unix ms, or null. */
fun parseIsoMillis(text: String): Long? =
    runCatching { Instant.parse(text.trim()).toEpochMilli() }.getOrNull()
        ?: runCatching { OffsetDateTime.parse(text.trim()).toInstant().toEpochMilli() }.getOrNull()

/**
 * "Starts at Sat 17 Oct, 10:00" in [zone]: the year only when it is not
 * the current one, "today" / "tomorrow" in place of the date when it is.
 */
fun formatStartsAt(
    startMs: Long,
    zone: ZoneId = ZoneId.systemDefault(),
    locale: Locale = Locale.getDefault(),
    nowMs: Long = System.currentTimeMillis(),
): String {
    val start = Instant.ofEpochMilli(startMs).atZone(zone)
    val now = Instant.ofEpochMilli(nowMs).atZone(zone)
    val time = DateTimeFormatter.ofPattern("HH:mm", locale).format(start)
    val day = when (start.toLocalDate()) {
        now.toLocalDate() -> "today"
        now.toLocalDate().plusDays(1) -> "tomorrow"
        else -> DateTimeFormatter.ofPattern(if (start.year == now.year) "EEE d MMM" else "EEE d MMM yyyy", locale).format(start)
    }
    return if (day == "today" || day == "tomorrow") "Starts $day at $time" else "Starts at $day, $time"
}
