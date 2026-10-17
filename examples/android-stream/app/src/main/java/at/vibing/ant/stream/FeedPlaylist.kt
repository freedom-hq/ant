package at.vibing.ant.stream

import android.net.Uri
import androidx.annotation.OptIn
import androidx.media3.common.C
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.ByteArrayDataSource
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.DefaultHttpDataSource
import androidx.media3.datasource.TransferListener
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import org.json.JSONObject

/**
 * Follows a solar-punk channel's feed through the in-process gateway and
 * hands ExoPlayer the newest playlist. Every feed update **is** the
 * playlist (a v2 update wrapping the playlist's root chunk), listing its
 * segments and `#EXT-X-MAP` as bare references.
 *
 * - **Joining** reads a ladder time marker first (the period before this
 *   one, then the one before that, as Solar Punk's player does): a
 *   channel published with `"ladder": true` names its newest index there,
 *   and that slot is one chunk away. Without one, the head lookup
 *   `GET /feeds/{owner}/{topic}` finds the newest update (its
 *   `swarm-feed-index` header names the index).
 * - **Following**, each playlist reload reads the slot after the newest
 *   one read, `GET /soc/{owner}/{keccak256(topic ‖ index_be8)}`, and on
 *   on while slots answer: the update, or a `404` "not written yet", in
 *   which case the playlist read last is served again.
 * - Bare references become `<gateway>/bytes/<ref>` ([rewrite]), the
 *   rewrite Solar Punk's viewers do.
 *
 * Thread-safe: ExoPlayer reloads on its loader thread.
 */
class FeedFollower(val gateway: String, val channel: ChannelAddress) {
    private var index: Long? = null
    private var last: ByteArray? = null

    /** The URI ExoPlayer is given for the playlist; [FeedDataSource] answers it. */
    val playlistUri: Uri = Uri.parse("$SCHEME://feed/${channel.owner}/${channel.topicHex}.m3u8")

    /** Where the head is read, shown under the player. */
    val feedUrl: String get() = "$gateway/feeds/${channel.owner}/${channel.topicHex}"

    /** The newest playlist, rewritten for ExoPlayer. Blocking. */
    @Synchronized
    fun playlist(): ByteArray {
        val at = index
        if (at == null) join() else follow(at)
        val text = String(last ?: throw IOException("no playlist"), Charsets.UTF_8)
        return rewrite(text, gateway).toByteArray(Charsets.UTF_8)
    }

    private fun join() {
        val marker = markerIndex()
        if (marker != null) {
            val slot = get("soc/${channel.owner}/${Keccak.feedIdentifier(channel.topicHex, marker)}")
            if (slot.code == 200 && isPlaylist(slot.body)) {
                accept(marker, slot.body)
                follow(marker)
                return
            }
        }
        val head = get("feeds/${channel.owner}/${channel.topicHex}")
        if (head.code == 404) throw IOException("The channel has no broadcast yet (404)")
        if (head.code != 200) throw IOException("Can't read the channel (${head.code})")
        if (!isPlaylist(head.body)) throw IOException("This feed is not a solar-punk channel")
        val at = head.index ?: throw IOException("The head read named no feed index")
        accept(at, head.body)
    }

    /** Reads the slots after `from` until one is not written yet. */
    private fun follow(from: Long) {
        var at = from
        repeat(MAX_SLOTS_PER_RELOAD) {
            val next = at + 1
            val slot = get("soc/${channel.owner}/${Keccak.feedIdentifier(channel.topicHex, next)}")
            if (slot.code != 200 || !isPlaylist(slot.body)) return
            accept(next, slot.body)
            at = next
        }
    }

    private fun accept(at: Long, body: ByteArray) {
        index = at
        last = body
    }

    private fun markerIndex(): Long? {
        val now = System.currentTimeMillis() / MARKER_PERIOD_MS
        val group = channel.groupHex
        for (period in listOf(now - 1, now - 2)) {
            val r = get("soc/${channel.owner}/${Keccak.ladderMarkerIdentifier(group, period)}", timeoutMs = 10_000)
            if (r.code == 200) return parseMarker(String(r.body, Charsets.UTF_8), period, channel.topicHex)
            if (r.code != 404) return null
        }
        return null
    }

    private class Response(val code: Int, val body: ByteArray, val index: Long?)

    private fun get(path: String, timeoutMs: Int = 60_000): Response {
        val conn = URL("$gateway/$path").openConnection() as HttpURLConnection
        return try {
            conn.connectTimeout = 10_000
            conn.readTimeout = timeoutMs
            conn.useCaches = false
            val code = conn.responseCode
            if (code != 200) return Response(code, ByteArray(0), null)
            val body = conn.inputStream.use { it.readBytes() }
            val declared = conn.getHeaderField("Content-Length")?.toLongOrNull()
            // A join cut short after the header went out reads as a
            // shorter body: never take it for the playlist.
            if (declared != null && declared != body.size.toLong()) throw IOException("short read: ${body.size} of $declared bytes")
            Response(code, body, conn.getHeaderField("swarm-feed-index")?.trim()?.toLongOrNull(16))
        } catch (e: IOException) {
            if (path.startsWith("soc/")) Response(0, ByteArray(0), null) else throw e
        } finally {
            conn.disconnect()
        }
    }

    companion object {
        const val SCHEME = "antstream-feed"
        private const val MARKER_PERIOD_MS = 10_000L
        /** Slots read ahead on one reload before the newest is served. */
        private const val MAX_SLOTS_PER_RELOAD = 8

        fun isPlaylist(body: ByteArray): Boolean =
            body.size >= 7 && String(body, 0, 7, Charsets.US_ASCII) == "#EXTM3U"

        /**
         * The feed index a ladder marker read for `period` names for the
         * channel with topic `topicHex`, or `null` for anything that is not
         * exactly a version 2 marker of that period (Solar Punk's
         * `parseLadderMarker`): the index is jumped to without a second look.
         */
        fun parseMarker(text: String, period: Long, topicHex: String): Long? = runCatching {
            val o = JSONObject(text)
            if (o.length() != 5 || o.getInt("v") != 2 || o.getLong("period") != period) return null
            if (o.getLong("writtenAt") / MARKER_PERIOD_MS != period || o.getLong("segmentMs") <= 0) return null
            val rungs = o.getJSONObject("rungs")
            val v = rungs.opt(topicHex.lowercase()) as? Number ?: return null
            v.toLong().takeIf { it >= 0 && v.toDouble() == it.toDouble() }
        }.getOrNull()

        /**
         * `playlist` with every bare reference (a segment line or the
         * `#EXT-X-MAP` URI, 64 or 128 hex) made `<gateway>/bytes/<ref>`.
         * Everything else, `#EXT-X-GAP` placeholders (`gap-<n>`) included,
         * passes through.
         */
        fun rewrite(playlist: String, gateway: String): String {
            val base = gateway.trimEnd('/')
            fun absolute(value: String): String? {
                val v = value.trim()
                return if ((v.length == 64 || v.length == 128) && v.all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }) {
                    "$base/bytes/${v.lowercase()}"
                } else {
                    null
                }
            }
            val map = "#EXT-X-MAP:URI=\""
            return playlist.split("\n").joinToString("\n") { line ->
                when {
                    !line.startsWith("#") -> absolute(line) ?: line
                    line.startsWith(map) && line.endsWith("\"") ->
                        absolute(line.substring(map.length, line.length - 1))?.let { "$map$it\"" } ?: line
                    else -> line
                }
            }
        }
    }
}

/**
 * ExoPlayer's data source for a channel: the playlist URI comes from the
 * [FeedFollower], everything else (segments, the init segment, all plain
 * `http://` on the gateway) goes over HTTP.
 */
@OptIn(UnstableApi::class)
class FeedDataSource(private val follower: FeedFollower) : DataSource {
    private val http = DefaultHttpDataSource.Factory().setAllowCrossProtocolRedirects(false).createDataSource()
    private var current: DataSource? = null

    override fun addTransferListener(transferListener: TransferListener) = http.addTransferListener(transferListener)

    override fun open(dataSpec: DataSpec): Long {
        val source = if (dataSpec.uri.scheme == FeedFollower.SCHEME) {
            if (dataSpec.uri.lastPathSegment?.endsWith(".m3u8") != true) {
                // An `#EXT-X-GAP` entry's placeholder: names no media.
                throw IOException("no such segment: ${dataSpec.uri}")
            }
            ByteArrayDataSource(follower.playlist())
        } else {
            http
        }
        current = source
        return source.open(dataSpec)
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int =
        current?.read(buffer, offset, length) ?: C.RESULT_END_OF_INPUT

    override fun getUri(): Uri? = current?.uri

    override fun getResponseHeaders(): Map<String, List<String>> = current?.responseHeaders ?: emptyMap()

    override fun close() {
        current?.close()
        current = null
    }

    class Factory(private val follower: FeedFollower) : DataSource.Factory {
        override fun createDataSource(): DataSource = FeedDataSource(follower)
    }
}
