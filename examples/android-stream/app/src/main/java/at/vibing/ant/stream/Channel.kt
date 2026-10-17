package at.vibing.ant.stream

import android.content.Context
import java.io.File
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * The account's permanent channel: the account's address (the feed
 * owner) and the topic text [ChannelAddress.TOPIC], `antstream`, the same
 * for every broadcast. One channel per account; the owner is what tells
 * channels apart.
 *
 * Broadcasts go out in the publisher's `"solar-punk"` format, so a
 * channel is shown and shared as owner + topic (no channel reference to
 * create first), and every broadcast continues the feed the last one left:
 * the publisher reads the feed head itself (issue #173), nothing here
 * tracks an index.
 *
 * The [name] is only a label: it goes in the broadcast's notes and the
 * UI, never in the topic, so renaming the channel never forks it.
 */
class Channel(context: Context, val owner: String) {
    private val prefs = context.getSharedPreferences(prefsName(owner), Context.MODE_PRIVATE)

    /** Where viewers find this channel. */
    val address: ChannelAddress = ChannelAddress.ofText(owner, ChannelAddress.TOPIC)

    var name: String
        get() = prefs.getString(KEY_NAME, null) ?: "My channel"
        set(value) = prefs.edit().putString(KEY_NAME, value).apply()

    private val _info = MutableStateFlow(SavedStreamInfo.fromPrefsJson(prefs.getString(KEY_INFO, null)))

    /**
     * The stream info the next broadcast goes out with (and the running
     * one shows), remembered for this account (issue #220).
     */
    val info: StateFlow<SavedStreamInfo> = _info.asStateFlow()

    @Synchronized
    fun saveInfo(value: SavedStreamInfo) {
        _info.value = value
        prefs.edit().putString(KEY_INFO, value.toPrefsJson()).apply()
    }

    /** Change the remembered info in place: [saveInfo] of `change(current)`. */
    @Synchronized
    fun updateInfo(change: (SavedStreamInfo) -> SavedStreamInfo) = saveInfo(change(_info.value))

    /**
     * The thumbnail image on the phone (the photo picked or the frame
     * captured last, a downscaled JPEG): the editor's preview, and what
     * goes up at Go live while [SavedStreamInfo.thumbnailPending].
     */
    val thumbnailFile: File = File(context.filesDir, "antstream-info/${ownerKey(owner)}/thumbnail.jpg")

    companion object {
        private const val KEY_NAME = "name"
        private const val KEY_INFO = "stream_info"

        /**
         * The preferences file holding this account's channel state (its
         * name and stream info): one per account, keyed by the address,
         * whatever its case.
         */
        fun prefsName(owner: String): String = "antstream.channel.${owner.lowercase()}"

        /** The account address as a path segment: 40 lowercase hex, no `0x`. */
        fun ownerKey(owner: String): String = owner.trim().removePrefix("0x").removePrefix("0X").lowercase()
    }
}

/**
 * A solar-punk channel: the feed owner (40 lowercase hex, no `0x`) and its
 * topic, as text ([topic], hashed to [topicHex] the way bee-js
 * `Topic.fromString` does) or as 32 raw bytes ([rawTopic]).
 */
data class ChannelAddress(val owner: String, val topic: String, val topicHex: String, val rawTopic: Boolean) {
    /**
     * `0x<owner>/<topic>`, the form the web player's link carries after
     * its `#`: the topic URL-encoded, or `0x<64 hex>` for a raw one.
     */
    val key: String get() = "0x$owner/" + if (rawTopic) "0x$topicHex" else encodeComponent(topic)

    /** What the Watch field shows for it. */
    val display: String get() = "0x$owner/" + if (rawTopic) "0x$topicHex" else topic

    /**
     * Solar Punk's `swarm://<owner>/<topic>` URI, the form its players and
     * the publisher's master playlist name a channel by. Only for a text
     * topic: Solar Punk's readers hash the text in the URI.
     */
    val swarmUri: String? get() = if (rawTopic) null else "swarm://$owner/$topic"

    /** The web player on this channel, in Freedom (`bzz://`). */
    val playerLink: String get() = "bzz://$PLAYER_REF/#$key"

    /** The web player on this channel, through a public gateway, for any browser. */
    val gatewayPlayerLink: String get() = "$PUBLIC_GATEWAY/bzz/$PLAYER_REF/#$key"

    /** The ladder group topic its time markers are written for. */
    val groupHex: String get() = Keccak.ladderGroup(topicHex)

    /** What "Share" sends: the web player links, then the Solar Punk URI. */
    fun watchLinks(): String = buildString {
        append("Watch my live stream on Swarm:\n")
        append("Freedom browser: $playerLink\n")
        append("Any browser: $gatewayPlayerLink\n")
        swarmUri?.let { append("Solar Punk players: $it\n") }
        append("Channel: $display")
    }

    companion object {
        /** The topic text every AntStream app broadcast goes out on. */
        const val TOPIC = "antstream"

        /**
         * The AntStream web player's feed manifest (examples/antstream-player):
         * always serves its newest build.
         */
        const val PLAYER_REF = "b268072928bd99d637992eb54c2a826b83affa36f61cf495297541ac42857c35"
        const val PUBLIC_GATEWAY = "https://api.gateway.ethswarm.org"

        private val OWNER = Regex("^(?:0[xX])?([0-9a-fA-F]{40})$")
        private val RAW_TOPIC = Regex("^(?:0[xX])?([0-9a-fA-F]{64})$")
        // What Swift's `CharacterSet.newlines` holds: U+000A-U+000D, NEL,
        // LS and PS.
        private const val LINE_BREAKS = "\n\u000B\u000C\r\u0085\u2028\u2029"

        /**
         * Swift's `CharacterSet.whitespacesAndNewlines`: tab, the space
         * separators (Zs) and [LINE_BREAKS]. Kotlin's own `trim()` differs
         * both ways (it keeps NEL, it drops U+001C-U+001F), so `parse` trims
         * with this to read the same input as iOS.
         */
        private fun isSwiftWhitespace(c: Char): Boolean =
            c == '\t' || c in LINE_BREAKS || Character.getType(c) == Character.SPACE_SEPARATOR.toInt()
        private val SWARM_URI = Regex("^swarm://([^/?#]+)/([^/?#]+)", RegexOption.IGNORE_CASE)

        /** The channel `owner` (with or without `0x`) broadcasts on topic text `text`. */
        fun ofText(owner: String, text: String): ChannelAddress {
            val o = owner.removePrefix("0x").removePrefix("0X").lowercase()
            return ChannelAddress(o, text, Keccak.topicFromString(text), rawTopic = false)
        }

        /**
         * A channel from what a viewer typed or pasted, read the way the web
         * player's `parseChannel` reads one link or URI (input with no
         * whitespace): `<owner>/<topic>` (the owner `0x`
         * + 40 hex, the topic text, URL-encoded or not), Solar Punk's
         * `swarm://<owner>/<topic>`, or a player link carrying either after
         * its `#`. A topic of 64 hex digits (`0x` optional) is the raw topic.
         * An owner alone is that account's AntStream channel (topic
         * [TOPIC]). `null` for anything else. Unlike the web player, this
         * does not search free text (the Share text, a message it was pasted
         * into) for a channel: the whole input is the one link, so input
         * over more than one line (pasted Share text) is `null` here. A
         * topic never has a line break in it. That only holds while the
         * line breaks reach this: a field that turns them into spaces hands
         * over one line, read as one link whose topic is the rest of the
         * text.
         */
        fun parse(input: String): ChannelAddress? {
            var s = input.trim(::isSwiftWhitespace)
            if (s.isEmpty() || s.any { it in LINE_BREAKS }) return null
            val hash = s.indexOf('#')
            if (hash >= 0) s = s.substring(hash + 1)
            val owner: String
            val rest: String
            val uri = SWARM_URI.find(s)
            if (uri != null) {
                owner = uri.groupValues[1]
                rest = uri.groupValues[2]
            } else {
                val slash = s.indexOf('/')
                owner = if (slash < 0) s else s.substring(0, slash)
                rest = if (slash < 0) "" else s.substring(slash + 1)
            }
            val o = OWNER.matchEntire(owner)?.groupValues?.get(1)?.lowercase() ?: return null
            val topic = decodeComponent(rest.trimEnd('/')).ifEmpty { TOPIC }
            val raw = RAW_TOPIC.matchEntire(topic)
            return if (raw != null) {
                val hex = raw.groupValues[1].lowercase()
                ChannelAddress(o, "0x$hex", hex, rawTopic = true)
            } else {
                ofText(o, topic)
            }
        }

        /** `encodeURIComponent`: what the web player decodes. */
        fun encodeComponent(text: String): String = buildString {
            for (b in text.toByteArray(Charsets.UTF_8)) {
                val c = (b.toInt() and 0xff).toChar()
                if (c.isLetterOrDigit() && c.code < 0x80 || c in "-_.!~*'()") append(c)
                else append("%%%02X".format(b.toInt() and 0xff))
            }
        }

        /** `decodeURIComponent`, or the text as it is when it isn't valid percent-encoding. */
        fun decodeComponent(text: String): String {
            if ('%' !in text) return text
            val out = java.io.ByteArrayOutputStream()
            var i = 0
            while (i < text.length) {
                val c = text[i]
                if (c == '%') {
                    val byte = text.substring(i + 1, minOf(i + 3, text.length)).takeIf { it.length == 2 }
                        ?.toIntOrNull(16) ?: return text
                    out.write(byte)
                    i += 3
                } else {
                    out.write(c.toString().toByteArray(Charsets.UTF_8))
                    i++
                }
            }
            return out.toString(Charsets.UTF_8.name())
        }
    }
}
