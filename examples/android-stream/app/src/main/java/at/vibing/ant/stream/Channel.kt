package at.vibing.ant.stream

import android.content.Context
import java.net.HttpURLConnection
import java.net.URL
import java.security.SecureRandom
import org.json.JSONObject

/**
 * The account's permanent channel: one feed topic kept across broadcasts,
 * so the channel reference (the feed manifest of owner + topic) is one
 * link a viewer can bookmark, shared before going live.
 *
 * Every broadcast continues the feed where the last one stopped
 * ([nextIndex] becomes the publisher's `start_index`): rewriting indices
 * that already hold an old broadcast's playlists would collide with them.
 * The index is saved as the broadcast runs, so even an app killed
 * mid-stream resumes at most one update behind.
 *
 * Scoped to the account address: a restored account gets its own channel.
 */
class Channel(context: Context, val owner: String) {
    private val prefs = context.getSharedPreferences("antstream.channel.${owner.lowercase()}", Context.MODE_PRIVATE)

    /** 32-byte feed topic, hex, created once. */
    val topic: String = prefs.getString(KEY_TOPIC, null) ?: run {
        val bytes = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val hex = bytes.joinToString("") { "%02x".format(it) }
        prefs.edit().putString(KEY_TOPIC, hex).apply()
        hex
    }

    var name: String
        get() = prefs.getString(KEY_NAME, null) ?: "My channel"
        set(value) = prefs.edit().putString(KEY_NAME, value).apply()

    var nextIndex: Long
        get() = prefs.getLong(KEY_INDEX, 0)
        set(value) {
            // Only ever forward: a stale report must not rewind the feed.
            if (value > nextIndex) prefs.edit().putLong(KEY_INDEX, value).apply()
        }

    /** The shareable channel reference, once its feed manifest exists. */
    var reference: String?
        get() = prefs.getString(KEY_REF, null)
        private set(value) = prefs.edit().putString(KEY_REF, value).apply()

    /**
     * Create the channel's feed manifest (`POST /feeds/{owner}/{topic}`,
     * bee-js `createFeedManifest`) so the link exists before the first
     * broadcast. Content-addressed, so re-creating it yields the same
     * reference. Blocking: call off the main thread.
     */
    fun ensureReference(gatewayUrl: String, batchId: String): String {
        reference?.let { return it }
        val ownerHex = owner.removePrefix("0x").lowercase()
        val conn = URL("$gatewayUrl/feeds/$ownerHex/$topic").openConnection() as HttpURLConnection
        try {
            conn.requestMethod = "POST"
            conn.connectTimeout = 10_000
            conn.readTimeout = 120_000
            conn.doOutput = true
            conn.setRequestProperty("swarm-postage-batch-id", batchId.removePrefix("0x"))
            conn.outputStream.use { }
            val code = conn.responseCode
            val body = (if (code in 200..299) conn.inputStream else conn.errorStream)
                ?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) throw AntException("could not create the channel ($code): ${body.take(200)}")
            val ref = JSONObject(body).getString("reference")
            reference = ref
            return ref
        } finally {
            conn.disconnect()
        }
    }

    companion object {
        private const val KEY_TOPIC = "topic"
        private const val KEY_NAME = "name"
        private const val KEY_INDEX = "next_index"
        private const val KEY_REF = "reference"

        /** The AntStream web player's Swarm reference (examples/antstream-player). */
        const val PLAYER_REF = "1d7ee3e99ad90cff1275dd3954bb5ef54fbe638c35d6d6ef7c64dfa07f259e4f"

        /** What "Share" sends: the channel in the web player, for Freedom and any browser. */
        fun watchLinks(channel: String): String =
            "Watch my live stream on Swarm:\n" +
                "Freedom browser: bzz://$PLAYER_REF/#$channel\n" +
                "Any browser: https://api.gateway.ethswarm.org/bzz/$PLAYER_REF/#$channel\n" +
                "Channel: $channel"
    }
}
