package at.vibing.ant.stream

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class FeedPlaylistTest {
    private val ref = "ab".repeat(32)
    private val map = "cd".repeat(32)
    private val gw = "http://127.0.0.1:1633"

    @Test
    fun rewritesBareReferencesOntoTheGateway() {
        val playlist = listOf(
            "#EXTM3U",
            "#EXT-X-TARGETDURATION:2",
            "#EXT-X-MAP:URI=\"$map\"",
            "#EXT-X-PROGRAM-DATE-TIME:2026-10-17T10:00:00.000Z",
            "#EXTINF:2.000,",
            ref.uppercase(),
            "#EXT-X-GAP",
            "#EXTINF:2.000,",
            "gap-7",
            "#EXTINF:2.000,",
            "ef".repeat(64),
            "https://elsewhere.example/seg.m4s",
            "#EXT-X-ENDLIST",
            "",
        ).joinToString("\n")
        val out = FeedFollower.rewrite(playlist, "$gw/").split("\n")
        assertEquals("#EXT-X-MAP:URI=\"$gw/bytes/$map\"", out[2])
        assertEquals("$gw/bytes/$ref", out[5])
        assertEquals("gap-7", out[8])
        assertEquals("$gw/bytes/${"ef".repeat(64)}", out[10])
        assertEquals("https://elsewhere.example/seg.m4s", out[11])
        assertEquals("#EXT-X-ENDLIST", out[12])
        assertEquals("", out.last())
    }

    @Test
    fun leavesNonAsciiDigitsAlone() {
        // Fullwidth and Arabic-Indic digits pass Kotlin's Char.isDigit; a
        // gateway reads only ASCII hex, and iOS passes these through too.
        val fullwidth = "\uFF11".repeat(64)
        val arabic = "\u0661".repeat(64)
        val playlist = "#EXTM3U\n#EXT-X-MAP:URI=\"$arabic\"\n#EXTINF:2.000,\n$fullwidth\n"
        assertEquals(playlist, FeedFollower.rewrite(playlist, gw))
    }

    @Test
    fun readsOnlyVersion2MarkersOfThePeriod() {
        val topic = "f526f44fb746b8c958e2686aafec618f61351d834bd7d9c11b4a6726b6276646"
        val ok = """{"v":2,"period":100,"writtenAt":1004321,"rungs":{"$topic":42},"segmentMs":2000}"""
        assertEquals(42L, FeedFollower.parseMarker(ok, 100, topic))
        assertNull(FeedFollower.parseMarker(ok, 101, topic))
        assertNull(FeedFollower.parseMarker(ok.replace("\"v\":2", "\"v\":1"), 100, topic))
        assertNull(FeedFollower.parseMarker(ok.replace("1004321", "994321"), 100, topic))
        assertNull(FeedFollower.parseMarker(ok.replace(topic, "00".repeat(32)), 100, topic))
        assertNull(FeedFollower.parseMarker(ok.replace("42", "4.5"), 100, topic))
        assertNull(FeedFollower.parseMarker("not json", 100, topic))
    }

    @Test
    fun tellsPlaylistsApart() {
        assertTrue(FeedFollower.isPlaylist("#EXTM3U\n".toByteArray()))
        assertFalse(FeedFollower.isPlaylist(ByteArray(40)))
    }
}
