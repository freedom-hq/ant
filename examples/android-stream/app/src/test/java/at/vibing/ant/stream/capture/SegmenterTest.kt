package at.vibing.ant.stream.capture

import at.vibing.ant.stream.Segment
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class SegmenterTest {
    private val video = Fmp4Muxer.VideoConfig(640, 360, byteArrayOf(0x67, 0x42, 0x00, 0x1e), byteArrayOf(0x68, 0x01), 0)
    private val audio = Fmp4Muxer.AudioConfig(48_000, 1, byteArrayOf(0x11, 0x88.toByte()))

    /** Count `traf` boxes (one per track carrying samples) in a media segment. */
    private fun tracks(segment: Segment): Int =
        String(segment.data, Charsets.ISO_8859_1).windowed(4).count { it == "traf" }

    private fun run(audioClockOffsetUs: Long): List<Segment> {
        val out = mutableListOf<Segment>()
        val seg = Segmenter(2_000_000, expectAudio = true) { out += it }
        seg.setVideoConfig(video)
        seg.setAudioConfig(audio)
        val start = 5_000_000_000L // camera clock, µs
        var audioPts = start + audioClockOffsetUs
        var t = 0L
        var frame = 0
        while (t < 7_000_000) {
            // 30 fps video, keyframe every second.
            seg.onVideo(byteArrayOf(0, 0, 0, 1, 0x65), start + t, frame++ % 30 == 0)
            // Audio frames (1024 samples at 48 kHz = 21.3 ms) up to "now".
            while (audioPts - audioClockOffsetUs <= start + t) {
                seg.onAudio(byteArrayOf(0x21, 0x00), audioPts)
                audioPts += 21_333
            }
            t += 33_333
        }
        seg.finish()
        return out
    }

    @Test
    fun audioOnTheSameClockLandsInEverySegment() {
        val media = run(audioClockOffsetUs = 0).filter { !it.isInit }
        assertTrue(media.size >= 3)
        media.forEach { assertEquals("video + audio", 2, tracks(it)) }
    }

    @Test
    fun audioOnAnotherClockIsAlignedNotDropped() {
        // An hour apart, as boot time vs monotonic after a day of sleep.
        for (offset in listOf(-3_600_000_000L, 3_600_000_000L)) {
            val media = run(audioClockOffsetUs = offset).filter { !it.isInit }
            assertTrue(media.size >= 3)
            media.forEach { assertEquals("video + audio at offset $offset", 2, tracks(it)) }
        }
    }
}
