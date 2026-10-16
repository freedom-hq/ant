package at.vibing.ant.stream.capture

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

/** Portrait broadcasts are encoded as real portrait frames (#194). */
class PortraitTest {
    private val sps = byteArrayOf(0x67, 0x42, 0x00, 0x1e)
    private val pps = byteArrayOf(0x68, 0x01)

    /** Offset of the first box of [type] at or after [from] (type field − 4). */
    private fun find(data: ByteArray, type: String, from: Int = 0): Int {
        val t = type.toByteArray(Charsets.US_ASCII)
        for (i in maxOf(from, 4)..data.size - 4) {
            if ((0 until 4).all { data[i + it] == t[it] }) return i - 4
        }
        error("no $type box")
    }

    private fun int(data: ByteArray, at: Int): Int =
        ((data[at].toInt() and 0xFF) shl 24) or ((data[at + 1].toInt() and 0xFF) shl 16) or
            ((data[at + 2].toInt() and 0xFF) shl 8) or (data[at + 3].toInt() and 0xFF)

    private fun short(data: ByteArray, at: Int): Int =
        ((data[at].toInt() and 0xFF) shl 8) or (data[at + 1].toInt() and 0xFF)

    @Test
    fun portraitInitSegmentIsCodedPortraitWithIdentityMatrix() {
        val init = Fmp4Muxer(Fmp4Muxer.VideoConfig(480, 640, sps, pps), Fmp4Muxer.AudioConfig(48_000, 1, byteArrayOf(0x11, 0x88.toByte())))
            .initSegment()

        // Video tkhd (version 0): matrix after 8 header + 4 version/flags
        // + 20 times/ids + 8 reserved + 8 layer/group/volume/reserved.
        val tkhd = find(init, "tkhd")
        assertEquals("video track first", 1, int(init, tkhd + 8 + 4 + 8))
        val matrix = IntArray(9) { int(init, tkhd + 48 + it * 4) }
        assertArrayEquals(
            "identity matrix",
            intArrayOf(0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000),
            matrix,
        )
        assertEquals("tkhd width", 480 shl 16, int(init, tkhd + 84))
        assertEquals("tkhd height", 640 shl 16, int(init, tkhd + 88))

        // avc1 sample entry: coded size after 8 header + 6 reserved + 2
        // data_reference_index + 16 pre_defined/reserved.
        val avc1 = find(init, "avc1")
        assertEquals("coded width", 480, short(init, avc1 + 32))
        assertEquals("coded height", 640, short(init, avc1 + 34))

        // mvhd and the audio tkhd are identity too: no rotation anywhere.
        val mvhd = find(init, "mvhd")
        assertEquals(0x00010000, int(init, mvhd + 8 + 4 + 16 + 4 + 2 + 10))
        val audioTkhd = find(init, "tkhd", tkhd + 8)
        assertEquals("audio track second", 2, int(init, audioTkhd + 8 + 4 + 8))
        assertArrayEquals(
            intArrayOf(0x00010000, 0, 0, 0, 0x00010000, 0, 0, 0, 0x40000000),
            IntArray(9) { int(init, audioTkhd + 48 + it * 4) },
        )
    }

    /**
     * Render a W×H "camera frame" through [UprightRenderer.texCoords] the
     * way GL does (texture coordinates interpolated across the quad,
     * sampled at each output pixel's centre) and return the output rows.
     */
    private fun render(frame: List<String>, rotation: Int): List<String> {
        val w = frame[0].length
        val h = frame.size
        val quarter = rotation % 180 != 0
        val outW = if (quarter) h else w
        val outH = if (quarter) w else h
        val c = UprightRenderer.texCoords(rotation)
        // Corners: 0 bottom-left, 1 bottom-right, 2 top-left, 3 top-right.
        val tl = c[4] to c[5]
        val tr = c[6] to c[7]
        val bl = c[0] to c[1]
        return (0 until outH).map { y ->
            val fy = (y + 0.5f) / outH
            (0 until outW).map { x ->
                val fx = (x + 0.5f) / outW
                val s = tl.first + fx * (tr.first - tl.first) + fy * (bl.first - tl.first)
                val t = tl.second + fx * (tr.second - tl.second) + fy * (bl.second - tl.second)
                frame[(t * h).toInt().coerceIn(0, h - 1)][(s * w).toInt().coerceIn(0, w - 1)]
            }.joinToString("")
        }
    }

    // A landscape camera frame as the sensor delivers it: 4 wide, 2 tall,
    // "a" at its top-left.
    private val frame = listOf("abcd", "efgh")

    @Test
    fun noRotationKeepsTheFrame() {
        assertEquals(frame, render(frame, 0))
    }

    @Test
    fun quarterTurnClockwiseMakesAPortraitFrame() {
        // Turned 90° clockwise, the left column becomes the top row (read
        // bottom to top) and the frame is 2 wide, 4 tall.
        assertEquals(listOf("ea", "fb", "gc", "hd"), render(frame, 90))
    }

    @Test
    fun threeQuarterTurnIsTheOtherWay() {
        assertEquals(listOf("dh", "cg", "bf", "ae"), render(frame, 270))
    }

    @Test
    fun halfTurnIsUpsideDown() {
        assertEquals(listOf("hgfe", "dcba"), render(frame, 180))
        assertEquals(render(frame, 90), render(frame, -270))
    }
}
