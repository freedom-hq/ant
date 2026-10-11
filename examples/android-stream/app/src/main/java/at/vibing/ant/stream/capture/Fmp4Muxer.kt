package at.vibing.ant.stream.capture

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

/**
 * Minimal fragmented-MP4 writer for HLS (RFC 8216 §3.3): one
 * initialization segment (`ftyp` + `moov`) and media segments of one
 * `moof` + `mdat` each, carrying H.264 video and optional AAC audio.
 *
 * Android has no muxer that emits standalone CMAF segments
 * (`MediaMuxer` writes whole files), so this is the counterpart of the
 * iOS app's `AVAssetWriter` in `.mpeg4AppleHLS` mode: just the boxes an
 * HLS player needs, nothing else.
 *
 * Video uses a 90 kHz timescale, audio its sample rate. Samples carry
 * their own durations; there are no B-frames (decode order equals
 * presentation order), so no composition offsets are written.
 */
class Fmp4Muxer(private val video: VideoConfig, private val audio: AudioConfig?) {

    /** H.264 parameters: SPS and PPS NAL units *without* start codes. */
    class VideoConfig(val width: Int, val height: Int, val sps: ByteArray, val pps: ByteArray, val rotation: Int)

    /** AAC parameters: the AudioSpecificConfig from the encoder's csd-0. */
    class AudioConfig(val sampleRate: Int, val channels: Int, val asc: ByteArray)

    class Sample(val data: ByteArray, val duration: Int, val isSync: Boolean)

    fun initSegment(): ByteArray = build {
        box("ftyp") {
            fourcc("iso6")
            int(0)
            fourcc("iso6"); fourcc("cmfc"); fourcc("mp41"); fourcc("dash")
        }
        box("moov") {
            fullBox("mvhd", 0, 0) {
                int(0); int(0) // creation, modification
                int(1000) // timescale
                int(0) // duration: unknown for a live stream
                int(0x00010000) // rate 1.0
                short(0x0100) // volume 1.0
                zeros(10)
                matrix(0)
                zeros(24) // pre_defined
                int(if (audio != null) 3 else 2) // next_track_ID
            }
            videoTrak()
            audio?.let { audioTrak(it) }
            box("mvex") {
                trex(VIDEO_TRACK)
                if (audio != null) trex(AUDIO_TRACK)
            }
        }
    }

    /**
     * One media segment. [videoBase] / [audioBase] are each track's
     * decode time of its first sample, in the track's timescale.
     */
    fun mediaSegment(
        sequence: Int,
        videoSamples: List<Sample>,
        videoBase: Long,
        audioSamples: List<Sample>,
        audioBase: Long,
    ): ByteArray {
        val tracks = buildList {
            add(Track(VIDEO_TRACK, videoSamples, videoBase))
            if (audio != null && audioSamples.isNotEmpty()) add(Track(AUDIO_TRACK, audioSamples, audioBase))
        }
        // The trun data offsets point into the mdat that follows the
        // moof, so the moof is built once to learn its size (it does not
        // depend on the offsets' values) and then again with them.
        val probe = moof(sequence, tracks, IntArray(tracks.size))
        val offsets = IntArray(tracks.size)
        var cursor = probe.size + 8 // + mdat header
        tracks.forEachIndexed { i, t ->
            offsets[i] = cursor
            cursor += t.samples.sumOf { it.data.size }
        }
        val moof = moof(sequence, tracks, offsets)
        val mdatSize = 8 + tracks.sumOf { t -> t.samples.sumOf { it.data.size } }
        val out = ByteArrayOutputStream(moof.size + mdatSize)
        out.write(moof)
        DataOutputStream(out).apply {
            writeInt(mdatSize)
            writeBytes("mdat")
        }
        tracks.forEach { t -> t.samples.forEach { out.write(it.data) } }
        return out.toByteArray()
    }

    private class Track(val id: Int, val samples: List<Sample>, val base: Long)

    private fun moof(sequence: Int, tracks: List<Track>, offsets: IntArray): ByteArray = build {
        box("moof") {
            fullBox("mfhd", 0, 0) { int(sequence) }
            tracks.forEachIndexed { i, t ->
                box("traf") {
                    // default-base-is-moof: data offsets count from the moof.
                    fullBox("tfhd", 0, 0x020000) { int(t.id) }
                    fullBox("tfdt", 1, 0) { long(t.base) }
                    // data-offset, sample-duration, sample-size, sample-flags
                    fullBox("trun", 0, 0x000701) {
                        int(t.samples.size)
                        int(offsets[i])
                        t.samples.forEach { s ->
                            int(s.duration)
                            int(s.data.size)
                            int(if (s.isSync) SYNC_FLAGS else NON_SYNC_FLAGS)
                        }
                    }
                }
            }
        }
    }

    private fun Writer.videoTrak() = box("trak") {
        fullBox("tkhd", 0, 7) {
            int(0); int(0)
            int(VIDEO_TRACK)
            int(0)
            int(0) // duration
            zeros(8)
            short(0); short(0) // layer, alternate_group
            short(0) // volume
            short(0)
            matrix(video.rotation)
            int(video.width shl 16)
            int(video.height shl 16)
        }
        box("mdia") {
            mdhd(VIDEO_TIMESCALE)
            hdlr("vide", "VideoHandler")
            box("minf") {
                fullBox("vmhd", 0, 1) { short(0); short(0); short(0); short(0) }
                dinf()
                box("stbl") {
                    fullBox("stsd", 0, 0) {
                        int(1)
                        box("avc1") {
                            zeros(6); short(1) // reserved, data_reference_index
                            zeros(16) // pre_defined + reserved
                            short(video.width); short(video.height)
                            int(0x00480000); int(0x00480000) // 72 dpi
                            int(0)
                            short(1) // frame_count
                            zeros(32) // compressorname
                            short(0x0018) // depth
                            short(-1) // pre_defined
                            box("avcC") {
                                byte(1)
                                byte(video.sps[1].toInt())
                                byte(video.sps[2].toInt())
                                byte(video.sps[3].toInt())
                                byte(0xFF) // 4-byte NAL lengths
                                byte(0xE1) // one SPS
                                short(video.sps.size); bytes(video.sps)
                                byte(1) // one PPS
                                short(video.pps.size); bytes(video.pps)
                            }
                        }
                    }
                    emptySampleTables()
                }
            }
        }
    }

    private fun Writer.audioTrak(a: AudioConfig) = box("trak") {
        fullBox("tkhd", 0, 7) {
            int(0); int(0)
            int(AUDIO_TRACK)
            int(0)
            int(0)
            zeros(8)
            short(0); short(1) // layer, alternate_group
            short(0x0100) // volume
            short(0)
            matrix(0)
            int(0); int(0)
        }
        box("mdia") {
            mdhd(a.sampleRate)
            hdlr("soun", "SoundHandler")
            box("minf") {
                fullBox("smhd", 0, 0) { short(0); short(0) }
                dinf()
                box("stbl") {
                    fullBox("stsd", 0, 0) {
                        int(1)
                        box("mp4a") {
                            zeros(6); short(1)
                            zeros(8)
                            short(a.channels)
                            short(16) // samplesize
                            short(0); short(0)
                            int(a.sampleRate shl 16)
                            fullBox("esds", 0, 0) { esDescriptor(a) }
                        }
                    }
                    emptySampleTables()
                }
            }
        }
    }

    private fun Writer.esDescriptor(a: AudioConfig) {
        val decoderSpecific = descriptor(0x05, a.asc)
        val decoderConfig = descriptor(
            0x04,
            build {
                byte(0x40) // objectTypeIndication: MPEG-4 audio
                byte(0x15) // streamType audio (5) << 2 | 1
                byte(0); short(0) // bufferSizeDB
                int(AUDIO_BITRATE); int(AUDIO_BITRATE)
                bytes(decoderSpecific)
            },
        )
        val sl = descriptor(0x06, byteArrayOf(0x02))
        bytes(
            descriptor(
                0x03,
                build {
                    short(AUDIO_TRACK) // ES_ID
                    byte(0) // flags
                    bytes(decoderConfig)
                    bytes(sl)
                },
            ),
        )
    }

    private fun descriptor(tag: Int, payload: ByteArray): ByteArray {
        require(payload.size < 128) { "descriptor too large" }
        return byteArrayOf(tag.toByte(), payload.size.toByte()) + payload
    }

    private fun Writer.mdhd(timescale: Int) = fullBox("mdhd", 0, 0) {
        int(0); int(0)
        int(timescale)
        int(0)
        short(0x55C4) // language "und"
        short(0)
    }

    private fun Writer.hdlr(type: String, name: String) = fullBox("hdlr", 0, 0) {
        int(0)
        fourcc(type)
        zeros(12)
        bytes(name.toByteArray(Charsets.US_ASCII))
        byte(0)
    }

    private fun Writer.dinf() = box("dinf") {
        fullBox("dref", 0, 0) {
            int(1)
            fullBox("url ", 0, 1) {}
        }
    }

    private fun Writer.emptySampleTables() {
        fullBox("stts", 0, 0) { int(0) }
        fullBox("stsc", 0, 0) { int(0) }
        fullBox("stsz", 0, 0) { int(0); int(0) }
        fullBox("stco", 0, 0) { int(0) }
    }

    private fun Writer.trex(track: Int) = fullBox("trex", 0, 0) {
        int(track)
        int(1) // default_sample_description_index
        int(0); int(0); int(0)
    }

    /** The tkhd/mvhd transformation matrix for a clockwise rotation. */
    private fun Writer.matrix(rotation: Int) {
        val one = 0x00010000
        val minusOne = -0x00010000
        val (a, b, c, d) = when ((rotation % 360 + 360) % 360) {
            90 -> listOf(0, one, minusOne, 0)
            180 -> listOf(minusOne, 0, 0, minusOne)
            270 -> listOf(0, minusOne, one, 0)
            else -> listOf(one, 0, 0, one)
        }
        int(a); int(b); int(0)
        int(c); int(d); int(0)
        int(0); int(0); int(0x40000000)
    }

    /** Tiny big-endian box builder. */
    private class Writer {
        val out = ByteArrayOutputStream()
        private val data = DataOutputStream(out)

        fun byte(v: Int) = data.writeByte(v)
        fun short(v: Int) = data.writeShort(v)
        fun int(v: Int) = data.writeInt(v)
        fun long(v: Long) = data.writeLong(v)
        fun bytes(v: ByteArray) = data.write(v)
        fun zeros(n: Int) = repeat(n) { data.writeByte(0) }
        fun fourcc(s: String) = data.writeBytes(s)

        fun box(type: String, body: Writer.() -> Unit) {
            val inner = Writer().apply(body).out.toByteArray()
            int(8 + inner.size)
            fourcc(type)
            bytes(inner)
        }

        fun fullBox(type: String, version: Int, flags: Int, body: Writer.() -> Unit) = box(type) {
            int((version shl 24) or (flags and 0xFFFFFF))
            body()
        }
    }

    private fun build(body: Writer.() -> Unit): ByteArray = Writer().apply(body).out.toByteArray()

    companion object {
        const val VIDEO_TIMESCALE = 90_000
        private const val VIDEO_TRACK = 1
        private const val AUDIO_TRACK = 2
        private const val AUDIO_BITRATE = 64_000
        // sample_depends_on = 2 (independent)
        private const val SYNC_FLAGS = 0x02000000
        // sample_depends_on = 1, sample_is_non_sync_sample = 1
        private const val NON_SYNC_FLAGS = 0x01010000

        /** Annex-B (start codes) → AVCC (4-byte lengths), dropping SPS/PPS/AUD. */
        fun annexBToAvcc(annexB: ByteArray): ByteArray {
            val out = ByteArrayOutputStream(annexB.size + 16)
            val data = DataOutputStream(out)
            for (nal in splitNals(annexB)) {
                val type = nal[0].toInt() and 0x1F
                if (type == 7 || type == 8 || type == 9) continue
                data.writeInt(nal.size)
                data.write(nal)
            }
            return out.toByteArray()
        }

        /** Split an Annex-B stream into NAL units without start codes. */
        fun splitNals(annexB: ByteArray): List<ByteArray> {
            val starts = mutableListOf<Pair<Int, Int>>() // (code start, payload start)
            var i = 0
            while (i + 2 < annexB.size) {
                if (annexB[i].toInt() == 0 && annexB[i + 1].toInt() == 0) {
                    if (annexB[i + 2].toInt() == 1) {
                        starts += i to i + 3
                        i += 3
                        continue
                    }
                    if (i + 3 < annexB.size && annexB[i + 2].toInt() == 0 && annexB[i + 3].toInt() == 1) {
                        starts += i to i + 4
                        i += 4
                        continue
                    }
                }
                i++
            }
            if (starts.isEmpty()) return if (annexB.isEmpty()) emptyList() else listOf(annexB)
            return starts.mapIndexedNotNull { idx, (_, payload) ->
                val end = if (idx + 1 < starts.size) starts[idx + 1].first else annexB.size
                if (end > payload) annexB.copyOfRange(payload, end) else null
            }
        }
    }
}
