package at.vibing.ant.stream.capture

import at.vibing.ant.stream.Segment
import kotlin.math.roundToLong

/**
 * Cuts the encoders' output into HLS segments: an initialization segment
 * once both tracks are configured, then one media segment per GOP run of
 * at least [segmentUs], always starting on a video keyframe so every
 * segment decodes on its own.
 *
 * Video timestamps are the camera's; audio is stamped on the same clock
 * by [CaptureEngine]. Both are rebased to the first video keyframe, so
 * the tracks share a zero and stay in sync across segments.
 *
 * Thread-safe: the video and audio encoders deliver from their own
 * threads. [onSegment] is called with the lock held, in segment order.
 */
class Segmenter(
    private val segmentUs: Long,
    private val expectAudio: Boolean,
    private val onSegment: (Segment) -> Unit,
) {
    private class Pending(val data: ByteArray, val ptsUs: Long, val isSync: Boolean)

    private var videoConfig: Fmp4Muxer.VideoConfig? = null
    private var audioConfig: Fmp4Muxer.AudioConfig? = null
    private var muxer: Fmp4Muxer? = null
    private var originUs = -1L
    private var segmentStartUs = 0L
    private var sequence = 1
    private var pendingVideo: Pending? = null
    private val videoSamples = mutableListOf<Fmp4Muxer.Sample>()
    private var videoBase = 0L
    private val audioQueue = ArrayDeque<Pending>()
    private var audioNextDts = -1L
    private var lastVideoDuration = Fmp4Muxer.VIDEO_TIMESCALE / 30
    /**
     * Added to every audio timestamp to put it on the video's clock. The
     * camera stamps frames on a clock the device reports only loosely
     * (some phones say "unknown" and use boot time, which runs ahead of
     * the monotonic clock by every second the phone ever slept); audio
     * stamped on the other clock would then sit hours away from the video
     * and be dropped as "before the first frame", leaving segments with a
     * declared but empty audio track that stalls browser playback. So the
     * offset is measured from the streams themselves: null until both
     * have produced a frame, 0 when they already agree.
     */
    private var audioOffsetUs: Long? = null
    private var lastVideoPtsUs = Long.MIN_VALUE

    @Synchronized
    fun setVideoConfig(config: Fmp4Muxer.VideoConfig) {
        videoConfig = config
    }

    @Synchronized
    fun setAudioConfig(config: Fmp4Muxer.AudioConfig) {
        audioConfig = config
    }

    /** One encoded access unit in AVCC form. */
    @Synchronized
    fun onVideo(avcc: ByteArray, ptsUs: Long, isSync: Boolean) {
        lastVideoPtsUs = ptsUs
        if (audioOffsetUs == null && audioQueue.isNotEmpty()) alignAudio(audioQueue.last().ptsUs)
        if (muxer == null) {
            val video = videoConfig ?: return
            if (!isSync) return
            if (expectAudio && audioConfig == null) return
            val m = Fmp4Muxer(video, if (expectAudio) audioConfig else null)
            muxer = m
            originUs = ptsUs
            segmentStartUs = ptsUs
            // Audio captured before the first keyframe has no video to
            // sit next to.
            while (audioQueue.isNotEmpty() && audioQueue.first().ptsUs < ptsUs) audioQueue.removeFirst()
            onSegment(Segment(isInit = true, data = m.initSegment(), durationMs = 0, discontinuity = false))
        }
        pendingVideo?.let { prev ->
            val duration = (videoDts(ptsUs) - videoDts(prev.ptsUs)).toInt()
                .takeIf { it > 0 } ?: lastVideoDuration
            lastVideoDuration = duration
            if (videoSamples.isEmpty()) videoBase = videoDts(prev.ptsUs)
            videoSamples += Fmp4Muxer.Sample(prev.data, duration, prev.isSync)
        }
        if (isSync && videoSamples.isNotEmpty() && ptsUs - segmentStartUs >= segmentUs * 9 / 10) {
            flush(cutUs = ptsUs)
        }
        pendingVideo = Pending(avcc, ptsUs, isSync)
    }

    /** One encoded AAC frame (1024 samples). */
    @Synchronized
    fun onAudio(frame: ByteArray, ptsUs: Long) {
        if (audioOffsetUs == null && lastVideoPtsUs != Long.MIN_VALUE) alignAudio(ptsUs)
        val pts = ptsUs + (audioOffsetUs ?: 0)
        if (muxer != null && pts < originUs) return
        audioQueue.addLast(Pending(frame, pts, true))
    }

    /**
     * Fix [audioOffsetUs] from an audio timestamp captured about now and
     * the newest video timestamp. Both were taken within a frame or two of
     * each other, so a gap of more than a couple of seconds is a clock
     * mismatch, not latency. Queued audio is shifted onto the video clock.
     */
    private fun alignAudio(audioPtsUs: Long) {
        val delta = lastVideoPtsUs - audioPtsUs
        val offset = if (kotlin.math.abs(delta) > CLOCK_MISMATCH_US) delta else 0L
        audioOffsetUs = offset
        if (offset != 0L && audioQueue.isNotEmpty()) {
            val shifted = audioQueue.map { Pending(it.data, it.ptsUs + offset, it.isSync) }
            audioQueue.clear()
            audioQueue.addAll(shifted)
        }
    }

    /** Emit whatever is buffered as the final segment. */
    @Synchronized
    fun finish() {
        val prev = pendingVideo ?: return
        if (muxer == null) return
        if (videoSamples.isEmpty()) videoBase = videoDts(prev.ptsUs)
        videoSamples += Fmp4Muxer.Sample(prev.data, lastVideoDuration, prev.isSync)
        pendingVideo = null
        val endUs = prev.ptsUs + lastVideoDuration * 1_000_000L / Fmp4Muxer.VIDEO_TIMESCALE
        flush(cutUs = endUs)
    }

    private fun flush(cutUs: Long) {
        val m = muxer ?: return
        val audio = audioConfig
        val audioSamples = mutableListOf<Fmp4Muxer.Sample>()
        var audioBase = 0L
        if (expectAudio && audio != null) {
            while (audioQueue.isNotEmpty() && audioQueue.first().ptsUs < cutUs) {
                val frame = audioQueue.removeFirst()
                if (audioNextDts < 0) {
                    audioNextDts = ((frame.ptsUs - originUs) * audio.sampleRate / 1_000_000.0).roundToLong()
                        .coerceAtLeast(0)
                }
                if (audioSamples.isEmpty()) audioBase = audioNextDts
                audioSamples += Fmp4Muxer.Sample(frame.data, AAC_FRAME, true)
                audioNextDts += AAC_FRAME
            }
        }
        val data = m.mediaSegment(sequence++, videoSamples.toList(), videoBase, audioSamples, audioBase)
        val durationMs = ((cutUs - segmentStartUs) / 1000).toInt().coerceAtLeast(1)
        videoSamples.clear()
        segmentStartUs = cutUs
        onSegment(Segment(isInit = false, data = data, durationMs = durationMs, discontinuity = false))
    }

    private fun videoDts(ptsUs: Long): Long =
        ((ptsUs - originUs) * Fmp4Muxer.VIDEO_TIMESCALE / 1_000_000.0).roundToLong()

    companion object {
        const val AAC_FRAME = 1024
        private const val CLOCK_MISMATCH_US = 2_000_000L
    }
}
