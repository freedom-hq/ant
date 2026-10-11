package at.vibing.ant.stream.capture

import android.annotation.SuppressLint
import android.content.Context
import android.graphics.SurfaceTexture
import android.hardware.camera2.CameraCaptureSession
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraDevice
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CaptureRequest
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaCodecList
import android.media.MediaFormat
import android.media.MediaRecorder
import android.os.Handler
import android.os.HandlerThread
import android.os.SystemClock
import android.util.Log
import android.util.Range
import android.util.Size
import android.view.Surface
import at.vibing.ant.stream.Segment
import kotlin.math.abs

/**
 * Camera + mic → H.264/AAC → fMP4 HLS segments, the Android counterpart
 * of the iOS `CaptureEngine` (#65).
 *
 * Camera2 renders into two surfaces at once: the on-screen preview and
 * the input surface of a hardware H.264 encoder. The microphone is read
 * with [AudioRecord] into an AAC-LC encoder. Both encoders feed a
 * [Segmenter], which cuts on keyframes (requested every second) and
 * hands finished segments to [onSegment] in order.
 *
 * The activity is locked to the broadcast's orientation while live. The
 * camera's frames are encoded as the sensor delivers them; the rotation
 * that makes them upright for that orientation (none for landscape on
 * usual sensors, 90° or 270° for portrait) is recorded in the track
 * header, as phone cameras do for portrait video.
 */
class CaptureEngine(
    private val context: Context,
    private val settings: Settings,
    private val withAudio: Boolean,
    private val onSegment: (Segment) -> Unit,
    private val onFailure: (String) -> Unit,
) {
    /**
     * Rendition presets. Upload throughput is the constraint (#67: a phone
     * sustains well under 1 Mbit/s of Swarm publishing on many networks,
     * #74), so the default is the lightest.
     */
    enum class Quality(val label: String, val width: Int, val height: Int, val bitrateKbps: Int) {
        Low("360p", 640, 360, 600),
        Medium("540p", 960, 540, 1000),
        High("720p", 1280, 720, 1800),
    }

    data class Settings(
        val quality: Quality = Quality.Low,
        val segmentSeconds: Int = 2,
        val frameRate: Int = 30,
        val frontCamera: Boolean = false,
        val portrait: Boolean = false,
    ) {
        val bitrateKbps get() = quality.bitrateKbps
    }

    private val cameraManager = context.getSystemService(CameraManager::class.java)
    private val cameraThread = HandlerThread("antstream.camera").apply { start() }
    private val cameraHandler = Handler(cameraThread.looper)
    private val encoderThread = HandlerThread("antstream.video").apply { start() }
    private val encoderHandler = Handler(encoderThread.looper)

    private var camera: CameraDevice? = null
    private var chars: CameraCharacteristics? = null
    private var previewSurface: Surface? = null
    private var session: CameraCaptureSession? = null
    private var videoCodec: MediaCodec? = null
    private var encoderSurface: Surface? = null
    private var audioThread: Thread? = null
    @Volatile private var running = false
    @Volatile private var realtimeClock = false

    private lateinit var segmenter: Segmenter
    private var audioRecord: AudioRecord? = null

    /** False when the microphone could not be opened: video only. */
    var hasAudio = false
        private set

    /** The encoded frame size; the preview uses the same aspect. */
    lateinit var videoSize: Size
        private set

    /** True while the front (selfie) camera is in use. */
    @Volatile var frontCamera = settings.frontCamera
        private set

    private fun cameraIdFor(front: Boolean): String? {
        val facing = if (front) CameraCharacteristics.LENS_FACING_FRONT else CameraCharacteristics.LENS_FACING_BACK
        return cameraManager.cameraIdList.firstOrNull {
            cameraManager.getCameraCharacteristics(it).get(CameraCharacteristics.LENS_FACING) == facing
        }
    }

    private val cameraId: String
        get() = cameraIdFor(frontCamera) ?: cameraIdFor(!frontCamera) ?: cameraManager.cameraIdList.first()

    /** Whether this device has both a front and a back camera to switch between. */
    val canSwitchCamera: Boolean
        get() = cameraIdFor(true) != null && cameraIdFor(false) != null

    /**
     * Rotation that makes this camera's frames upright for the locked
     * screen orientation (landscape = `ROTATION_90`, portrait =
     * `ROTATION_0`), written into the track header so players turn the
     * picture: back cameras turn against the display, front cameras with
     * it. Landscape on the usual 90° back / 270° front sensors needs none;
     * portrait needs 90° / 270°, a vertical video like any phone camera's.
     */
    private fun rotationFor(chars: CameraCharacteristics): Int {
        val sensor = chars.get(CameraCharacteristics.SENSOR_ORIENTATION) ?: 90
        val front = chars.get(CameraCharacteristics.LENS_FACING) == CameraCharacteristics.LENS_FACING_FRONT
        val display = if (settings.portrait) 0 else 90
        return if (front) (sensor + display) % 360 else ((sensor - display) % 360 + 360) % 360
    }

    private var rotation = 0

    /** Pick the frame size before the preview surface is laid out. */
    fun prepare(): Size {
        videoSize = chooseSize()
        return videoSize
    }

    /** Start capture, rendering the preview into [preview]. */
    @SuppressLint("MissingPermission")
    fun start(preview: SurfaceTexture) {
        if (running) return
        running = true
        if (!::videoSize.isInitialized) prepare()
        val chars = cameraManager.getCameraCharacteristics(cameraId)
        this.chars = chars
        realtimeClock = chars.get(CameraCharacteristics.SENSOR_INFO_TIMESTAMP_SOURCE) ==
            CameraCharacteristics.SENSOR_INFO_TIMESTAMP_SOURCE_REALTIME
        rotation = rotationFor(chars)

        // Open the mic up front: a broadcast without one is still a
        // broadcast, so a failure here means video only, not an error.
        audioRecord = if (withAudio) openMic() else null
        hasAudio = audioRecord != null
        segmenter = Segmenter(settings.segmentSeconds * 1_000_000L, hasAudio, onSegment)
        try {
            startVideoEncoder(rotation)
        } catch (e: Exception) {
            fail("video encoder: ${e.message}")
            return
        }
        audioRecord?.let { record -> audioThread = Thread({ runAudio(record) }, "antstream.audio").apply { start() } }

        preview.setDefaultBufferSize(videoSize.width, videoSize.height)
        previewSurface = Surface(preview)
        openCamera(cameraId)
    }

    /**
     * Switch between the front and back camera, live: the encoder keeps
     * running and only the camera feeding its input surface changes, so the
     * broadcast goes on with a short freeze. A camera whose frames need a
     * different rotation gets a fresh initialization segment.
     */
    fun switchCamera() {
        cameraHandler.post {
            if (!running || !canSwitchCamera) return@post
            runCatching { session?.stopRepeating() }
            runCatching { session?.close() }
            session = null
            camera?.close()
            camera = null
            frontCamera = !frontCamera
            val chars = cameraManager.getCameraCharacteristics(cameraId)
            this.chars = chars
            val r = rotationFor(chars)
            if (r != rotation) {
                rotation = r
                segmenter.reconfigure(r)
            }
            // Cut promptly: the next segment starts on the new camera.
            runCatching {
                videoCodec?.setParameters(android.os.Bundle().apply { putInt(MediaCodec.PARAMETER_KEY_REQUEST_SYNC_FRAME, 0) })
            }
            openCamera(cameraId)
        }
    }

    @SuppressLint("MissingPermission")
    private fun openCamera(id: String) {
        cameraManager.openCamera(
            id,
            object : CameraDevice.StateCallback() {
                override fun onOpened(device: CameraDevice) {
                    camera = device
                    startSession(device)
                }

                override fun onDisconnected(device: CameraDevice) {
                    device.close()
                    camera = null
                    fail("the camera was taken by another app")
                }

                override fun onError(device: CameraDevice, error: Int) {
                    device.close()
                    camera = null
                    fail("camera error $error")
                }
            },
            cameraHandler,
        )
    }

    /**
     * Swap the preview while capture keeps running: `null` when the app
     * leaves the screen (the broadcast continues into the encoder alone),
     * a fresh texture when it comes back. The camera session is rebuilt
     * with the new outputs; [onSwitched] runs once the old preview
     * surface is no longer in use, so the caller can release its texture.
     */
    fun setPreview(preview: SurfaceTexture?, onSwitched: () -> Unit = {}) {
        cameraHandler.post {
            val old = previewSurface
            previewSurface = preview?.let {
                it.setDefaultBufferSize(videoSize.width, videoSize.height)
                Surface(it)
            }
            val device = camera
            if (!running || device == null) {
                old?.release()
                onSwitched()
                return@post
            }
            runCatching { session?.stopRepeating() }
            runCatching { session?.close() }
            session = null
            startSession(device) {
                old?.release()
                onSwitched()
            }
        }
    }

    @Suppress("DEPRECATION")
    private fun startSession(device: CameraDevice, onConfigured: (() -> Unit)? = null) {
        val encoderSurface = encoderSurface ?: return
        val chars = chars ?: return
        val preview = previewSurface
        device.createCaptureSession(
            listOfNotNull(preview, encoderSurface),
            object : CameraCaptureSession.StateCallback() {
                override fun onConfigured(s: CameraCaptureSession) {
                    onConfigured?.invoke()
                    if (!running) {
                        s.close()
                        return
                    }
                    session = s
                    val request = device.createCaptureRequest(CameraDevice.TEMPLATE_RECORD).apply {
                        preview?.let { addTarget(it) }
                        addTarget(encoderSurface)
                        chooseFpsRange(chars)?.let { set(CaptureRequest.CONTROL_AE_TARGET_FPS_RANGE, it) }
                        set(CaptureRequest.CONTROL_VIDEO_STABILIZATION_MODE, CaptureRequest.CONTROL_VIDEO_STABILIZATION_MODE_ON)
                    }.build()
                    runCatching { s.setRepeatingRequest(request, null, cameraHandler) }
                        .onFailure { fail("camera: ${it.message}") }
                }

                override fun onConfigureFailed(s: CameraCaptureSession) {
                    onConfigured?.invoke()
                    fail("the camera could not be configured at ${videoSize.width}x${videoSize.height}")
                }
            },
            cameraHandler,
        )
    }

    /**
     * Stop capture. The last partial segment is cut and delivered before
     * [onDone] runs, so the final seconds of a broadcast are published.
     */
    fun stop(onDone: () -> Unit) {
        if (!running) {
            onDone()
            return
        }
        running = false
        cameraHandler.post {
            runCatching { session?.stopRepeating() }
            runCatching { session?.close() }
            runCatching { camera?.close() }
            session = null
            camera = null
            previewSurface?.release()
            previewSurface = null
            encoderHandler.post {
                runCatching { videoCodec?.stop() }
                runCatching { videoCodec?.release() }
                videoCodec = null
                encoderSurface?.release()
                encoderSurface = null
                audioThread?.join(2_000)
                audioThread = null
                if (::segmenter.isInitialized) segmenter.finish()
                cameraThread.quitSafely()
                encoderThread.quitSafely()
                onDone()
            }
        }
    }

    /** Free the worker threads of an engine that never started. */
    fun release() {
        if (running) return
        cameraThread.quitSafely()
        encoderThread.quitSafely()
    }

    private fun fail(message: String) {
        Log.w(TAG, message)
        if (running) onFailure(message)
    }

    // -- video -------------------------------------------------------------

    private fun startVideoEncoder(rotation: Int) {
        val format = MediaFormat.createVideoFormat(MediaFormat.MIMETYPE_VIDEO_AVC, videoSize.width, videoSize.height).apply {
            setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatSurface)
            setInteger(MediaFormat.KEY_BIT_RATE, settings.bitrateKbps * 1000)
            setInteger(MediaFormat.KEY_FRAME_RATE, settings.frameRate)
            // One keyframe a second: segments cut on the first keyframe
            // past the target, so they land within a second of it.
            setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, 1)
            setInteger(MediaFormat.KEY_BITRATE_MODE, MediaCodecInfo.EncoderCapabilities.BITRATE_MODE_CBR)
        }
        val codec = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_VIDEO_AVC)
        codec.setCallback(
            object : MediaCodec.Callback() {
                override fun onInputBufferAvailable(codec: MediaCodec, index: Int) = Unit

                override fun onOutputBufferAvailable(codec: MediaCodec, index: Int, info: MediaCodec.BufferInfo) {
                    val buffer = runCatching { codec.getOutputBuffer(index) }.getOrNull()
                    if (buffer != null && info.size > 0 &&
                        info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG == 0
                    ) {
                        val bytes = ByteArray(info.size)
                        buffer.position(info.offset)
                        buffer.get(bytes)
                        val sync = info.flags and MediaCodec.BUFFER_FLAG_KEY_FRAME != 0
                        segmenter.onVideo(Fmp4Muxer.annexBToAvcc(bytes), info.presentationTimeUs, sync)
                    }
                    runCatching { codec.releaseOutputBuffer(index, false) }
                }

                override fun onOutputFormatChanged(codec: MediaCodec, format: MediaFormat) {
                    val sps = format.getByteBuffer("csd-0")?.toArray()?.let { Fmp4Muxer.splitNals(it).firstOrNull() }
                    val pps = format.getByteBuffer("csd-1")?.toArray()?.let { Fmp4Muxer.splitNals(it).firstOrNull() }
                    if (sps == null || pps == null || sps.size < 4) {
                        fail("the video encoder gave no H.264 parameters")
                        return
                    }
                    segmenter.setVideoConfig(Fmp4Muxer.VideoConfig(videoSize.width, videoSize.height, sps, pps, rotation))
                }

                override fun onError(codec: MediaCodec, e: MediaCodec.CodecException) {
                    fail("video encoder: ${e.diagnosticInfo}")
                }
            },
            encoderHandler,
        )
        codec.configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
        encoderSurface = codec.createInputSurface()
        codec.start()
        videoCodec = codec
    }

    /**
     * The encoded size: the size closest to the chosen quality that every
     * camera (front and back, so a live switch keeps the encoder) and the
     * encoder support. 16:9 is preferred, but not at the price of a much
     * bigger frame: many cameras lack a small 16:9 size, and 640x480 beats
     * 1280x720 for a 360p budget.
     */
    private fun chooseSize(): Size {
        val caps = avcCapabilities()
        val ids = listOfNotNull(cameraIdFor(false), cameraIdFor(true)).ifEmpty { listOf(cameraId) }
        val perCamera = ids.map { id ->
            val map = cameraManager.getCameraCharacteristics(id).get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP)
            val encoder = map?.getOutputSizes(MediaCodec::class.java)?.toSet().orEmpty()
            val preview = map?.getOutputSizes(SurfaceTexture::class.java)?.toSet().orEmpty()
            encoder intersect preview
        }
        var usable = perCamera.reduce { a, b -> a intersect b }
            .filter { caps?.isSizeSupported(it.width, it.height) ?: true }
        if (usable.isEmpty()) usable = perCamera.first().toList()
        val q = settings.quality
        val wide = { s: Size -> abs(s.width * 9 - s.height * 16) <= 16 }
        // Pixel count decides the bandwidth; aspect only breaks near-ties.
        val cost = { s: Size ->
            abs(s.width.toLong() * s.height - q.width.toLong() * q.height) + if (wide(s)) 0 else q.width.toLong() * q.height / 4
        }
        return usable.minByOrNull(cost) ?: Size(640, 480)
    }

    private fun avcCapabilities(): MediaCodecInfo.VideoCapabilities? =
        MediaCodecList(MediaCodecList.REGULAR_CODECS).codecInfos
            .firstOrNull { it.isEncoder && MediaFormat.MIMETYPE_VIDEO_AVC in it.supportedTypes }
            ?.getCapabilitiesForType(MediaFormat.MIMETYPE_VIDEO_AVC)?.videoCapabilities

    private fun chooseFpsRange(chars: CameraCharacteristics): Range<Int>? {
        val ranges = chars.get(CameraCharacteristics.CONTROL_AE_AVAILABLE_TARGET_FPS_RANGES) ?: return null
        return ranges.filter { it.upper == settings.frameRate }.maxByOrNull { it.lower }
            ?: ranges.maxByOrNull { it.upper }
    }

    // -- audio -------------------------------------------------------------

    @SuppressLint("MissingPermission")
    private fun openMic(): AudioRecord? {
        val minBuf = AudioRecord.getMinBufferSize(AUDIO_RATE, AudioFormat.CHANNEL_IN_MONO, AudioFormat.ENCODING_PCM_16BIT)
        if (minBuf <= 0) return null
        val record = runCatching {
            AudioRecord(
                MediaRecorder.AudioSource.CAMCORDER,
                AUDIO_RATE,
                AudioFormat.CHANNEL_IN_MONO,
                AudioFormat.ENCODING_PCM_16BIT,
                maxOf(minBuf, 8192) * 2,
            )
        }.getOrNull()
        if (record == null || record.state != AudioRecord.STATE_INITIALIZED) {
            record?.release()
            Log.w(TAG, "microphone unavailable; broadcasting video only")
            return null
        }
        return record
    }

    private fun runAudio(record: AudioRecord) {
        val rate = AUDIO_RATE
        val format = MediaFormat.createAudioFormat(MediaFormat.MIMETYPE_AUDIO_AAC, rate, 1).apply {
            setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC)
            setInteger(MediaFormat.KEY_BIT_RATE, 64_000)
            setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, 16_384)
        }
        val codec = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_AUDIO_AAC)
        codec.configure(format, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
        codec.start()
        record.startRecording()
        val pcm = ByteArray(2048)
        var framesRead = 0L
        var baseUs = -1L
        val info = MediaCodec.BufferInfo()
        try {
            while (running) {
                val n = record.read(pcm, 0, pcm.size)
                if (n <= 0) continue
                if (baseUs < 0) {
                    // Stamp audio on the camera's clock so the two tracks
                    // line up; count samples from there so the timeline
                    // never jitters.
                    baseUs = nowUs() - (n / 2) * 1_000_000L / rate
                }
                val ptsUs = baseUs + framesRead * 1_000_000L / rate
                framesRead += n / 2
                val inIndex = codec.dequeueInputBuffer(10_000)
                if (inIndex >= 0) {
                    codec.getInputBuffer(inIndex)?.apply {
                        clear()
                        put(pcm, 0, n)
                    }
                    codec.queueInputBuffer(inIndex, 0, n, ptsUs, 0)
                }
                drainAudio(codec, info, rate)
            }
        } catch (e: Exception) {
            fail("audio: ${e.message}")
        } finally {
            runCatching { record.stop() }
            record.release()
            runCatching { codec.stop() }
            codec.release()
        }
    }

    private fun drainAudio(codec: MediaCodec, info: MediaCodec.BufferInfo, rate: Int) {
        while (true) {
            val index = codec.dequeueOutputBuffer(info, 0)
            when {
                index == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
                    val asc = codec.outputFormat.getByteBuffer("csd-0")?.toArray() ?: byteArrayOf(0x11, 0x88.toByte())
                    segmenter.setAudioConfig(Fmp4Muxer.AudioConfig(rate, 1, asc))
                }
                index >= 0 -> {
                    val buffer = codec.getOutputBuffer(index)
                    if (buffer != null && info.size > 0 && info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG == 0) {
                        val bytes = ByteArray(info.size)
                        buffer.position(info.offset)
                        buffer.get(bytes)
                        segmenter.onAudio(bytes, info.presentationTimeUs)
                    }
                    codec.releaseOutputBuffer(index, false)
                }
                else -> return
            }
        }
    }

    private fun nowUs(): Long =
        if (realtimeClock) SystemClock.elapsedRealtimeNanos() / 1000 else System.nanoTime() / 1000

    private fun java.nio.ByteBuffer.toArray(): ByteArray {
        val copy = duplicate()
        copy.rewind()
        return ByteArray(copy.remaining()).also { copy.get(it) }
    }

    companion object {
        private const val TAG = "CaptureEngine"
        private const val AUDIO_RATE = 48_000
    }
}
