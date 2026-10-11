package at.vibing.ant.stream

import android.content.Context
import android.graphics.SurfaceTexture
import android.os.Build
import at.vibing.ant.stream.capture.CaptureEngine
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlin.coroutines.resume

/**
 * Drives one broadcast: capture → publisher, mirroring `LiveBroadcast.swift`.
 *
 * Segments go to `ant_publisher_push_segment` through one single-thread
 * executor, because the publisher numbers segments in arrival order and
 * that number fixes the playlist order and which `#EXT-X-MAP` a segment
 * is listed under. The push never blocks; backpressure is the node's
 * drop-oldest backlog.
 */
class LiveBroadcast(private val context: Context, private val node: AntNode) {
    sealed interface State {
        data object Idle : State
        data object Starting : State
        data object Live : State
        data object Stopping : State
        data class Failed(val message: String) : State
    }

    private val _state = MutableStateFlow<State>(State.Idle)
    val state: StateFlow<State> = _state.asStateFlow()
    private val _progress = MutableStateFlow<PublisherSnapshot?>(null)
    val progress: StateFlow<PublisherSnapshot?> = _progress.asStateFlow()
    private val _report = MutableStateFlow<PublisherReport?>(null)
    val report: StateFlow<PublisherReport?> = _report.asStateFlow()
    private val _captured = MutableStateFlow(0L)
    val captured: StateFlow<Long> = _captured.asStateFlow()

    val isBusy get() = state.value.let { it is State.Starting || it is State.Live || it is State.Stopping }

    var settings = CaptureEngine.Settings()

    /**
     * Debug self-test (`adb shell am start … --es antstream.selftest <dir>`):
     * segments are written to this directory instead of being published,
     * so the capture pipeline can be checked without a storage plan.
     */
    var selfTestDir: java.io.File? = null

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private var engine: CaptureEngine? = null
    private var pushQueue = Executors.newSingleThreadExecutor()
    private var poll: Job? = null
    private val capturedCount = AtomicLong()

    /** The frame size the engine will encode; lay the preview out to it. */
    fun prepareEngine(withAudio: Boolean): CaptureEngine {
        engine?.let { return it }
        pushQueue = Executors.newSingleThreadExecutor()
        val e = CaptureEngine(
            context,
            settings,
            withAudio,
            onSegment = { segment ->
                val n = capturedCount.incrementAndGet()
                pushQueue.execute {
                    val dir = selfTestDir
                    if (dir != null) {
                        dir.mkdirs()
                        java.io.File(dir, if (segment.isInit) "init-$n.mp4" else "seg-$n.m4s").writeBytes(segment.data)
                        java.io.File(dir, "durations.txt").appendText("$n ${segment.isInit} ${segment.durationMs}\n")
                    } else {
                        node.pushSegment(segment)
                    }
                    _captured.value = capturedCount.get()
                }
            },
            onFailure = { message -> scope.launch { stop(failure = message) } },
        )
        e.prepare()
        engine = e
        return e
    }

    suspend fun start(channel: String, preview: SurfaceTexture, withAudio: Boolean) {
        if (isBusy) return
        _report.value = null
        _progress.value = null
        capturedCount.set(0)
        _captured.value = 0
        _state.value = State.Starting
        val plan = node.plan.value
        val selfTest = selfTestDir != null
        if (!selfTest && (plan == null || !plan.enabled)) {
            _state.value = State.Failed("Connect a storage plan before going live.")
            return
        }
        if (!selfTest) try {
            node.startPublisher(
                channel = channel,
                batchId = plan!!.batchId,
                bitrateKbps = settings.bitrateKbps,
                segmentMs = settings.segmentSeconds * 1000,
                notes = "Android ${Build.VERSION.RELEASE} / ${Build.MANUFACTURER} ${Build.MODEL} / " +
                    "${node.networkLabel.value} / camera",
            )
        } catch (e: Exception) {
            _state.value = State.Failed(e.message ?: "could not start the broadcast")
            engine = null
            return
        }
        val e = prepareEngine(withAudio)
        try {
            e.start(preview)
        } catch (ex: Exception) {
            stop(failure = "camera: ${ex.message}")
            return
        }
        _state.value = State.Live
        poll = scope.launch {
            while (isActive && selfTestDir == null) {
                node.publisherProgress()?.let { _progress.value = it }
                delay(1_000)
            }
        }
    }

    /**
     * End the broadcast: capture stops first so the segment in flight is
     * finished and pushed, then the publisher drains and closes the
     * playlist with `#EXT-X-ENDLIST`.
     */
    suspend fun stop(failure: String? = null) {
        if (!isBusy) {
            engine = null
            return
        }
        if (state.value == State.Stopping) return
        _state.value = State.Stopping
        poll?.cancel()
        poll = null
        engine?.let { e ->
            suspendCancellableCoroutine { cont -> e.stop { cont.resume(Unit) } }
        }
        engine = null
        withContext(Dispatchers.IO) {
            pushQueue.shutdown()
            pushQueue.awaitTermination(5, TimeUnit.SECONDS)
        }
        if (selfTestDir == null) try {
            _report.value = node.stopPublisher()
        } catch (e: Exception) {
            if (failure == null) {
                _state.value = State.Failed(e.message ?: "could not stop the broadcast")
                return
            }
        }
        _state.value = failure?.let { State.Failed(it) } ?: State.Idle
    }

    /** Drop a prepared-but-unused engine (the live screen closed). */
    fun discardEngine() {
        if (isBusy) return
        engine?.release()
        engine = null
    }
}
