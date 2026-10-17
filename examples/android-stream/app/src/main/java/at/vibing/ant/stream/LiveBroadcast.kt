package at.vibing.ant.stream

import android.content.Context
import android.graphics.SurfaceTexture
import android.os.Build
import at.vibing.ant.stream.capture.CaptureEngine
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
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
import kotlinx.coroutines.withTimeoutOrNull
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

    private val prefs = context.getSharedPreferences("antstream.capture", Context.MODE_PRIVATE)

    private val _quality = MutableStateFlow(
        runCatching { CaptureEngine.Quality.valueOf(prefs.getString("quality", null) ?: "") }
            .getOrDefault(CaptureEngine.Quality.Low),
    )
    /** The rendition the next broadcast encodes; remembered. */
    val quality: StateFlow<CaptureEngine.Quality> = _quality.asStateFlow()

    private val _frontCamera = MutableStateFlow(prefs.getBoolean("front", false))
    /** Front (selfie) or back camera; remembered, switchable while live. */
    val frontCamera: StateFlow<Boolean> = _frontCamera.asStateFlow()

    private val _portrait = MutableStateFlow(prefs.getBoolean("portrait", false))
    /** Broadcast in portrait (vertical video) or landscape; remembered. */
    val portrait: StateFlow<Boolean> = _portrait.asStateFlow()

    val settings
        get() = CaptureEngine.Settings(
            quality = _quality.value,
            frontCamera = _frontCamera.value,
            portrait = _portrait.value,
        )

    /** Pick the orientation before going live; drops a prepared engine. */
    fun setPortrait(value: Boolean) {
        if (isBusy || value == _portrait.value) return
        _portrait.value = value
        prefs.edit().putBoolean("portrait", value).apply()
        discardEngine()
    }

    /** Pick the rendition before going live; drops a prepared engine. */
    fun setQuality(q: CaptureEngine.Quality) {
        if (isBusy || q == _quality.value) return
        _quality.value = q
        prefs.edit().putString("quality", q.name).apply()
        discardEngine()
    }

    /**
     * Whether [switchCamera] can swap cameras during the running broadcast
     * ([CaptureEngine.canSwitchCamera]): the live screen offers the switch
     * only when it does something.
     */
    val canSwitchLive: Boolean get() = engine?.canSwitchCamera ?: false

    /**
     * Whether the running engine's device has a second camera at all, so
     * the live screen can tell "only one camera" (nothing to say) apart from
     * "two cameras mounted differently" when [canSwitchLive] is false.
     */
    val hasBothCamerasLive: Boolean get() = engine?.hasBothCameras ?: false

    /** Swap front/back camera, live or not. */
    fun switchCamera() {
        val e = engine
        if (isBusy && e != null) {
            if (!e.canSwitchCamera) return
            e.switchCamera()
        } else {
            // Not live: the prepared engine was built for the other camera.
            discardEngine()
        }
        _frontCamera.value = !_frontCamera.value
        prefs.edit().putBoolean("front", _frontCamera.value).apply()
    }

    /**
     * Debug self-test (`adb shell am start … --es antstream.selftest <dir>`):
     * segments are written to this directory instead of being published,
     * so the capture pipeline can be checked without a storage plan.
     */
    var selfTestDir: java.io.File? = null

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    private val announcer = Announcer(scope)

    /**
     * An announcement is being written to the catalog ([announceAsync]):
     * Go live waits for it ([ANNOUNCING_HOLD_NOTE]), as the node refuses a
     * start meanwhile.
     */
    val announcing: StateFlow<Boolean> = announcer.running

    /**
     * The last announcement's failure (null once one succeeds), for the
     * stream info card and dialog. Kept until the next announcement, so
     * show it only while [announceErrorFor] says it is about what's on
     * screen.
     */
    internal val announceFailure: StateFlow<Announcer.Failure?> = announcer.lastFailure

    /** The [Announcer.Failure.seq] the next announcement gets. */
    val nextAnnounceSeq: Long get() = announcer.nextSeq

    /**
     * [failure]'s error if it was about [owner]'s channel and its saved
     * [startMs] (what the card shows now), else null: a failure for an
     * earlier start time, or another account restored since, no longer
     * says anything about this one.
     */
    internal fun announceErrorFor(failure: Announcer.Failure?, owner: String, startMs: Long?): String? =
        failure?.takeIf { startMs != null && it.isAbout(announceSubject(owner, startMs)) }?.error
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
        // Held while an announcement is out. Both screens show
        // [ANNOUNCING_HOLD_NOTE] from [announcing] itself, so it goes away
        // with the write; a Failed state here would outlive it.
        if (announcing.value) return
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
        val ch = node.channel.value
        if (!selfTest && ch == null) {
            _state.value = State.Failed("The account is still loading; try again in a moment.")
            return
        }
        if (!selfTest) try {
            ch!!.name = channel
            val info = ch.info.value
            node.startPublisher(
                channel = channel,
                batchId = plan!!.batchId,
                bitrateKbps = settings.bitrateKbps,
                segmentMs = settings.segmentSeconds * 1000,
                notes = "Android ${Build.VERSION.RELEASE} / ${Build.MANUFACTURER} ${Build.MODEL} / " +
                    "${node.networkLabel.value} / camera",
                info = info.forBroadcast(channel),
                // The selected quality's size, as the announcement uses: the
                // camera picks the nearest size it supports, which the
                // publisher starts before.
                width = if (settings.portrait) settings.quality.height else settings.quality.width,
                height = if (settings.portrait) settings.quality.width else settings.quality.height,
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
        node.liveBroadcastActive = true
        // Keeps capture and publishing alive when the app leaves the screen.
        BroadcastService.start(context, channel, e.hasAudio)
        if (!selfTest) {
            _infoError.value = null
            thumbnailJob = scope.launch { thumbnailAtStart(ch!!, channel, e) }
        }
        poll = scope.launch {
            while (isActive && selfTestDir == null) {
                node.publisherProgress()?.let { _progress.value = it }
                delay(1_000)
            }
        }
    }

    /**
     * End the broadcast: capture stops first so the segment in flight is
     * finished and pushed, then the publisher drains, closes the
     * playlist with `#EXT-X-ENDLIST` and publishes the whole broadcast
     * as the recording.
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
        thumbnailJob?.cancel()
        thumbnailJob = null
        engine?.let { e ->
            suspendCancellableCoroutine { cont -> e.stop { cont.resume(Unit) } }
        }
        engine = null
        withContext(Dispatchers.IO) {
            pushQueue.shutdown()
            pushQueue.awaitTermination(5, TimeUnit.SECONDS)
        }
        // The foreground service stays up until the publisher has drained:
        // in the background, Android freezes the app the moment it goes,
        // and the closing playlist (#EXT-X-ENDLIST) would never land.
        try {
            if (selfTestDir == null) {
                _report.value = node.stopPublisher()
                node.channel.value?.updateInfo { it.afterBroadcast() }
            }
        } catch (e: Exception) {
            if (failure == null) {
                _state.value = State.Failed(e.message ?: "could not stop the broadcast")
                return
            }
        } finally {
            node.liveBroadcastActive = false
            BroadcastService.stop(context)
        }
        _state.value = failure?.let { State.Failed(it) } ?: State.Idle
    }

    // -- stream info (issue #220) -------------------------------------------

    private val _infoError = MutableStateFlow<String?>(null)

    /** Why the last thumbnail upload or info update failed; null after one works. */
    val infoError: StateFlow<String?> = _infoError.asStateFlow()

    private var thumbnailJob: Job? = null

    private val isLivePublishing get() = state.value == State.Live && selfTestDir == null

    /**
     * Right after Go live: a picked photo still waiting for its upload
     * goes up now; with no picked photo, a frame from the camera is taken
     * a few seconds in ([THUMBNAIL_DELAY_MS], once the exposure settled)
     * and becomes the thumbnail. Either way the catalog entry gets it
     * through `ant_publisher_set_info`, and the account remembers it. The
     * broadcast never waits for any of this.
     */
    private suspend fun thumbnailAtStart(ch: Channel, channelName: String, e: CaptureEngine) {
        val saved = ch.info.value
        if (saved.thumbnailPicked) {
            if (saved.thumbnailPending) uploadPendingThumbnail(ch, channelName)
            return
        }
        delay(THUMBNAIL_DELAY_MS)
        captureThumbnail(ch, channelName, e)
    }

    private suspend fun captureThumbnail(ch: Channel, channelName: String, e: CaptureEngine) {
        val frame = withTimeoutOrNull(5_000) {
            suspendCancellableCoroutine<android.graphics.Bitmap?> { cont -> e.snapshot { if (cont.isActive) cont.resume(it) } }
        } ?: return
        val jpeg = withContext(Dispatchers.Default) { Thumbnail.encode(frame).also { frame.recycle() } }
        try {
            val ref = node.uploadThumbnail(jpeg)
            // A photo picked meanwhile wins over the captured frame.
            if (ch.info.value.thumbnailPicked) return
            withContext(Dispatchers.IO) { writeThumbnailFile(ch, jpeg) }
            ch.updateInfo { it.copy(info = it.info.copy(thumbnail = ref)) }
            pushInfo(ch, channelName)
        } catch (ex: Exception) {
            _infoError.value = "Thumbnail: ${ex.message}"
        }
    }

    private suspend fun uploadPendingThumbnail(ch: Channel, channelName: String) {
        try {
            val jpeg = withContext(Dispatchers.IO) { ch.thumbnailFile.readBytes() }
            val ref = node.uploadThumbnail(jpeg)
            ch.updateInfo { it.copy(info = it.info.copy(thumbnail = ref), thumbnailPending = false) }
            pushInfo(ch, channelName)
        } catch (ex: Exception) {
            _infoError.value = "Thumbnail: ${ex.message}"
        }
    }

    /** The running broadcast takes the remembered info; nothing when not live. */
    private suspend fun pushInfo(ch: Channel, channelName: String) {
        if (!isLivePublishing) return
        node.setPublisherInfo(ch.info.value.forBroadcast(channelName))
        _infoError.value = null
    }

    private fun writeThumbnailFile(ch: Channel, jpeg: ByteArray) {
        ch.thumbnailFile.parentFile?.mkdirs()
        ch.thumbnailFile.writeBytes(jpeg)
    }

    /**
     * Save the editor's stream info for this account, before going live or
     * while live. A [pickedPhoto] (a thumbnail JPEG) goes up at once when
     * there is a storage plan, at Go live otherwise; [removePhoto] drops the
     * picked one (a live broadcast then gets a camera frame again). While
     * live, the broadcast's catalog entry is updated at once. Returns a
     * message for anything that did not work; the info is saved anyway.
     */
    suspend fun saveInfo(
        channelName: String,
        title: String,
        description: String,
        scheduledStartMs: Long?,
        pickedPhoto: ByteArray?,
        removePhoto: Boolean,
    ): String? {
        val ch = node.channel.value ?: return "The account is still loading; try again in a moment."
        var problem: String? = null
        ch.updateInfo { s ->
            var next = s.copy(info = s.info.copy(title = title.trim(), description = description.trim(), scheduledStartMs = scheduledStartMs))
            if (removePhoto && pickedPhoto == null) {
                next = next.copy(info = next.info.copy(thumbnail = null), thumbnailPicked = false, thumbnailPending = false)
            }
            if (pickedPhoto != null) {
                next = next.copy(info = next.info.copy(thumbnail = null), thumbnailPicked = true, thumbnailPending = true)
            }
            next
        }
        withContext(Dispatchers.IO) {
            if (pickedPhoto != null) writeThumbnailFile(ch, pickedPhoto)
            else if (removePhoto) ch.thumbnailFile.delete()
        }
        if (pickedPhoto != null && node.hasStorage) {
            try {
                val ref = node.uploadThumbnail(pickedPhoto)
                ch.updateInfo { it.copy(info = it.info.copy(thumbnail = ref), thumbnailPending = false) }
            } catch (ex: Exception) {
                problem = "Saved. The photo goes up at Go live (upload failed: ${ex.message})."
            }
        }
        if (isLivePublishing) {
            try {
                pushInfo(ch, channelName)
            } catch (ex: Exception) {
                problem = "Saved, but the live broadcast didn't take it: ${ex.message}"
            }
            val e = engine
            if (removePhoto && pickedPhoto == null && e != null) {
                thumbnailJob?.cancel()
                thumbnailJob = scope.launch { captureThumbnail(ch, channelName, e) }
            }
        }
        return problem
    }

    /**
     * Announce the saved info's scheduled start on the account's catalog
     * feed (`ant_stream_catalog_publish`), with the rendition the next
     * broadcast will have. Needs a storage plan; refused while live.
     * Returns the library's error, or null once announced.
     *
     * Runs on this object's own scope, not the caller's: the native write
     * keeps going once called, so a caller that goes away (the dialog on a
     * rotation) must not take the recording of its result with it. One at
     * a time; a call while one is out returns that one.
     */
    fun announceAsync(channelName: String): Deferred<String?> {
        val ch = node.channel.value
        val subject = ch?.let { announceSubject(it.owner, it.info.value.info.scheduledStartMs) }
        return announcer.launch(subject) { announce(channelName) }
    }

    /** What an announcement is about: the account and the start time it announces. */
    private fun announceSubject(owner: String, startMs: Long?): Any = owner to startMs

    private suspend fun announce(channelName: String): String? {
        if (isBusy) return "Announcing is for an upcoming broadcast; you're live now."
        val ch = node.channel.value ?: return "The account is still loading; try again in a moment."
        if (!node.hasStorage) return "Connect a storage plan to announce a broadcast."
        if (ch.info.value.info.scheduledStartMs == null) return "Pick a start time first."
        return try {
            if (ch.info.value.thumbnailPending) {
                val ref = node.uploadThumbnail(withContext(Dispatchers.IO) { ch.thumbnailFile.readBytes() })
                ch.updateInfo { it.copy(info = it.info.copy(thumbnail = ref), thumbnailPending = false) }
            }
            val saved = ch.info.value
            val q = settings.quality
            val (w, h) = if (settings.portrait) q.height to q.width else q.width to q.height
            node.announceBroadcast(channelName, saved.published(channelName), w, h, q.bitrateKbps)
            ch.updateInfo { it.copy(announcedStartMs = saved.info.scheduledStartMs) }
            null
        } catch (ex: Exception) {
            ex.message ?: "could not announce the broadcast"
        }
    }

    /**
     * Swap the live preview: `null` when the screen goes away (capture
     * continues into the encoder), a texture when it comes back.
     */
    fun setPreview(preview: SurfaceTexture?, onSwitched: () -> Unit = {}) {
        val e = engine
        if (e == null || !isBusy) {
            onSwitched()
            return
        }
        e.setPreview(preview, onSwitched)
    }

    /** The live screen's preview texture while it exists. */
    @Volatile var previewTexture: SurfaceTexture? = null

    /** The app left the screen: keep broadcasting, without a preview. */
    fun onScreenHidden() {
        if (isBusy) setPreview(null)
    }

    /** The app is back: show the preview again. */
    fun onScreenShown() {
        if (isBusy) previewTexture?.let { setPreview(it) }
    }

    /** Drop a prepared-but-unused engine (the live screen closed). */
    fun discardEngine() {
        if (isBusy) return
        engine?.release()
        engine = null
    }

    companion object {
        /** How long after Go live the thumbnail frame is taken. */
        private const val THUMBNAIL_DELAY_MS = 4_000L

        /** Why Go live is held while [announcing]: the Broadcast and live screens' note. */
        const val ANNOUNCING_HOLD_NOTE =
            "An announcement is still being written to the catalog. Go live once it's done; " +
                "it can take a few minutes on a slow network."
    }
}
