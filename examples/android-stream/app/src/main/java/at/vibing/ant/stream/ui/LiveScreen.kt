package at.vibing.ant.stream.ui

import android.Manifest
import android.app.Activity
import android.content.pm.ActivityInfo
import android.content.pm.PackageManager
import android.graphics.Matrix
import android.graphics.RectF
import android.graphics.SurfaceTexture
import android.view.Surface
import android.view.TextureView
import android.view.WindowManager
import androidx.activity.compose.BackHandler
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.FilterChip
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.LiveBroadcast
import at.vibing.ant.stream.capture.CaptureEngine
import at.vibing.ant.stream.formatBytes
import at.vibing.ant.stream.formatBzz
import kotlinx.coroutines.launch

/**
 * The going-live screen: camera preview, publish-lag indicator and live
 * counters, mirroring `LiveView.swift`. Locked to landscape while open
 * (see `CaptureEngine`), with the screen held on.
 */
@Composable
fun LiveScreen(node: AntNode, broadcast: LiveBroadcast, onClose: () -> Unit, onWatch: (String) -> Unit) {
    val context = LocalContext.current
    val activity = context as Activity
    val scope = rememberCoroutineScope()
    val state by broadcast.state.collectAsState()
    val credit by node.credit.collectAsState()
    val progress by broadcast.progress.collectAsState()
    val report by broadcast.report.collectAsState()
    val captured by broadcast.captured.collectAsState()
    val quality by broadcast.quality.collectAsState()
    val front by broadcast.frontCamera.collectAsState()
    val portrait by broadcast.portrait.collectAsState()
    val announcing by broadcast.announcing.collectAsState()
    var channel by rememberSaveable { mutableStateOf(node.channel.value?.name ?: "My channel") }
    var texture by remember { mutableStateOf<SurfaceTexture?>(null) }
    var permissionsAsked by remember { mutableStateOf(false) }

    fun granted(p: String) = ContextCompat.checkSelfPermission(context, p) == PackageManager.PERMISSION_GRANTED
    var cameraOk by remember { mutableStateOf(granted(Manifest.permission.CAMERA)) }
    var micOk by remember { mutableStateOf(granted(Manifest.permission.RECORD_AUDIO)) }
    val launcher = rememberLauncherForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) {
        cameraOk = granted(Manifest.permission.CAMERA)
        micOk = granted(Manifest.permission.RECORD_AUDIO)
    }
    LaunchedEffect(Unit) {
        val needsNotify = android.os.Build.VERSION.SDK_INT >= 33 && !granted(Manifest.permission.POST_NOTIFICATIONS)
        if (!cameraOk || !micOk || needsNotify) {
            permissionsAsked = true
            // Notifications: the "Live on Swarm" notification of a broadcast
            // that keeps running in the background (Android 13+).
            val wanted = mutableListOf(Manifest.permission.CAMERA, Manifest.permission.RECORD_AUDIO)
            if (android.os.Build.VERSION.SDK_INT >= 33) wanted += Manifest.permission.POST_NOTIFICATIONS
            launcher.launch(wanted.toTypedArray())
        }
    }

    // The broadcast's orientation is fixed per broadcast, so the screen is
    // locked to it: the camera's frames are upright (or carry the rotation
    // that makes them so) only for the display rotation the engine assumed.
    // Start in the orientation the phone is held in: picking the phone up
    // upright means a vertical broadcast. The choice can still be changed
    // before going live, and is locked once live.
    LaunchedEffect(Unit) {
        if (!broadcast.isBusy) {
            val heldPortrait = activity.resources.configuration.orientation ==
                android.content.res.Configuration.ORIENTATION_PORTRAIT
            broadcast.setPortrait(heldPortrait)
        }
    }
    DisposableEffect(Unit) {
        val previous = activity.requestedOrientation
        activity.window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        onDispose {
            activity.window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            activity.requestedOrientation = previous
            broadcast.discardEngine()
        }
    }
    DisposableEffect(portrait) {
        activity.requestedOrientation = if (portrait) {
            ActivityInfo.SCREEN_ORIENTATION_PORTRAIT
        } else {
            ActivityInfo.SCREEN_ORIENTATION_LANDSCAPE
        }
        onDispose { }
    }

    val active = state is LiveBroadcast.State.Starting || state is LiveBroadcast.State.Live ||
        state is LiveBroadcast.State.Stopping
    BackHandler(enabled = !active) { onClose() }
    BackHandler(enabled = active) { scope.launch { broadcast.stop() } }

    // Preview, sized to the picture viewers get (the encoded frame, turned
    // upright in portrait).
    val previewPane = @Composable { paneModifier: Modifier ->
        Box(paneModifier, contentAlignment = Alignment.Center) {
            if (cameraOk) {
                // Re-prepared whenever a setting the engine is built with changes
                // (while live only the camera changes, inside the engine).
                val size = remember(quality, portrait, if (active) null else front) {
                    broadcast.prepareEngine(withAudio = micOk).videoSize
                }
                val sizeNow by rememberUpdatedState(size)
                val frontNow by rememberUpdatedState(front)
                AndroidView(
                    factory = { ctx ->
                        TextureView(ctx).apply {
                            surfaceTextureListener = object : TextureView.SurfaceTextureListener {
                                override fun onSurfaceTextureAvailable(st: SurfaceTexture, w: Int, h: Int) {
                                    setTransform(previewTransform(activity, w, h, sizeNow.width, sizeNow.height, frontNow))
                                    texture = st
                                    broadcast.previewTexture = st
                                    // Back on a broadcast that kept running.
                                    if (broadcast.isBusy) broadcast.setPreview(st)
                                }

                                override fun onSurfaceTextureSizeChanged(st: SurfaceTexture, w: Int, h: Int) {
                                    setTransform(previewTransform(activity, w, h, sizeNow.width, sizeNow.height, frontNow))
                                }

                                override fun onSurfaceTextureDestroyed(st: SurfaceTexture): Boolean {
                                    texture = null
                                    broadcast.previewTexture = null
                                    if (!broadcast.isBusy) return true
                                    // The broadcast goes on: detach the camera from
                                    // this texture first, then free it.
                                    broadcast.setPreview(null) { st.release() }
                                    return false
                                }

                                override fun onSurfaceTextureUpdated(st: SurfaceTexture) = Unit
                            }
                        }
                    },
                    update = { tv ->
                        // Re-fit after a quality change, and mirror the
                        // selfie camera like any camera app (viewers get the
                        // unmirrored picture).
                        if (tv.isAvailable) {
                            tv.setTransform(previewTransform(activity, tv.width, tv.height, size.width, size.height, front))
                        }
                    },
                    modifier = Modifier.aspectRatio(
                        if (portrait) size.height.toFloat() / size.width else size.width.toFloat() / size.height,
                    ),
                )
            } else {
                Text(
                    if (permissionsAsked) "AntStream needs the camera to broadcast." else "",
                    color = Color.Gray,
                )
            }
            if (state is LiveBroadcast.State.Live) {
                Text(
                    "● LIVE",
                    color = Color.White,
                    fontWeight = FontWeight.Bold,
                    modifier = Modifier
                        .align(Alignment.TopStart)
                        .padding(12.dp)
                        .background(Red, RoundedCornerShape(8.dp))
                        .padding(horizontal = 10.dp, vertical = 4.dp),
                )
            }
        }

    }

    // Controls and counters.
    val controlsPane = @Composable { paneModifier: Modifier ->
        Column(
            paneModifier
                .verticalScroll(rememberScrollState())
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            when (val s = state) {
                LiveBroadcast.State.Idle, is LiveBroadcast.State.Failed -> {
                    if (s is LiveBroadcast.State.Failed) Text(s.message, color = Amber)
                    if (announcing) Text(LiveBroadcast.ANNOUNCING_HOLD_NOTE, color = Amber)
                    Button(
                        onClick = {
                            val st = texture ?: return@Button
                            scope.launch { broadcast.start(channel.ifBlank { "My channel" }, st, micOk) }
                        },
                        enabled = cameraOk && texture != null && !announcing &&
                            (node.isReadyToBroadcast || broadcast.selfTestDir != null),
                        colors = ButtonDefaults.buttonColors(containerColor = Red),
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text("Go live") }
                    if (!micOk && cameraOk) Text("No microphone access: video only.", color = Color.Gray)
                    ChoiceRow(
                        listOf("Portrait" to true, "Landscape" to false),
                        selected = portrait,
                        onSelect = { broadcast.setPortrait(it) },
                    )
                    ChoiceRow(
                        listOf("Back camera" to false, "Selfie" to true),
                        selected = front,
                        onSelect = { if (it != front) broadcast.switchCamera() },
                    )
                    ChoiceRow(
                        CaptureEngine.Quality.entries.map { it.label to it },
                        selected = quality,
                        onSelect = { broadcast.setQuality(it) },
                    )
                    Text(
                        "${quality.label} uses about ${"%.1f".format(quality.bitrateKbps / 1000.0)} Mbit/s " +
                            "(${quality.bitrateKbps * 450 / 1000} MB per hour). Lower is more reliable on mobile data.",
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                    ChannelField(channel) { channel = it }
                    StreamInfoCard(node, broadcast, channel.ifBlank { "My channel" })
                    OutlinedButton(onClick = onClose, modifier = Modifier.fillMaxWidth()) { Text("Close") }
                }
                LiveBroadcast.State.Starting -> Text("Starting the broadcast…")
                LiveBroadcast.State.Live -> {
                    // Fixed for the broadcast: the engine and orientation are.
                    val canSwitch = remember { broadcast.canSwitchLive }
                    // A one-camera device (front-only tablet, Chromebook) has
                    // nothing to switch to, so it gets no note at all.
                    val hasBoth = remember { broadcast.hasBothCamerasLive }
                    if (canSwitch) {
                        OutlinedButton(onClick = { broadcast.switchCamera() }, modifier = Modifier.fillMaxWidth()) {
                            Text(if (front) "Switch to back camera" else "Switch to selfie camera")
                        }
                    } else if (hasBoth) {
                        Text(
                            "This phone's front and back cameras are mounted differently, so they can't be " +
                                "switched mid-broadcast in ${if (portrait) "portrait" else "landscape"}. " +
                                "Pick the camera before going live.",
                            color = Color.Gray,
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                    Button(
                        onClick = { scope.launch { broadcast.stop() } },
                        colors = ButtonDefaults.buttonColors(containerColor = Red),
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text("Stop") }
                    StreamInfoCard(node, broadcast, channel.ifBlank { "My channel" })
                }
                LiveBroadcast.State.Stopping -> Text("Publishing the last segments…")
            }

            progress?.takeIf { active }?.let { p ->
                Text(
                    p.lagLabel,
                    style = MaterialTheme.typography.titleLarge,
                    color = when {
                        p.playlistsPublished <= 0 -> Color.Gray
                        p.keepingUp -> Green
                        else -> Amber
                    },
                )
                Stat("Captured", "$captured segments")
                Stat("Published", "${p.segmentsPublished} · ${formatBytes(p.bytesPublished)}")
                Stat("Dropped / failed", "${p.segmentsDropped} / ${p.segmentsFailed}")
                Stat("Uplink", String.format("%.2f Mbit/s", p.sustainedMbitS))
                Stat("Peers", "${p.peers}")
                creditSummary(credit)?.let { Stat("Credit", it) }
                credit?.takeIf { it.swap.issuedPlur != null }?.let { Stat("Paid this session", formatBzz(it.sessionPlur)) }
                if (p.lastError.isNotEmpty()) Text(p.lastError, color = Amber, style = MaterialTheme.typography.bodySmall)
                p.channel?.let { c ->
                    Text("Channel", color = Color.Gray)
                    MonoText(c.display)
                    OutlinedButton(
                        onClick = { copyToClipboard(context, "Player link", c.playerLink) },
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text("Copy player link") }
                    OutlinedButton(
                        onClick = { shareText(context, c.watchLinks()) },
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text("Share") }
                }
            }

            report?.takeIf { !active }?.let { r ->
                Text("Broadcast finished", style = MaterialTheme.typography.titleMedium)
                Stat("Duration", String.format("%.0f s", r.durationS))
                Stat("Segments", "${r.segmentsListed} listed, ${r.segmentsDropped} dropped")
                Stat("Published", formatBytes(r.bytesPublished))
                Stat("Lag p50 / max", "${r.lagMsP50 / 1000.0} s / ${r.lagMsMax / 1000.0} s")
                Stat("Kept up", if (r.keptUp) "yes" else "no")
                r.channel?.let { c ->
                    MonoText(c.display)
                    OutlinedButton(
                        onClick = { onWatch(c.key) },
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text("Watch it") }
                }
            }
        }
    }

    if (portrait) {
        Column(Modifier.fillMaxSize().background(Color.Black)) {
            previewPane(Modifier.fillMaxWidth().weight(1.3f))
            controlsPane(Modifier.fillMaxWidth().weight(1f))
        }
    } else {
        Row(Modifier.fillMaxSize().background(Color.Black)) {
            previewPane(Modifier.fillMaxHeight().weight(1f))
            controlsPane(Modifier.width(300.dp).fillMaxHeight())
        }
    }
}

/** One-tap choice between a few options, as a row of chips. */
@Composable
private fun <T> ChoiceRow(options: List<Pair<String, T>>, selected: T, onSelect: (T) -> Unit) {
    Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
        options.forEach { (label, value) ->
            FilterChip(
                selected = selected == value,
                onClick = { onSelect(value) },
                label = { Text(label) },
            )
        }
    }
}

@Composable
private fun Stat(label: String, value: String) {
    Row(Modifier.fillMaxWidth()) {
        Text(label, color = Color.Gray, modifier = Modifier.weight(1f))
        Text(value)
    }
}

/**
 * Map the camera buffer onto the TextureView in a landscape activity
 * (the Camera2Basic `configureTransform` recipe): rotate the buffer by
 * the display rotation and scale it to fill the view.
 */
private fun previewTransform(activity: Activity, viewW: Int, viewH: Int, bufW: Int, bufH: Int, mirror: Boolean): Matrix {
    @Suppress("DEPRECATION")
    val rotation = activity.windowManager.defaultDisplay.rotation
    val matrix = Matrix()
    val viewRect = RectF(0f, 0f, viewW.toFloat(), viewH.toFloat())
    val bufferRect = RectF(0f, 0f, bufH.toFloat(), bufW.toFloat())
    val cx = viewRect.centerX()
    val cy = viewRect.centerY()
    if (rotation == Surface.ROTATION_90 || rotation == Surface.ROTATION_270) {
        bufferRect.offset(cx - bufferRect.centerX(), cy - bufferRect.centerY())
        matrix.setRectToRect(viewRect, bufferRect, Matrix.ScaleToFit.FILL)
        val scale = maxOf(viewH.toFloat() / bufH, viewW.toFloat() / bufW)
        matrix.postScale(scale, scale, cx, cy)
        matrix.postRotate(90f * (rotation - 2), cx, cy)
    } else if (rotation == Surface.ROTATION_180) {
        matrix.postRotate(180f, cx, cy)
    }
    if (mirror) matrix.postScale(-1f, 1f, cx, cy)
    return matrix
}
