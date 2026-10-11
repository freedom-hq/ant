package at.vibing.ant.stream.ui

import android.net.Uri
import androidx.annotation.OptIn
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.media3.common.MediaItem
import androidx.media3.common.MimeTypes
import androidx.media3.common.PlaybackException
import androidx.media3.common.Player
import androidx.media3.common.util.UnstableApi
import androidx.media3.exoplayer.ExoPlayer
import androidx.media3.ui.PlayerView
import at.vibing.ant.stream.AntNode

/**
 * Plays a channel through the in-process gateway: the channel reference
 * is a feed manifest, so `/bzz/<ref>/stream.m3u8` re-resolves the feed to
 * the newest playlist on every refresh and ExoPlayer's HLS engine follows
 * the live edge on its own.
 */
@OptIn(UnstableApi::class)
@Composable
fun WatchScreen(node: AntNode, initialReference: String) {
    val context = LocalContext.current
    var reference by rememberSaveable(initialReference) { mutableStateOf(initialReference) }
    var playing by rememberSaveable(initialReference) { mutableStateOf<String?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    var buffering by remember { mutableStateOf(false) }

    Column(
        Modifier.fillMaxSize().padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("Watch", style = MaterialTheme.typography.headlineMedium)
        OutlinedTextField(
            value = reference,
            onValueChange = { reference = it.trim() },
            label = { Text("Channel reference") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth(),
        )
        Button(
            onClick = {
                error = null
                playing = reference.removePrefix("bzz://").removePrefix("0x")
            },
            enabled = reference.length >= 64,
            modifier = Modifier.fillMaxWidth(),
        ) { Text("Play") }

        playing?.let { ref ->
            val url = "${node.gatewayUrl}/bzz/$ref/stream.m3u8"
            val player = remember(ref) {
                ExoPlayer.Builder(context).build().apply {
                    setMediaItem(
                        MediaItem.Builder().setUri(Uri.parse(url)).setMimeType(MimeTypes.APPLICATION_M3U8).build(),
                    )
                    addListener(
                        object : Player.Listener {
                            override fun onPlayerError(e: PlaybackException) {
                                error = "${e.errorCodeName}: ${e.message}"
                            }

                            override fun onPlaybackStateChanged(state: Int) {
                                buffering = state == Player.STATE_BUFFERING
                            }
                        },
                    )
                    playWhenReady = true
                    prepare()
                }
            }
            DisposableEffect(player) { onDispose { player.release() } }
            AndroidView(
                factory = { PlayerView(it).apply { this.player = player } },
                modifier = Modifier.fillMaxWidth().aspectRatio(16f / 9f),
            )
            if (buffering) Text("Buffering…", color = Color.Gray)
            error?.let { Text(it, color = Amber) }
            MonoText(url)
        }
    }
}
