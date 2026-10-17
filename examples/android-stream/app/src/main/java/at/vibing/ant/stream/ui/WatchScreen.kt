package at.vibing.ant.stream.ui

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
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
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
import androidx.media3.exoplayer.source.DefaultMediaSourceFactory
import androidx.media3.exoplayer.hls.playlist.HlsPlaylistTracker
import kotlinx.coroutines.delay
import androidx.media3.ui.PlayerView
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.platform.testTag
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.Catalog
import at.vibing.ant.stream.CatalogEntry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import at.vibing.ant.stream.ChannelAddress
import at.vibing.ant.stream.FeedDataSource
import at.vibing.ant.stream.FeedFollower

/**
 * Plays a solar-punk channel through the in-process gateway. The channel
 * is an owner and a topic: typed as `0x<owner>/<topic>`, Solar Punk's
 * `swarm://<owner>/<topic>`, or a web player link; it defaults to this
 * account's own channel. [FeedFollower] reads the feed (a ladder marker
 * or `GET /feeds/{owner}/{topic}` to join, then `/soc` slot reads) and
 * hands ExoPlayer the newest playlist with its bare references made
 * `<gateway>/bytes/<ref>`; ExoPlayer's HLS engine follows the live edge
 * on its own, and plays a recording (the feed's final update once a
 * broadcast ends) from the start.
 */
@OptIn(UnstableApi::class)
@Composable
fun WatchScreen(node: AntNode, initialChannel: String) {
    val context = LocalContext.current
    val own by node.channel.collectAsState()
    // An empty field means this account's own channel.
    val start = initialChannel.ifEmpty { own?.address?.display.orEmpty() }
    var input by rememberSaveable(start) { mutableStateOf(start) }
    var playing by rememberSaveable(initialChannel) { mutableStateOf(initialChannel.ifEmpty { null }) }
    var error by remember { mutableStateOf<String?>(null) }
    var buffering by remember { mutableStateOf(false) }
    val parsed = ChannelAddress.parse(input)

    Column(
        Modifier.fillMaxSize().padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("Watch", style = MaterialTheme.typography.headlineMedium)
        OutlinedTextField(
            value = input,
            onValueChange = { input = it.trim() },
            label = { Text("Channel (0x<owner>/<topic>, swarm:// or a player link)") },
            singleLine = true,
            isError = input.isNotEmpty() && parsed == null,
            modifier = Modifier.fillMaxWidth(),
        )
        if (input.isNotEmpty() && parsed == null) {
            Text(
                "Enter the broadcaster's address and topic: 0x<40 hex>/<topic>, swarm://<owner>/<topic>, " +
                    "or a player link.",
                color = Amber,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        Button(
            onClick = {
                error = null
                playing = parsed?.key
            },
            enabled = parsed != null,
            modifier = Modifier.fillMaxWidth(),
        ) { Text("Play") }

        playing?.let(ChannelAddress::parse)?.let { channel ->
            val ref = channel.key
            val follower = remember(ref) { FeedFollower(node.gatewayUrl, channel) }
            var retries by remember(ref) { mutableStateOf(0) }
            val player = remember(ref) {
                ExoPlayer.Builder(context)
                    .setMediaSourceFactory(DefaultMediaSourceFactory(FeedDataSource.Factory(follower)))
                    .build().apply {
                    setMediaItem(
                        MediaItem.Builder().setUri(follower.playlistUri).setMimeType(MimeTypes.APPLICATION_M3U8).build(),
                    )
                    addListener(
                        object : Player.Listener {
                            override fun onPlayerError(e: PlaybackException) {
                                // A live playlist that stops changing (a
                                // broadcast that ended without closing its
                                // playlist, or a feed not yet propagated) and
                                // a 404 on a channel that is not live yet both
                                // land here. Neither is final: reload the
                                // channel, which also picks up a new broadcast.
                                val stuck = generateSequence(e.cause) { it.cause }
                                    .any { it is HlsPlaylistTracker.PlaylistStuckException }
                                error = if (stuck) {
                                    "No new video from this channel. The broadcast may have ended " +
                                        "without closing; checking again…"
                                } else {
                                    val why = generateSequence<Throwable>(e) { it.cause }.mapNotNull { it.message }.lastOrNull()
                                    "Can't load the channel yet (${why ?: e.errorCodeName}); retrying…"
                                }
                                retries += 1
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
            LaunchedEffect(retries) {
                if (retries == 0) return@LaunchedEffect
                delay(8_000)
                player.prepare()
            }
            LaunchedEffect(player) {
                // Clear a stale retry message once video flows again.
                while (true) {
                    if (player.isPlaying) error = null
                    delay(1_000)
                }
            }
            // The channel's listing (title, description, a scheduled start)
            // from the owner's catalog feed: read beside playback, never
            // before it, and again every minute. A read that lands after
            // another channel took over is dropped; no catalog, or one
            // without an entry for this channel, shows nothing extra.
            var listing by remember(ref) { mutableStateOf<CatalogEntry?>(null) }
            val shown by rememberUpdatedState(playing)
            LaunchedEffect(ref) {
                while (true) {
                    val result = withContext(Dispatchers.IO) { runCatching { Catalog.fetch(node.gatewayUrl, channel) } }
                    // A failed read (the gateway busy) keeps what was shown.
                    if (shown?.let(ChannelAddress::parse)?.key == ref) result.onSuccess { listing = it }
                    delay(CATALOG_REFRESH_MS)
                }
            }
            listing?.let { entry ->
                if (entry.title.isNotEmpty()) {
                    Text(entry.title, style = MaterialTheme.typography.titleMedium, modifier = Modifier.testTag("watchTitle"))
                }
                entry.description?.let {
                    Text(it, color = Color.Gray, style = MaterialTheme.typography.bodySmall, modifier = Modifier.testTag("watchDescription"))
                }
                entry.startsAt()?.let { Text(it, color = Amber, modifier = Modifier.testTag("watchStartsAt")) }
            }
            AndroidView(
                factory = { PlayerView(it).apply { this.player = player } },
                // Another channel played is another player.
                update = { it.player = player },
                modifier = Modifier.fillMaxWidth().aspectRatio(16f / 9f),
            )
            if (buffering) Text("Buffering…", color = Color.Gray)
            error?.let { Text(it, color = Amber) }
            MonoText(follower.feedUrl)
        }
    }
}

/** How often the Watch tab re-reads the channel's catalog entry. */
private const val CATALOG_REFRESH_MS = 60_000L
