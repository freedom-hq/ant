package at.vibing.ant.stream.ui

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.LiveBroadcast

private enum class Tab(val label: String) { Broadcast("Broadcast"), Watch("Watch"), Storage("Storage") }

@Composable
fun RootScreen(node: AntNode, broadcast: LiveBroadcast) {
    var tab by rememberSaveable { mutableStateOf(Tab.Broadcast) }
    var live by rememberSaveable { mutableStateOf(false) }
    var gettingStarted by rememberSaveable { mutableStateOf(false) }
    var watchRef by rememberSaveable { mutableStateOf("") }
    val state by broadcast.state.collectAsState()

    if (live || state is LiveBroadcast.State.Starting || state is LiveBroadcast.State.Live ||
        state is LiveBroadcast.State.Stopping
    ) {
        LiveScreen(
            node = node,
            broadcast = broadcast,
            onClose = { live = false },
            onWatch = { ref ->
                watchRef = ref
                live = false
                tab = Tab.Watch
            },
        )
        return
    }
    if (gettingStarted) {
        GetStartedScreen(node = node, onClose = { gettingStarted = false })
        return
    }

    Scaffold(
        bottomBar = {
            NavigationBar {
                Tab.entries.forEach { t ->
                    NavigationBarItem(
                        selected = tab == t,
                        onClick = { tab = t },
                        icon = {},
                        label = { Text(t.label) },
                    )
                }
            }
        },
    ) { padding ->
        Box(Modifier.fillMaxSize().padding(padding)) {
            when (tab) {
                Tab.Broadcast -> BroadcastScreen(
                    node = node,
                    broadcast = broadcast,
                    onGetStarted = { gettingStarted = true },
                    onGoLive = { live = true },
                    onWatch = { ref ->
                        watchRef = ref
                        tab = Tab.Watch
                    },
                )
                Tab.Watch -> WatchScreen(node = node, initialChannel = watchRef)
                Tab.Storage -> StorageScreen(node = node, onGetStarted = { gettingStarted = true })
            }
        }
    }
}
