package at.vibing.ant.stream

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.ui.Modifier
import androidx.compose.material3.darkColorScheme
import androidx.lifecycle.lifecycleScope
import at.vibing.ant.stream.ui.RootScreen
import kotlinx.coroutines.launch

/**
 * Single-activity host. Starts the node on launch and, like the iOS app's
 * scene-phase handling, checkpoints it when the app leaves the screen
 * (`ant_suspend`) and re-dials on return (`ant_resume` + `ant_wake`).
 * A running broadcast instead keeps going in the background
 * ([BroadcastService]); only its preview is detached.
 */
class MainActivity : ComponentActivity() {
    private var stopped = false
    private var suspended = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val app = application as AntStreamApp
        intent.getStringExtra("antstream.selftest")?.let { app.broadcast.selfTestDir = java.io.File(cacheDir, it) }
        lifecycleScope.launch { app.node.start() }
        setContent {
            MaterialTheme(colorScheme = darkColorScheme()) {
                // A Surface supplies the content colour for screens that
                // sit outside the tab Scaffold (onboarding, live).
                Surface(
                    color = MaterialTheme.colorScheme.background,
                    modifier = Modifier.fillMaxSize().safeDrawingPadding(),
                ) {
                    RootScreen(node = app.node, broadcast = app.broadcast)
                }
            }
        }
    }

    override fun onStart() {
        super.onStart()
        val app = application as AntStreamApp
        app.broadcast.onScreenShown()
        if (stopped) {
            stopped = false
            // A broadcast kept the node awake; only a checkpointed node
            // needs waking.
            if (suspended) app.appScope.launch { app.node.wake() }
            suspended = false
        }
    }

    override fun onStop() {
        super.onStop()
        if (isChangingConfigurations) return
        val app = application as AntStreamApp
        stopped = true
        if (app.broadcast.isBusy) {
            // BroadcastService keeps capture and publishing going; only
            // the preview goes away with the screen.
            app.broadcast.onScreenHidden()
            return
        }
        suspended = true
        app.appScope.launch { app.node.suspendNode() }
    }
}
