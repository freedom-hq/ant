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
 * (`ant_suspend`) and re-dials on return (`ant_resume` + `ant_wake`),
 * unless a broadcast is running.
 */
class MainActivity : ComponentActivity() {
    private var stopped = false

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
        if (stopped) {
            stopped = false
            val app = application as AntStreamApp
            app.appScope.launch { app.node.wake() }
        }
    }

    override fun onStop() {
        super.onStop()
        if (isChangingConfigurations) return
        val app = application as AntStreamApp
        stopped = true
        app.appScope.launch {
            if (app.broadcast.isBusy) {
                // The camera goes with the screen; end the broadcast
                // cleanly so the playlist is closed rather than left
                // dangling, and only then checkpoint the node.
                app.broadcast.stop(failure = "The broadcast ended when the app left the screen.")
            }
            app.node.suspendNode()
        }
    }
}
