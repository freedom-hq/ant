package at.vibing.ant.stream

import android.app.Application
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob

/** Holds the node and the broadcast so they survive activity recreation. */
class AntStreamApp : Application() {
    val appScope = CoroutineScope(SupervisorJob() + Dispatchers.Main)
    val node: AntNode by lazy { AntNode(this) }
    val broadcast: LiveBroadcast by lazy { LiveBroadcast(this, node) }
}
