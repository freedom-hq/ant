package at.vibing.ant.stream

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * One announcement at a time, run on a scope that outlives the screen
 * that asked for it (issue #220). `ant_stream_catalog_publish` blocks for
 * minutes on a slow network and keeps going once called, so the run that
 * records its result must not be the stream info dialog's: a rotation
 * would cancel that one while the native write lands anyway, leaving the
 * entry unrecorded. [running] holds Go live meanwhile, as the node itself
 * refuses a start while an announcement is out.
 */
internal class Announcer(private val scope: CoroutineScope) {
    private val _running = MutableStateFlow(false)
    val running: StateFlow<Boolean> = _running.asStateFlow()
    private var current: Deferred<String?>? = null
    private val _lastError = MutableStateFlow<String?>(null)

    /**
     * The last finished announcement's error, null once one succeeds or
     * while a new one is out. Recorded here, not by the caller, so a
     * failure still reaches the screen when the dialog that started the
     * write was closed (a rotation) before it returned.
     */
    val lastError: StateFlow<String?> = _lastError.asStateFlow()

    /**
     * Start [block] (the announcement; returns its error, or null) unless
     * one is already running, in which case that one is returned: a
     * reopened dialog awaits the write already out instead of starting a
     * second. Cancelling the caller's await never cancels the run.
     * Call from the main thread.
     */
    fun launch(block: suspend () -> String?): Deferred<String?> {
        current?.takeIf { it.isActive }?.let { return it }
        _running.value = true
        _lastError.value = null
        val run = scope.async {
            try {
                block().also { _lastError.value = it }
            } finally {
                _running.value = false
            }
        }
        current = run
        return run
    }
}
