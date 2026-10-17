package at.vibing.ant.stream

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.io.Serializable
import java.util.UUID

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
    private var runs = 0L
    // Run numbers restart with the process; this tells a mark taken by an
    // earlier process (restored from saved state) from one taken by us.
    private val epoch = UUID.randomUUID().toString()
    private val _lastFailure = MutableStateFlow<Failure?>(null)

    /**
     * How an announcement failed: its [error], what it was announcing
     * ([subject], the caller's key: the account and start time), and
     * [seq], which run it was (counting up from 1).
     */
    data class Failure(val seq: Long, val error: String, val subject: Any?) {
        /** Whether this failure is about [subject], i.e. still true of what the screen shows. */
        fun isAbout(subject: Any?): Boolean = subject == this.subject
    }

    /**
     * The last finished announcement's failure, null once one succeeds or
     * while a new one is out. Recorded here, not by the caller, so a
     * failure still reaches the screen when the dialog that started the
     * write was closed (a rotation) before it returned. It is kept until
     * the next announcement, so a reader checks [Failure.isAbout] against
     * what it shows now: a failure for another start time or another
     * account is no longer news.
     */
    val lastFailure: StateFlow<Failure?> = _lastFailure.asStateFlow()

    /**
     * Where the next run will start. A screen saves it when it opens
     * (saveable, so it survives a rotation and a process death) and shows
     * only failures [since] it: an older one is not news there.
     */
    data class Mark(val epoch: String, val seq: Long) : Serializable

    /** A [Mark] for the next run: a screen opened now shows only failures from here on. */
    fun nextMark(): Mark = Mark(epoch, runs + 1)

    /**
     * Whether [failure] ran at or after [mark]. A mark from an earlier
     * process (the app was killed and restored with the screen open)
     * predates every run of this one, since run numbers restart at 1.
     */
    fun since(failure: Failure, mark: Mark): Boolean = mark.epoch != epoch || failure.seq >= mark.seq

    /**
     * Start [block] (the announcement; returns its error, or null) unless
     * one is already running, in which case that one is returned: a
     * reopened dialog awaits the write already out instead of starting a
     * second. Cancelling the caller's await never cancels the run.
     * [subject] is recorded with a failure (see [Failure.isAbout]).
     * Call from the main thread.
     */
    fun launch(subject: Any? = null, block: suspend () -> String?): Deferred<String?> {
        current?.takeIf { it.isActive }?.let { return it }
        _running.value = true
        _lastFailure.value = null
        val seq = ++runs
        val run = scope.async {
            try {
                block().also { err -> _lastFailure.value = err?.let { Failure(seq, it, subject) } }
            } finally {
                _running.value = false
            }
        }
        current = run
        return run
    }
}
