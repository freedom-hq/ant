package at.vibing.ant.stream

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class AnnouncerTest {
    /** The dialog going away (its scope cancelled) leaves the write running and recorded. */
    @Test
    fun aCancelledCallerLeavesTheAnnouncementRunning() = runBlocking {
        val app = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val announcer = Announcer(app)
        val release = CompletableDeferred<Unit>()
        var recorded = false
        val dialog = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val caller = dialog.launch {
            announcer.launch {
                release.await()
                recorded = true
                null
            }.await()
        }
        while (!announcer.running.value) Thread.yield()
        caller.cancelAndJoin()
        // Still out, still holding Go live.
        assertTrue(announcer.running.value)
        // The dialog reopened: it awaits the write still out.
        val reopened = announcer.launch { "a second write" }
        release.complete(Unit)
        assertEquals(null, reopened.await())
        assertTrue(recorded)
        assertFalse(announcer.running.value)
    }

    /** A second tap (or a reopened dialog) joins the write already out. */
    @Test
    fun oneAnnouncementAtATime() = runBlocking {
        val announcer = Announcer(CoroutineScope(SupervisorJob() + Dispatchers.Default))
        val release = CompletableDeferred<Unit>()
        var runs = 0
        val first = announcer.launch { runs++; release.await(); "refused" }
        val second = announcer.launch { runs++; null }
        assertSame(first, second)
        release.complete(Unit)
        assertEquals("refused", second.await())
        assertEquals(1, runs)
        assertFalse(announcer.running.value)
        // Once done, the next tap writes again.
        assertEquals(null, announcer.launch { runs++; null }.await())
        assertEquals(2, runs)
    }

    /**
     * A failure is recorded on the announcer even when its caller went
     * away, so the card can still show it; the next run clears it.
     */
    @Test
    fun theLastErrorOutlivesACancelledCaller() = runBlocking {
        val announcer = Announcer(CoroutineScope(SupervisorJob() + Dispatchers.Default))
        val release = CompletableDeferred<Unit>()
        val dialog = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val caller = dialog.launch { announcer.launch { release.await(); "gateway error" }.await() }
        while (!announcer.running.value) Thread.yield()
        caller.cancelAndJoin()
        release.complete(Unit)
        while (announcer.running.value) Thread.yield()
        assertEquals("gateway error", announcer.lastError.value)
        // A new announcement clears it while it runs, and on success.
        val again = CompletableDeferred<Unit>()
        val next = announcer.launch { again.await(); null }
        assertEquals(null, announcer.lastError.value)
        again.complete(Unit)
        assertEquals(null, next.await())
        assertEquals(null, announcer.lastError.value)
    }
}
