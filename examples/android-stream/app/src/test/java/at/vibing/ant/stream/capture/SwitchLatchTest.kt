package at.vibing.ant.stream.capture

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SwitchLatchTest {
    @Test
    fun tapWithNoOpenInFlightSwitchesNow() {
        val latch = SwitchLatch()
        assertTrue(latch.tap())
    }

    @Test
    fun tapWhileOpeningIsLatchedUntilOpened() {
        val latch = SwitchLatch()
        latch.startOpen()
        // The double tap from the review: the second tap must not open a
        // second camera while the first is still opening.
        assertFalse(latch.tap())
        assertTrue(latch.opening)
        assertTrue(latch.opened())
        assertFalse(latch.opening)
        // Applied once only.
        latch.startOpen()
        assertFalse(latch.opened())
    }

    @Test
    fun twoTapsWhileOpeningCancelOut() {
        val latch = SwitchLatch()
        latch.startOpen()
        assertFalse(latch.tap())
        assertFalse(latch.tap())
        assertFalse(latch.opened())
    }

    @Test
    fun failedOpenDropsTheLatchedTap() {
        val latch = SwitchLatch()
        latch.startOpen()
        assertFalse(latch.tap())
        latch.failed()
        assertFalse(latch.opening)
        assertTrue(latch.tap())
        latch.startOpen()
        assertFalse(latch.opened())
    }
}
