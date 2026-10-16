package at.vibing.ant.stream.capture

/**
 * Keeps [CaptureEngine.switchCamera] from opening two cameras at once:
 * a switch tap that arrives while a camera open is still in flight is
 * latched instead of run, and applied once that camera reports opened.
 * Two taps in that window cancel out. Camera-thread only.
 */
internal class SwitchLatch {
    /** A camera open is in flight (no camera device yet). */
    var opening = false
        private set
    private var pending = false

    /** A camera open was requested. */
    fun startOpen() {
        opening = true
    }

    /** A switch was tapped: true to switch now, false if latched for [opened]. */
    fun tap(): Boolean {
        if (!opening) return true
        pending = !pending
        return false
    }

    /** The camera opened: true if a latched switch should run now. */
    fun opened(): Boolean {
        opening = false
        return pending.also { pending = false }
    }

    /** The open failed or the camera went away: drop any latched switch. */
    fun failed() {
        opening = false
        pending = false
    }
}
