package at.vibing.ant.stream.capture

import android.graphics.Bitmap
import android.graphics.SurfaceTexture
import android.opengl.EGL14
import android.opengl.EGLConfig
import android.opengl.EGLContext
import android.opengl.EGLDisplay
import android.opengl.EGLExt
import android.opengl.EGLSurface
import android.opengl.GLES11Ext
import android.opengl.GLES20
import android.os.Handler
import android.os.HandlerThread
import android.util.Log
import android.view.Surface
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.FloatBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Turns the camera's frames upright on their way into the video encoder
 * (#194), so a portrait broadcast is encoded as real portrait frames
 * (480×640, not 640×480 with a rotation flag in the track header).
 * Players treat that flag differently: some ignore it (a sideways
 * picture), some apply it inside the unrotated box (a squeezed one).
 * Upright frames with an identity matrix play the same everywhere.
 *
 * Camera2 never rotates what it renders into an encoder surface: the
 * buffers arrive in the sensor's orientation. So the camera renders into
 * [inputSurface] (a [SurfaceTexture]) instead, and every frame is drawn
 * from there into the encoder's input surface on a GL thread of its own,
 * turned clockwise by [rotation] degrees, with the camera's timestamp.
 *
 * The texture is sampled through [texCoords], which addresses the
 * buffer's memory directly (row 0 at t = 0), not through the
 * SurfaceTexture's transform matrix as a whole: that matrix carries the
 * framework's own preview rotation and selfie mirroring, which the encoder
 * path has never had and must not get — [rotation] is the complete turn,
 * worked out from the sensor orientation as before. Only the matrix's crop
 * is kept ([cropOf]): a HAL may allocate a taller, aligned buffer (368
 * rows for 360) and mark the picture with a crop rect, and the matrix also
 * insets the crop by half a texel so linear filtering never reads the
 * padding.
 */
internal class UprightRenderer(
    encoderSurface: Surface,
    cameraWidth: Int,
    cameraHeight: Int,
    private val outWidth: Int,
    private val outHeight: Int,
    rotation: Int,
) {
    /** Clockwise turn applied to each camera frame; changes on a camera switch ([resume]). */
    @Volatile var rotation: Int = rotation
        private set

    /** Frames are dropped while set: a camera switch is in progress ([hold]). */
    @Volatile private var held = false

    private val thread = HandlerThread("antstream.gl").apply { start() }
    private val handler = Handler(thread.looper)

    private var display: EGLDisplay = EGL14.EGL_NO_DISPLAY
    private var context: EGLContext = EGL14.EGL_NO_CONTEXT
    private var surface: EGLSurface = EGL14.EGL_NO_SURFACE
    private var program = 0
    private var texture = 0
    private var posLoc = 0
    private var texLoc = 0
    private lateinit var surfaceTexture: SurfaceTexture
    private val positions = floatBuffer(floatArrayOf(-1f, -1f, 1f, -1f, -1f, 1f, 1f, 1f))
    private var drawnRotation = Int.MIN_VALUE
    private val coords = floatBuffer(FloatArray(8))
    private val matrix = FloatArray(16)
    private var drawnCrop = FloatArray(0)
    private var latchedTs = Long.MIN_VALUE
    private var released = false

    /** Where the camera renders. Valid until [release]. */
    lateinit var inputSurface: Surface
        private set

    init {
        var error: Throwable? = null
        val ready = CountDownLatch(1)
        handler.post {
            try {
                setUp(encoderSurface, cameraWidth, cameraHeight)
            } catch (e: Throwable) {
                error = e
            }
            ready.countDown()
        }
        if (!ready.await(5, TimeUnit.SECONDS)) error = IllegalStateException("GL setup timed out")
        error?.let {
            release()
            throw IllegalStateException("upright renderer: ${it.message}", it)
        }
    }

    private fun setUp(encoderSurface: Surface, cameraWidth: Int, cameraHeight: Int) {
        display = EGL14.eglGetDisplay(EGL14.EGL_DEFAULT_DISPLAY)
        val version = IntArray(2)
        check(EGL14.eglInitialize(display, version, 0, version, 1)) { "eglInitialize" }
        val attribs = intArrayOf(
            EGL14.EGL_RED_SIZE, 8,
            EGL14.EGL_GREEN_SIZE, 8,
            EGL14.EGL_BLUE_SIZE, 8,
            EGL14.EGL_RENDERABLE_TYPE, EGL14.EGL_OPENGL_ES2_BIT,
            // The encoder's surface only takes buffers from a recordable config.
            EGLExt.EGL_RECORDABLE_ANDROID, 1,
            EGL14.EGL_NONE,
        )
        val configs = arrayOfNulls<EGLConfig>(1)
        val count = IntArray(1)
        check(EGL14.eglChooseConfig(display, attribs, 0, configs, 0, 1, count, 0) && count[0] > 0) { "no recordable EGL config" }
        context = EGL14.eglCreateContext(
            display, configs[0], EGL14.EGL_NO_CONTEXT,
            intArrayOf(EGL14.EGL_CONTEXT_CLIENT_VERSION, 2, EGL14.EGL_NONE), 0,
        )
        check(context != EGL14.EGL_NO_CONTEXT) { "eglCreateContext" }
        surface = EGL14.eglCreateWindowSurface(display, configs[0], encoderSurface, intArrayOf(EGL14.EGL_NONE), 0)
        check(surface != EGL14.EGL_NO_SURFACE) { "eglCreateWindowSurface" }
        check(EGL14.eglMakeCurrent(display, surface, surface, context)) { "eglMakeCurrent" }

        program = link(VERTEX_SHADER, FRAGMENT_SHADER)
        posLoc = GLES20.glGetAttribLocation(program, "aPos")
        texLoc = GLES20.glGetAttribLocation(program, "aTex")
        val tex = IntArray(1)
        GLES20.glGenTextures(1, tex, 0)
        texture = tex[0]
        GLES20.glBindTexture(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, texture)
        GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, GLES20.GL_TEXTURE_MIN_FILTER, GLES20.GL_LINEAR)
        GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, GLES20.GL_TEXTURE_MAG_FILTER, GLES20.GL_LINEAR)
        GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, GLES20.GL_TEXTURE_WRAP_S, GLES20.GL_CLAMP_TO_EDGE)
        GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, GLES20.GL_TEXTURE_WRAP_T, GLES20.GL_CLAMP_TO_EDGE)

        surfaceTexture = SurfaceTexture(texture).apply {
            setDefaultBufferSize(cameraWidth, cameraHeight)
            setOnFrameAvailableListener({ draw() }, handler)
        }
        inputSurface = Surface(surfaceTexture)
    }

    private fun draw() {
        if (released) return
        try {
            // Latch even while held, so the old camera's buffers are
            // consumed (and dropped) rather than drawn later.
            surfaceTexture.updateTexImage()
            val ts = surfaceTexture.timestamp
            // Nothing new latched (a callback whose buffer an earlier draw
            // already took, or the old camera's last frame still in the
            // texture after a switch): don't draw it again.
            val fresh = ts != latchedTs || ts == 0L
            latchedTs = ts
            if (held || !fresh) return
            surfaceTexture.getTransformMatrix(matrix)
            val crop = cropOf(matrix)
            val r = rotation
            if (r != drawnRotation || !crop.contentEquals(drawnCrop)) {
                coords.clear()
                coords.put(texCoords(r, crop)).position(0)
                drawnRotation = r
                drawnCrop = crop
            }
            GLES20.glViewport(0, 0, outWidth, outHeight)
            GLES20.glUseProgram(program)
            GLES20.glActiveTexture(GLES20.GL_TEXTURE0)
            GLES20.glBindTexture(GLES11Ext.GL_TEXTURE_EXTERNAL_OES, texture)
            GLES20.glEnableVertexAttribArray(posLoc)
            GLES20.glVertexAttribPointer(posLoc, 2, GLES20.GL_FLOAT, false, 0, positions)
            GLES20.glEnableVertexAttribArray(texLoc)
            GLES20.glVertexAttribPointer(texLoc, 2, GLES20.GL_FLOAT, false, 0, coords)
            GLES20.glDrawArrays(GLES20.GL_TRIANGLE_STRIP, 0, 4)
            snapshotRequest?.let { request ->
                snapshotRequest = null
                request(readFrame())
            }
            // The encoder stamps the frame with this: the camera's own
            // clock, exactly as when the camera fed the encoder directly.
            EGLExt.eglPresentationTimeANDROID(display, surface, surfaceTexture.timestamp)
            EGL14.eglSwapBuffers(display, surface)
        } catch (e: RuntimeException) {
            // A frame racing teardown; the next one (if any) tries again.
            Log.w(TAG, "frame dropped: ${e.message}")
        }
    }

    /** Who asked for the next drawn frame ([snapshot]); touched on the GL thread only. */
    private var snapshotRequest: ((Bitmap?) -> Unit)? = null

    /**
     * Hand [onFrame] a copy of the next frame drawn into the encoder,
     * upright as viewers get it (the stream thumbnail, issue #220). Read
     * back from the GL surface right after the draw, so the camera and the
     * encoder are untouched. Runs [onFrame] on the GL thread; with `null`
     * when the read fails, never when no frame comes (the caller times out).
     */
    fun snapshot(onFrame: (Bitmap?) -> Unit) {
        handler.post {
            if (released) onFrame(null) else snapshotRequest = onFrame
        }
    }

    /** The frame just drawn, as a bitmap; GL's rows run bottom-up, so flipped. */
    private fun readFrame(): Bitmap? = runCatching {
        val pixels = ByteBuffer.allocateDirect(outWidth * outHeight * 4).order(ByteOrder.nativeOrder())
        GLES20.glReadPixels(0, 0, outWidth, outHeight, GLES20.GL_RGBA, GLES20.GL_UNSIGNED_BYTE, pixels)
        pixels.rewind()
        val raw = Bitmap.createBitmap(outWidth, outHeight, Bitmap.Config.ARGB_8888)
        raw.copyPixelsFromBuffer(pixels)
        val flip = android.graphics.Matrix().apply { preScale(1f, -1f) }
        Bitmap.createBitmap(raw, 0, 0, outWidth, outHeight, flip, false).also { if (it !== raw) raw.recycle() }
    }.onFailure { Log.w(TAG, "snapshot: ${it.message}") }.getOrNull()

    /**
     * Drop every frame from now on, until [resume]. Takes effect at once,
     * also for frames already queued on the GL thread.
     */
    fun hold() {
        held = true
    }

    /**
     * Draw again, turning frames by [rotation] from now on. Call once the
     * camera that was rendering has closed (so nothing it rendered can be
     * latched any more); runs on the GL thread after every frame callback
     * already queued there, which are still dropped.
     */
    fun resume(rotation: Int) {
        handler.post {
            this.rotation = rotation
            held = false
        }
    }

    /**
     * Stop drawing and free the GL state. Blocks (briefly) until the GL
     * thread is done, so the encoder's input surface can be released
     * right after without a frame still being drawn into it.
     */
    fun release() {
        val done = CountDownLatch(1)
        val posted = handler.post {
            released = true
            snapshotRequest?.invoke(null)
            snapshotRequest = null
            if (::surfaceTexture.isInitialized) {
                surfaceTexture.setOnFrameAvailableListener(null)
                runCatching { inputSurface.release() }
                runCatching { surfaceTexture.release() }
            }
            if (display != EGL14.EGL_NO_DISPLAY) {
                if (program != 0) GLES20.glDeleteProgram(program)
                if (texture != 0) GLES20.glDeleteTextures(1, intArrayOf(texture), 0)
                EGL14.eglMakeCurrent(display, EGL14.EGL_NO_SURFACE, EGL14.EGL_NO_SURFACE, EGL14.EGL_NO_CONTEXT)
                if (surface != EGL14.EGL_NO_SURFACE) EGL14.eglDestroySurface(display, surface)
                if (context != EGL14.EGL_NO_CONTEXT) EGL14.eglDestroyContext(display, context)
                EGL14.eglReleaseThread()
                EGL14.eglTerminate(display)
            }
            display = EGL14.EGL_NO_DISPLAY
            context = EGL14.EGL_NO_CONTEXT
            surface = EGL14.EGL_NO_SURFACE
            done.countDown()
        }
        if (posted) done.await(2, TimeUnit.SECONDS)
        thread.quitSafely()
    }

    private fun link(vertex: String, fragment: String): Int {
        fun compile(type: Int, source: String): Int {
            val shader = GLES20.glCreateShader(type)
            GLES20.glShaderSource(shader, source)
            GLES20.glCompileShader(shader)
            val ok = IntArray(1)
            GLES20.glGetShaderiv(shader, GLES20.GL_COMPILE_STATUS, ok, 0)
            check(ok[0] != 0) { "shader: ${GLES20.glGetShaderInfoLog(shader)}" }
            return shader
        }
        val p = GLES20.glCreateProgram()
        GLES20.glAttachShader(p, compile(GLES20.GL_VERTEX_SHADER, vertex))
        GLES20.glAttachShader(p, compile(GLES20.GL_FRAGMENT_SHADER, fragment))
        GLES20.glLinkProgram(p)
        val ok = IntArray(1)
        GLES20.glGetProgramiv(p, GLES20.GL_LINK_STATUS, ok, 0)
        check(ok[0] != 0) { "program: ${GLES20.glGetProgramInfoLog(p)}" }
        return p
    }

    companion object {
        private const val TAG = "UprightRenderer"

        private const val VERTEX_SHADER = """
            attribute vec4 aPos;
            attribute vec2 aTex;
            varying vec2 vTex;
            void main() {
                gl_Position = aPos;
                vTex = aTex;
            }
        """

        private const val FRAGMENT_SHADER = """
            #extension GL_OES_EGL_image_external : require
            precision mediump float;
            varying vec2 vTex;
            uniform samplerExternalOES uTex;
            void main() {
                gl_FragColor = texture2D(uTex, vTex);
            }
        """

        /**
         * Texture coordinates for the output quad's corners (bottom-left,
         * bottom-right, top-left, top-right, as GL draws them) that turn
         * the camera buffer clockwise by [rotation] degrees.
         *
         * Coordinates address the buffer as stored: (0, 0) is its first
         * pixel, the top-left of the frame as the sensor delivers it, and
         * t grows downwards. The output's top-left corner shows the
         * source pixel the turn carries there: for 90° that is the
         * source's bottom-left, for 180° its bottom-right, for 270° its
         * top-right.
         *
         * [crop] is the part of the buffer that holds the picture, as
         * (s0, t0, s1, t1) in the same coordinates ([cropOf]); the turned
         * coordinates are mapped into it.
         */
        fun texCoords(rotation: Int, crop: FloatArray = FULL): FloatArray {
            // Output corners in frame coordinates (x right, y down).
            val corners = arrayOf(0f to 1f, 1f to 1f, 0f to 0f, 1f to 0f)
            val r = (rotation % 360 + 360) % 360
            val out = FloatArray(8)
            corners.forEachIndexed { i, (x, y) ->
                val (s, t) = when (r) {
                    90 -> y to 1f - x
                    180 -> 1f - x to 1f - y
                    270 -> 1f - y to x
                    else -> x to y
                }
                out[2 * i] = crop[0] + s * (crop[2] - crop[0])
                out[2 * i + 1] = crop[1] + t * (crop[3] - crop[1])
            }
            return out
        }

        /** The whole buffer, (s0, t0, s1, t1). */
        val FULL = floatArrayOf(0f, 0f, 1f, 1f)

        /**
         * The picture's rectangle in the buffer, (s0, t0, s1, t1) with t = 0
         * at the buffer's first row, taken from a SurfaceTexture transform
         * matrix (column-major, as [SurfaceTexture.getTransformMatrix]
         * fills it). The matrix is a crop (with its half-texel inset)
         * composed with rotations and flips; those only permute the unit
         * square's corners, so the corners' bounding box is the crop alone.
         */
        fun cropOf(m: FloatArray): FloatArray {
            var s0 = Float.MAX_VALUE
            var t0 = Float.MAX_VALUE
            var s1 = -Float.MAX_VALUE
            var t1 = -Float.MAX_VALUE
            for ((x, y) in arrayOf(0f to 0f, 1f to 0f, 0f to 1f, 1f to 1f)) {
                val s = m[0] * x + m[4] * y + m[12]
                val t = m[1] * x + m[5] * y + m[13]
                s0 = minOf(s0, s); s1 = maxOf(s1, s)
                t0 = minOf(t0, t); t1 = maxOf(t1, t)
            }
            // A matrix that isn't one (all zero before the first frame)
            // or strays outside the buffer: sample the whole buffer.
            if (!(s1 - s0 > 0f && t1 - t0 > 0f) || s0 < -EPS || t0 < -EPS || s1 > 1f + EPS || t1 > 1f + EPS) {
                return FULL
            }
            return floatArrayOf(s0, t0, s1, t1)
        }

        private const val EPS = 1e-4f

        private fun floatBuffer(values: FloatArray): FloatBuffer =
            ByteBuffer.allocateDirect(values.size * 4).order(ByteOrder.nativeOrder()).asFloatBuffer().apply {
                put(values)
                position(0)
            }
    }
}
