package at.vibing.ant.stream

/**
 * One finished capture segment on its way to `ant_publisher_push_segment`:
 * either the fMP4 initialization segment ([isInit]) or a media segment.
 * [discontinuity] marks the first media segment after a writer restart.
 */
class Segment(val isInit: Boolean, val data: ByteArray, val durationMs: Int, val discontinuity: Boolean)
