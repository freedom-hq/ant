package at.vibing.ant.stream

/**
 * Keccak-256 (the original padding Ethereum and Swarm use, not SHA3-256),
 * and the feed addresses built on it. Plain Kotlin so the Watch tab and
 * its unit tests need no native library: the same functions the web
 * player's `keccak.js` and the publisher (`topic_from_string`,
 * `ladder_marker_identifier` in `crates/ant-ffi/src/publisher.rs`) use.
 */
object Keccak {
    private const val RATE = 136

    private val RC = listOf(
        0x0000000000000001uL, 0x0000000000008082uL, 0x800000000000808AuL, 0x8000000080008000uL,
        0x000000000000808BuL, 0x0000000080000001uL, 0x8000000080008081uL, 0x8000000000008009uL,
        0x000000000000008AuL, 0x0000000000000088uL, 0x0000000080008009uL, 0x000000008000000AuL,
        0x000000008000808BuL, 0x800000000000008BuL, 0x8000000000008089uL, 0x8000000000008003uL,
        0x8000000000008002uL, 0x8000000000000080uL, 0x000000000000800AuL, 0x800000008000000AuL,
        0x8000000080008081uL, 0x8000000000008080uL, 0x0000000080000001uL, 0x8000000080008008uL,
    ).map { it.toLong() }.toLongArray()
    private val ROT = intArrayOf(
        0, 1, 62, 28, 27, 36, 44, 6, 55, 20, 3, 10, 43, 25, 39, 41, 45, 15, 21, 8, 18, 2, 61, 56, 14,
    )

    fun keccak256(input: ByteArray): ByteArray {
        val s = LongArray(25)
        val padded = input.size / RATE * RATE + RATE
        val block = ByteArray(padded)
        input.copyInto(block)
        block[input.size] = (block[input.size].toInt() xor 0x01).toByte()
        block[padded - 1] = (block[padded - 1].toInt() xor 0x80).toByte()
        for (off in 0 until padded step RATE) {
            for (i in 0 until RATE / 8) {
                var lane = 0L
                for (b in 0 until 8) lane = lane or ((block[off + i * 8 + b].toLong() and 0xff) shl (8 * b))
                s[i] = s[i] xor lane
            }
            permute(s)
        }
        val out = ByteArray(32)
        for (i in 0 until 32) out[i] = (s[i / 8] ushr (8 * (i % 8))).toByte()
        return out
    }

    private fun permute(a: LongArray) {
        val c = LongArray(5)
        val b = LongArray(25)
        for (round in 0 until 24) {
            for (x in 0 until 5) c[x] = a[x] xor a[x + 5] xor a[x + 10] xor a[x + 15] xor a[x + 20]
            for (x in 0 until 5) {
                val d = c[(x + 4) % 5] xor java.lang.Long.rotateLeft(c[(x + 1) % 5], 1)
                for (y in 0 until 25 step 5) a[y + x] = a[y + x] xor d
            }
            for (x in 0 until 5) for (y in 0 until 5) {
                b[y + 5 * ((2 * x + 3 * y) % 5)] = java.lang.Long.rotateLeft(a[x + 5 * y], ROT[x + 5 * y])
            }
            for (y in 0 until 25 step 5) for (x in 0 until 5) {
                a[y + x] = b[y + x] xor (b[y + (x + 1) % 5].inv() and b[y + (x + 2) % 5])
            }
            a[0] = a[0] xor RC[round]
        }
    }

    fun hex(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it.toInt() and 0xff) }

    fun unhex(hex: String): ByteArray =
        ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

    private fun be8(n: Long): ByteArray = ByteArray(8) { (n ushr (8 * (7 - it))).toByte() }

    /** A feed topic (64 hex) from its text: bee-js `Topic.fromString`. */
    fun topicFromString(text: String): String = hex(keccak256(text.toByteArray(Charsets.UTF_8)))

    /** A sequence feed's slot identifier: `keccak256(topic ‖ index_be8)`. */
    fun feedIdentifier(topicHex: String, index: Long): String = hex(keccak256(unhex(topicHex) + be8(index)))

    /**
     * The ladder group topic the publisher writes a channel's time markers
     * for (`ladder_group_text`): `keccak256("antstream-ladder-<topic hex>")`.
     */
    fun ladderGroup(topicHex: String): String = topicFromString("antstream-ladder-$topicHex")

    /** A time marker's identifier: `keccak256("ladder-marker" ‖ group ‖ period_be8)`. */
    fun ladderMarkerIdentifier(groupHex: String, period: Long): String =
        hex(keccak256("ladder-marker".toByteArray() + unhex(groupHex) + be8(period)))
}
