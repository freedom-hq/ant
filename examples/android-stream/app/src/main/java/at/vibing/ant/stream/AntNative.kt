package at.vibing.ant.stream

/**
 * The JNI surface in `crates/ant-ffi/src/stream_jni.rs`. Each method is a
 * thin shim over the `ant.h` function of the same name, so the Kotlin
 * app runs exactly the code path the iOS AntStream app calls directly.
 *
 * Every call blocks for as long as the C function does (chain calls take
 * seconds, [nativePublisherStop] up to ~130 s, ~195 s worst case): call them off the main
 * thread. Failures throw [AntException].
 */
object AntNative {
    init {
        System.loadLibrary("ant_ffi")
    }

    @JvmStatic external fun nativeIdentityGenerate(): String
    @JvmStatic external fun nativeIdentityFromKey(key: String): String
    @JvmStatic external fun nativeInit(dataDir: String, identityJson: String): Long
    @JvmStatic external fun nativeShutdown(handle: Long)

    @JvmStatic external fun nativeStartGateway(handle: Long, addr: String, lightMode: Boolean, rpc: String)
    @JvmStatic external fun nativeStopGateway(handle: Long): Boolean
    @JvmStatic external fun nativePeerCount(handle: Long): Int
    @JvmStatic external fun nativeSuspend(handle: Long): Int
    @JvmStatic external fun nativeResume(handle: Long): Int
    @JvmStatic external fun nativeWake(handle: Long): Int

    @JvmStatic external fun nativeAccountInfo(handle: Long): String
    @JvmStatic external fun nativeAccountExportKey(handle: Long): String
    @JvmStatic external fun nativeStorageStatus(handle: Long): String
    @JvmStatic external fun nativeSettlementStatus(handle: Long): String
    @JvmStatic external fun nativeSwapStatus(handle: Long): String
    @JvmStatic external fun nativeStorageDiscover(handle: Long, rpc: String): String
    @JvmStatic external fun nativeStorageValidity(handle: Long, rpc: String): String
    @JvmStatic external fun nativeSettlementDeposit(handle: Long, rpc: String): String
    @JvmStatic external fun nativeSettlementTopup(handle: Long, rpc: String): String
    /** Spends real funds: moves `amountPlur` xBZZ from the wallet into the chequebook. */
    @JvmStatic external fun nativeSettlementTopupAmount(handle: Long, rpc: String, amountPlur: String): String
    /** Sends a real transaction: moves `amountPlur` xBZZ from the chequebook back to the wallet. */
    @JvmStatic external fun nativeSettlementWithdrawAmount(handle: Long, rpc: String, amountPlur: String): String
    @JvmStatic external fun nativeWalletBalance(handle: Long, rpc: String): String
    @JvmStatic external fun nativeStorageQuote(handle: Long, rpc: String, depth: Int, days: Long): String
    @JvmStatic external fun nativeStorageBuyXdai(
        handle: Long,
        rpc: String,
        depth: Int,
        amountPerChunk: String,
        immutable: Boolean,
    ): String

    @JvmStatic external fun nativePublisherStart(handle: Long, configJson: String)
    @JvmStatic external fun nativePublisherPushSegment(
        handle: Long,
        isInit: Boolean,
        data: ByteArray,
        durationMs: Int,
        discontinuity: Boolean,
    ): Int
    @JvmStatic external fun nativePublisherProgress(handle: Long): String
    @JvmStatic external fun nativePublisherStop(handle: Long): String
}
