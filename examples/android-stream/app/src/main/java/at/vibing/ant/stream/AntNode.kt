package at.vibing.ant.stream

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.util.Log
import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.json.JSONObject

/**
 * The embedded light node, mirroring `AntNode.swift` in the iOS app.
 *
 * `start()` resolves the data dir, seeds the peer store, loads the
 * account identity from [AccountKeystore], starts the node with
 * `ant_init_with_identity` and the in-process gateway in light mode on
 * 127.0.0.1:1633 — the surface the publisher and the player both use.
 *
 * Transitions (start / shutdown / restore) run one at a time under
 * [lifecycle]. Ordinary calls borrow the handle under the read side of
 * [handleLock]; shutdown takes the write side, so it waits for every
 * call still inside the node before `ant_shutdown` frees the handle.
 */
class AntNode(private val context: Context) {
    sealed interface Status {
        data object Idle : Status
        data object Starting : Status
        data object Ready : Status
        data class Failed(val message: String) : Status
    }

    private val _status = MutableStateFlow<Status>(Status.Idle)
    val status: StateFlow<Status> = _status.asStateFlow()
    private val _peerCount = MutableStateFlow(0)
    val peerCount: StateFlow<Int> = _peerCount.asStateFlow()
    private val _plan = MutableStateFlow<StoragePlan?>(null)
    val plan: StateFlow<StoragePlan?> = _plan.asStateFlow()
    private val _account = MutableStateFlow<AccountInfo?>(null)
    val account: StateFlow<AccountInfo?> = _account.asStateFlow()
    private val _settlement = MutableStateFlow<SettlementInfo?>(null)
    val settlement: StateFlow<SettlementInfo?> = _settlement.asStateFlow()
    private val _validity = MutableStateFlow<StorageValidity?>(null)
    val validity: StateFlow<StorageValidity?> = _validity.asStateFlow()
    private val _gatewayUp = MutableStateFlow(false)
    val gatewayUp: StateFlow<Boolean> = _gatewayUp.asStateFlow()
    private val _gatewayError = MutableStateFlow<String?>(null)
    val gatewayError: StateFlow<String?> = _gatewayError.asStateFlow()
    private val _gatewayAddress = MutableStateFlow(GATEWAY_ADDRESS)
    val gatewayAddress: StateFlow<String> = _gatewayAddress.asStateFlow()
    private val _offline = MutableStateFlow(false)
    val offline: StateFlow<Boolean> = _offline.asStateFlow()
    private val _networkLabel = MutableStateFlow("Unknown")
    val networkLabel: StateFlow<String> = _networkLabel.asStateFlow()
    private val _channel = MutableStateFlow<Channel?>(null)
    /** The account's permanent channel; null until the account is known. */
    val channel: StateFlow<Channel?> = _channel.asStateFlow()
    private val _channelReference = MutableStateFlow<String?>(null)
    /** The channel's shareable reference, once its feed manifest exists. */
    val channelReference: StateFlow<String?> = _channelReference.asStateFlow()
    private val _keyHardwareBacked = MutableStateFlow<Boolean?>(null)
    val keyHardwareBacked: StateFlow<Boolean?> = _keyHardwareBacked.asStateFlow()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val lifecycle = Mutex()
    private val handleLock = ReentrantReadWriteLock()
    @Volatile private var handle: Long = 0
    private var peerPoll: Job? = null
    private var networkCallback: ConnectivityManager.NetworkCallback? = null

    var gnosisRpc: String = DEFAULT_RPC
        private set

    /** Where the in-process gateway listens; the port can move off 1633. */
    val gatewayUrl get() = "http://${_gatewayAddress.value}"

    // -- lifecycle ---------------------------------------------------------

    suspend fun start() = lifecycle.withLock { performStart() }

    private suspend fun performStart() = withContext(Dispatchers.IO) {
        if (handle != 0L || _status.value == Status.Starting) return@withContext
        _status.value = Status.Starting
        val dataDir = dataDir()
        seedPeerstoreIfNeeded(dataDir)
        val identity = try {
            AccountKeystore.loadOrCreate(context, allowCreate = !File(dataDir, "account.json").exists())
        } catch (e: Exception) {
            _status.value = Status.Failed(e.message ?: "could not load the account key")
            return@withContext
        }
        _keyHardwareBacked.value = AccountKeystore.isHardwareBacked()
        val h = try {
            AntNative.nativeInit(dataDir.absolutePath, identity)
        } catch (e: AntException) {
            _status.value = Status.Failed(e.message ?: "could not start")
            return@withContext
        }
        handle = h
        _status.value = Status.Ready
        startPollingPeers()
        startNetworkMonitor()
        startGateway()
        refreshAll()
    }

    suspend fun shutdown() = lifecycle.withLock { performShutdown() }

    private suspend fun performShutdown() = withContext(Dispatchers.IO) {
        if (handle == 0L) return@withContext
        peerPoll?.cancel()
        stopNetworkMonitor()
        val h = handleLock.write {
            val h = handle
            handle = 0
            h
        }
        _peerCount.value = 0
        _gatewayUp.value = false
        _plan.value = null
        _account.value = null
        _settlement.value = null
        _validity.value = null
        AntNative.nativeShutdown(h)
        _status.value = Status.Idle
    }

    /** Replace the account with a backed-up key, then restart the node. */
    suspend fun restoreAccount(key: String) {
        // Validated before anything is written, so a typo can't destroy
        // a working account.
        val identity = withContext(Dispatchers.IO) { AntNative.nativeIdentityFromKey(key.trim()) }
        lifecycle.withLock {
            withContext(Dispatchers.IO) { AccountKeystore.store(context, identity) }
            performShutdown()
            performStart()
        }
        (status.value as? Status.Failed)?.let { throw AntException(it.message) }
    }

    private fun <T> withHandle(body: (Long) -> T): T = handleLock.read {
        val h = handle
        if (h == 0L) throw AntException("AntStream is still starting up")
        body(h)
    }

    /**
     * Start the gateway on 127.0.0.1:1633, or the next free port up to
     * 1643 when another app (another Swarm node, a Freedom install)
     * already holds it — the same fallback range Freedom desktop uses.
     * The last address that worked is tried first, so a restart after
     * suspend lands on the same port.
     */
    @Synchronized
    private fun startGateway(): Boolean {
        val preferred = _gatewayAddress.value
        val candidates = listOf(preferred) + (GATEWAY_PORT..GATEWAY_PORT + 10).map { "127.0.0.1:$it" }
        var lastError: String? = null
        for (addr in candidates.distinct()) {
            val result = runCatching {
                withHandle { AntNative.nativeStartGateway(it, addr, true, gnosisRpc) }
            }
            if (result.isSuccess) {
                _gatewayAddress.value = addr
                _gatewayError.value = null
                _gatewayUp.value = true
                return true
            }
            lastError = result.exceptionOrNull()?.message
            Log.w(TAG, "gateway on $addr: $lastError")
            if (handle == 0L) break
        }
        _gatewayError.value = lastError ?: "could not start"
        _gatewayUp.value = false
        return false
    }

    private fun restartGateway() {
        runCatching { withHandle { AntNative.nativeStopGateway(it) } }
        _gatewayUp.value = false
        startGateway()
    }

    /** Checkpoint the node for backgrounding or a lost network. */
    suspend fun suspendNode() = withContext(Dispatchers.IO) {
        runCatching { withHandle { AntNative.nativeSuspend(it) } }
            .onFailure { Log.w(TAG, "suspend: ${it.message}") }
    }

    /** Undo [suspendNode]: re-dial the swarm, wake jobs, rebind the gateway. */
    suspend fun wake() = withContext(Dispatchers.IO) {
        if (handle == 0L) return@withContext
        runCatching { withHandle { AntNative.nativeResume(it) } }
        runCatching { withHandle { AntNative.nativeWake(it) } }
        // A running broadcast publishes through the gateway: rebinding it
        // would cut the publisher off mid-stream.
        if (!liveBroadcastActive) restartGateway()
    }

    /** Set while a broadcast runs, so a wake never rebinds its gateway. */
    @Volatile var liveBroadcastActive = false

    private fun startPollingPeers() {
        peerPoll?.cancel()
        peerPoll = scope.launch {
            var tick = 0
            while (isActive) {
                // A gateway that failed to bind is retried every 10 s.
                if (++tick % 10 == 0 && handle != 0L && !_gatewayUp.value) startGateway()
                val h = handle
                _peerCount.value = if (h == 0L) 0 else handleLock.read {
                    if (handle == 0L) 0 else AntNative.nativePeerCount(handle).coerceAtLeast(0)
                }
                delay(1_000)
            }
        }
    }

    private fun startNetworkMonitor() {
        if (networkCallback != null) return
        val cm = context.getSystemService(ConnectivityManager::class.java) ?: return
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                if (_offline.value) {
                    _offline.value = false
                    scope.launch { wake() }
                }
            }

            override fun onLost(network: Network) {
                if (cm.activeNetwork == null && !_offline.value) {
                    _offline.value = true
                    _networkLabel.value = "Offline"
                    scope.launch { suspendNode() }
                }
            }

            override fun onCapabilitiesChanged(network: Network, caps: NetworkCapabilities) {
                _networkLabel.value = when {
                    caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "Wi-Fi"
                    caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "Cellular"
                    caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "Wired"
                    else -> "Unknown"
                }
            }
        }
        cm.registerDefaultNetworkCallback(callback)
        networkCallback = callback
    }

    private fun stopNetworkMonitor() {
        val cb = networkCallback ?: return
        runCatching { context.getSystemService(ConnectivityManager::class.java)?.unregisterNetworkCallback(cb) }
        networkCallback = null
    }

    // -- readiness ---------------------------------------------------------

    val hasStorage get() = plan.value?.enabled == true
    val isOnNetwork get() = !offline.value && status.value == Status.Ready && peerCount.value > 0
    val isReadyToBroadcast
        get() = isOnNetwork && hasStorage && settlement.value?.enabled == true && gatewayUp.value

    // -- storage plan ------------------------------------------------------

    suspend fun quoteStorage(depth: Int, days: Long): StorageQuote = withContext(Dispatchers.IO) {
        val json = withHandle { AntNative.nativeStorageQuote(it, gnosisRpc, depth, days) }
        StorageQuote.parse(json) ?: throw AntException("could not read plan price")
    }

    suspend fun buyStorage(depth: Int, amountPerChunk: String) = withContext(Dispatchers.IO) {
        val json = withHandle { AntNative.nativeStorageBuyXdai(it, gnosisRpc, depth, amountPerChunk, false) }
        StoragePlan.parse(json)?.let { _plan.value = it }
        refreshAll()
    }

    suspend fun discoverStorage() = withContext(Dispatchers.IO) {
        withHandle { AntNative.nativeStorageDiscover(it, gnosisRpc) }
        refreshAll()
    }

    suspend fun exportKey(): String = withContext(Dispatchers.IO) {
        withHandle { AntNative.nativeAccountExportKey(it) }
    }

    suspend fun refreshValidity() = withContext(Dispatchers.IO) {
        if (!hasStorage) return@withContext
        runCatching { withHandle { AntNative.nativeStorageValidity(it, gnosisRpc) } }
            .getOrNull()?.let { StorageValidity.parse(it) }?.let { _validity.value = it }
    }

    suspend fun refreshAll() = withContext(Dispatchers.IO) {
        runCatching { withHandle { AntNative.nativeStorageStatus(it) } }
            .getOrNull()?.let { StoragePlan.parse(it) }?.let { _plan.value = it }
        runCatching { withHandle { AntNative.nativeAccountInfo(it) } }
            .getOrNull()?.let { AccountInfo.parse(it) }?.let { a ->
                _account.value = a
                if (a.ethAddress.isNotEmpty() && _channel.value?.owner != a.ethAddress) {
                    val c = Channel(context, a.ethAddress)
                    _channel.value = c
                    _channelReference.value = c.reference
                }
            }
        runCatching { withHandle { AntNative.nativeSettlementStatus(it) } }
            .getOrNull()?.let { SettlementInfo.parse(it) }?.let { _settlement.value = it }
    }

    // -- channel -----------------------------------------------------------

    /**
     * Make sure the permanent channel's feed manifest exists, so its link
     * can be shared before the first broadcast. Needs a storage plan
     * (the manifest is a stamped upload) and the gateway.
     */
    suspend fun ensureChannelReference(): String? = withContext(Dispatchers.IO) {
        val c = _channel.value ?: return@withContext null
        c.reference?.let { return@withContext it.also { r -> _channelReference.value = r } }
        val batch = plan.value?.takeIf { it.enabled }?.batchId ?: return@withContext null
        if (!gatewayUp.value) return@withContext null
        runCatching { c.ensureReference(gatewayUrl, batch) }
            .onFailure { Log.w(TAG, "channel: ${it.message}") }
            .getOrNull()
            ?.also { _channelReference.value = it }
    }

    // -- publisher ---------------------------------------------------------

    suspend fun startPublisher(
        channel: String,
        topic: String,
        startIndex: Long,
        batchId: String,
        bitrateKbps: Int,
        segmentMs: Int,
        notes: String,
    ) =
        withContext(Dispatchers.IO) {
            val config = JSONObject()
                .put("channel", channel)
                .put("topic", topic)
                .put("start_index", startIndex)
                .put("gateway", gatewayUrl)
                .put("batch_id", batchId)
                .put("bitrate_kbps", bitrateKbps)
                .put("segment_ms", segmentMs)
                .put("notes", notes)
            withHandle { AntNative.nativePublisherStart(it, config.toString()) }
        }

    /** Never blocks on the uplink; returns the 0/1/2 disposition, or -1. */
    fun pushSegment(segment: Segment): Int = runCatching {
        withHandle {
            AntNative.nativePublisherPushSegment(
                it, segment.isInit, segment.data, segment.durationMs, segment.discontinuity,
            )
        }
    }.onFailure { Log.w(TAG, "push segment: ${it.message}") }.getOrDefault(-1)

    suspend fun publisherProgress(): PublisherSnapshot? = withContext(Dispatchers.IO) {
        runCatching { withHandle { AntNative.nativePublisherProgress(it) } }
            .getOrNull()?.let { PublisherSnapshot.parse(it) }
    }

    suspend fun stopPublisher(): PublisherReport = withContext(Dispatchers.IO) {
        val json = withHandle { AntNative.nativePublisherStop(it) }
        PublisherReport.parse(json) ?: throw AntException("could not read the broadcast report")
    }

    // -- data dir ----------------------------------------------------------

    private fun dataDir(): File = File(context.filesDir, "antstream").apply { mkdirs() }

    /** Cold-start bootstrap: copy the bundled peer snapshot when empty. */
    private fun seedPeerstoreIfNeeded(dataDir: File) {
        val dest = File(dataDir, "peers.json")
        val hasPeers = runCatching {
            (JSONObject(dest.readText()).optJSONArray("peers")?.length() ?: 0) > 0
        }.getOrDefault(false)
        if (hasPeers) return
        runCatching {
            context.assets.open("peers.seed.json").use { input ->
                dest.outputStream().use { input.copyTo(it) }
            }
        }
    }

    companion object {
        private const val TAG = "AntNode"
        const val GATEWAY_PORT = 1633
        const val GATEWAY_ADDRESS = "127.0.0.1:$GATEWAY_PORT"
        const val DEFAULT_RPC = "https://rpc.gnosischain.com"
    }
}
