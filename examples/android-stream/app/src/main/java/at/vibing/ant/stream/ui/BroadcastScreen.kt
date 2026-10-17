package at.vibing.ant.stream.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.ChannelAddress
import at.vibing.ant.stream.LiveBroadcast
import at.vibing.ant.stream.formatBytes

/** The "ready to broadcast" checklist and the Go-live entry point. */
@Composable
fun BroadcastScreen(
    node: AntNode,
    broadcast: LiveBroadcast,
    onGetStarted: () -> Unit,
    onGoLive: () -> Unit,
    onWatch: (String) -> Unit,
) {
    val status by node.status.collectAsState()
    val peers by node.peerCount.collectAsState()
    val plan by node.plan.collectAsState()
    val settlement by node.settlement.collectAsState()
    val gatewayUp by node.gatewayUp.collectAsState()
    val gatewayError by node.gatewayError.collectAsState()
    val gatewayAddress by node.gatewayAddress.collectAsState()
    val offline by node.offline.collectAsState()
    val network by node.networkLabel.collectAsState()
    val announcing by broadcast.announcing.collectAsState()
    val hardware by node.keyHardwareBacked.collectAsState()
    val report by broadcast.report.collectAsState()
    val channel by node.channel.collectAsState()
    val context = LocalContext.current

    val connected = !offline && status == AntNode.Status.Ready && peers > 0
    val hasPlan = plan?.enabled == true
    val settled = settlement?.enabled == true
    val ready = connected && hasPlan && settled && gatewayUp

    Column(
        Modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text("AntStream", style = MaterialTheme.typography.headlineLarge, fontWeight = FontWeight.Bold)
        Text(
            "Broadcast live video from your camera onto Swarm.",
            color = Color.Gray,
        )
        SectionCard {
            Text("Ready to broadcast", style = MaterialTheme.typography.titleMedium)
            CheckRow(
                "Connected",
                connected,
                when (val s = status) {
                    is AntNode.Status.Failed -> "Offline: ${s.message}"
                    AntNode.Status.Starting -> "Starting…"
                    AntNode.Status.Idle -> "Idle"
                    AntNode.Status.Ready -> if (offline) "No network" else "$peers peers · $network"
                },
            )
            CheckRow(
                "Key secured",
                hardware != null,
                when (hardware) {
                    true -> "Android Keystore (hardware-backed)"
                    false -> "Android Keystore"
                    null -> "…"
                },
            )
            CheckRow(
                "Storage plan",
                hasPlan,
                plan?.takeIf { it.enabled }?.let { "${formatBytes(it.usedBytes)} of ${formatBytes(it.totalBytes)} used" }
                    ?: "No plan yet",
            )
            CheckRow("Settlement", settled, if (settled) "Chequebook ready" else "Set up with your plan")
            CheckRow(
                "Gateway",
                gatewayUp,
                when {
                    gatewayUp -> "http://$gatewayAddress"
                    gatewayError != null -> "Not running: $gatewayError (retrying)"
                    else -> "Starting…"
                },
            )
        }
        if (!hasPlan && status == AntNode.Status.Ready) {
            Button(onClick = onGetStarted, modifier = Modifier.fillMaxWidth()) { Text("Get started") }
        }
        if (announcing) Text(LiveBroadcast.ANNOUNCING_HOLD_NOTE, color = Amber, style = MaterialTheme.typography.bodySmall)
        Button(
            onClick = onGoLive,
            enabled = (ready || broadcast.selfTestDir != null) && !announcing,
            modifier = Modifier.fillMaxWidth(),
        ) { Text("Go live") }
        channel?.let { ch -> StreamInfoCard(node, broadcast, ch.name) }
        channel?.address?.let { address ->
            ChannelCard(address, onWatch)
        }
        report?.let { r ->
            SectionCard {
                Text("Last broadcast", style = MaterialTheme.typography.titleMedium)
                Text(
                    String.format(
                        "%.0f s · %d segments · %s · %.2f Mbit/s",
                        r.durationS,
                        r.segmentsListed,
                        formatBytes(r.bytesPublished),
                        r.sustainedMbitS,
                    ),
                )
                r.channel?.let { c -> OutlinedButton(onClick = { onWatch(c.key) }) { Text("Watch") } }
            }
        }
    }
}

/**
 * The account's channel: owner + topic, what viewers open it by. It
 * exists from the start (no plan needed to share it); viewers who open
 * it join whenever the account is live, and see the last broadcast
 * otherwise.
 */
@Composable
fun ChannelCard(address: ChannelAddress, onWatch: (String) -> Unit) {
    val context = LocalContext.current
    SectionCard {
        Text("Your channel", style = MaterialTheme.typography.titleMedium)
        Text(
            "Your account address and the topic \"${address.topic}\": one channel for all your " +
                "broadcasts. Share it any time: viewers who open it join whenever you're live, and " +
                "see your last broadcast otherwise.",
            color = Color.Gray,
            style = MaterialTheme.typography.bodySmall,
        )
        Text("Owner", color = Color.Gray, style = MaterialTheme.typography.labelSmall)
        MonoText("0x${address.owner}")
        Text("Topic", color = Color.Gray, style = MaterialTheme.typography.labelSmall)
        MonoText(address.topic)
        Text("Web player (Freedom browser)", color = Color.Gray, style = MaterialTheme.typography.labelSmall)
        MonoText(address.playerLink)
        address.swarmUri?.let {
            Text("Solar Punk players", color = Color.Gray, style = MaterialTheme.typography.labelSmall)
            MonoText(it)
        }
        Button(onClick = { shareText(context, address.watchLinks()) }, modifier = Modifier.fillMaxWidth()) {
            Text("Share channel links")
        }
        OutlinedButton(
            onClick = { copyToClipboard(context, "Player link", address.playerLink) },
            modifier = Modifier.fillMaxWidth(),
        ) { Text("Copy player link") }
        address.swarmUri?.let { uri ->
            OutlinedButton(
                onClick = { copyToClipboard(context, "Swarm URI", uri) },
                modifier = Modifier.fillMaxWidth(),
            ) { Text("Copy swarm:// URI") }
        }
        OutlinedButton(onClick = { onWatch(address.key) }, modifier = Modifier.fillMaxWidth()) { Text("Watch") }
    }
}

/** Channel name field, shared by the live screen. */
@Composable
fun ChannelField(value: String, onChange: (String) -> Unit) {
    OutlinedTextField(
        value = value,
        onValueChange = onChange,
        label = { Text("Channel name") },
        singleLine = true,
        modifier = Modifier.fillMaxWidth(),
    )
}
