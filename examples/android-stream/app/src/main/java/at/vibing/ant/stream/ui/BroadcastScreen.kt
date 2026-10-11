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
    val hardware by node.keyHardwareBacked.collectAsState()
    val report by broadcast.report.collectAsState()
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
        Button(onClick = onGoLive, enabled = ready || broadcast.selfTestDir != null, modifier = Modifier.fillMaxWidth()) { Text("Go live") }
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
                if (r.channelReference.isNotEmpty()) {
                    MonoText(r.channelReference)
                    OutlinedButton(onClick = { copyToClipboard(context, "Channel", r.channelReference) }) {
                        Text("Copy channel reference")
                    }
                    OutlinedButton(onClick = { onWatch(r.channelReference) }) { Text("Watch") }
                }
            }
        }
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
