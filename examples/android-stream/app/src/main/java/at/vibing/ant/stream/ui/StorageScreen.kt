package at.vibing.ant.stream.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.formatBytes
import kotlinx.coroutines.launch

/** Plan meter, account, and key backup / restore. */
@Composable
fun StorageScreen(node: AntNode, onGetStarted: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val plan by node.plan.collectAsState()
    val validity by node.validity.collectAsState()
    val account by node.account.collectAsState()
    val settlement by node.settlement.collectAsState()
    val deposit by node.settlementDeposit.collectAsState()
    var busy by remember { mutableStateOf<String?>(null) }
    var message by remember { mutableStateOf<String?>(null) }
    var revealedKey by remember { mutableStateOf<String?>(null) }
    var restoring by remember { mutableStateOf(false) }
    var restoreKey by remember { mutableStateOf("") }
    var confirmDeposit by remember { mutableStateOf(false) }

    LaunchedEffect(plan?.batchId) { node.refreshValidity() }
    LaunchedEffect(settlement?.enabled) { node.refreshSettlementDeposit() }

    Column(
        Modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        Text("Storage", style = MaterialTheme.typography.headlineMedium)
        SectionCard {
            val p = plan
            if (p != null && p.enabled) {
                Text("${formatBytes(p.usedBytes)} of ${formatBytes(p.totalBytes)} used")
                LinearProgressIndicator(progress = { p.usedFraction.toFloat() }, modifier = Modifier.fillMaxWidth())
                validity?.let { Text("Valid for ${it.durationLabel}", color = Color.Gray) }
                Text("Batch", color = Color.Gray)
                MonoText(p.batchId)
            } else {
                Text("No storage plan yet.")
                Button(onClick = onGetStarted, modifier = Modifier.fillMaxWidth()) { Text("Get started") }
                OutlinedButton(
                    onClick = {
                        busy = "Looking for a plan on Gnosis…"
                        scope.launch {
                            message = runCatching { node.discoverStorage() }
                                .fold({ if (node.hasStorage) "Plan found." else "No plan found for this account." }) { it.message }
                            busy = null
                        }
                    },
                    enabled = busy == null,
                    modifier = Modifier.fillMaxWidth(),
                ) { Text("Find an existing plan") }
            }
            Text(
                if (settlement?.enabled == true) "Settlement: chequebook ${settlement?.chequebook.orEmpty()}" else "Settlement: not set up",
                color = Color.Gray,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        val d = deposit
        if (plan?.enabled == true && settlement?.enabled == true && d != null && d.needsTopUp) {
            // Mirrors the iOS Storage tab's deposit card: a chequebook
            // deployed with too little behind it signs cheques nobody
            // can cash, so broadcasts run for a while, then stall.
            SectionCard {
                Text("Broadcasts will stall without a deposit", color = Amber, style = MaterialTheme.typography.titleMedium)
                Text(
                    "Your account pays the network as it publishes, and the chequebook it pays from holds " +
                        "${d.depositBzz} of the ${d.targetBzz} xBZZ it needs. Adding the missing " +
                        "${d.shortfallBzz} xBZZ keeps segments flowing; it stays yours until it's spent.",
                )
                if (!d.sufficientFunds) {
                    Text(
                        "Add about ${d.xdaiToSendDisplay} xDAI to your account first.",
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
                Button(
                    onClick = { confirmDeposit = true },
                    enabled = busy == null,
                    modifier = Modifier.fillMaxWidth(),
                ) { Text("Add deposit") }
            }
        }
        if (settlement?.enabled == true) CreditCard(node)
        SectionCard {
            Text("Account", style = MaterialTheme.typography.titleMedium)
            account?.let { a ->
                Box(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
                    PaymentQr(address = a.ethAddress, size = 160.dp)
                }
                MonoText(a.ethAddress)
                GnosisOnlyNote()
                OutlinedButton(onClick = { copyToClipboard(context, "Address", a.ethAddress) }) { Text("Copy address") }
                Text(a.agent, color = Color.Gray, style = MaterialTheme.typography.bodySmall)
            }
            OutlinedButton(
                onClick = { scope.launch { revealedKey = runCatching { node.exportKey() }.getOrElse { message = it.message; null } } },
                modifier = Modifier.fillMaxWidth(),
            ) { Text("Back up key") }
            OutlinedButton(onClick = { restoring = true }, modifier = Modifier.fillMaxWidth()) { Text("Restore from key") }
        }
        busy?.let { Text(it, color = Color.Gray) }
        message?.let { Text(it, color = Amber) }
    }

    if (confirmDeposit) {
        val d = deposit
        AlertDialog(
            onDismissRequest = { confirmDeposit = false },
            title = { Text("Add the ${d?.shortfallBzz ?: "missing"} xBZZ deposit?") },
            text = {
                Text(
                    "This sends real transactions on Gnosis Chain: the deposit moves from your account into " +
                        "the chequebook, and the node first swaps xDAI for any xBZZ your account lacks. " +
                        "It costs a little xDAI for gas.",
                )
            },
            confirmButton = {
                TextButton(onClick = {
                    confirmDeposit = false
                    busy = "Adding the deposit… waiting for the transactions to confirm"
                    scope.launch {
                        message = runCatching { node.topUpDeposit() }
                            .fold({ "Deposit added: broadcasts are paid for." }) { it.message }
                        busy = null
                    }
                }) { Text("Add deposit") }
            },
            dismissButton = { TextButton(onClick = { confirmDeposit = false }) { Text("Cancel") } },
        )
    }
    revealedKey?.let { key ->
        AlertDialog(
            onDismissRequest = { revealedKey = null },
            title = { Text("Account key") },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text("Anyone with this key controls the account and its funds. Keep it somewhere safe.")
                    MonoText(key)
                }
            },
            confirmButton = {
                TextButton(onClick = { copyToClipboard(context, "Key", key) }) { Text("Copy") }
            },
            dismissButton = { TextButton(onClick = { revealedKey = null }) { Text("Done") } },
        )
    }
    if (restoring) {
        AlertDialog(
            onDismissRequest = { restoring = false },
            title = { Text("Restore account") },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text("This replaces the account on this device. Back up the current key first if it holds funds.")
                    OutlinedTextField(value = restoreKey, onValueChange = { restoreKey = it }, label = { Text("Account key (hex)") })
                }
            },
            confirmButton = {
                TextButton(onClick = {
                    restoring = false
                    busy = "Restoring…"
                    scope.launch {
                        message = runCatching { node.restoreAccount(restoreKey) }.fold({ "Account restored." }) { it.message }
                        restoreKey = ""
                        busy = null
                    }
                }) { Text("Restore") }
            },
            dismissButton = { TextButton(onClick = { restoring = false }) { Text("Cancel") } },
        )
    }
}
