package at.vibing.ant.stream.ui

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.TextButton
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.background
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.StoragePlanTier
import at.vibing.ant.stream.StorageQuote
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * Onboarding, ported from `GetStartedView.swift`: pick a plan (priced
 * live against Gnosis), send the quoted xDAI to the account, and the
 * screen activates the plan by itself the moment the funds land —
 * `ant_storage_buy_xdai` swaps the xBZZ shortfall, buys the batch and
 * deploys the chequebook.
 */
@Composable
fun GetStartedScreen(node: AntNode, onClose: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val account by node.account.collectAsState()
    val quotes = remember { mutableStateMapOf<String, StorageQuote>() }
    var loading by remember { mutableStateOf(true) }
    var selected by remember { mutableStateOf<StoragePlanTier?>(null) }
    var quote by remember { mutableStateOf<StorageQuote?>(null) }
    var activating by remember { mutableStateOf(false) }
    var error by remember { mutableStateOf<String?>(null) }

    BackHandler(enabled = !activating) {
        if (selected != null) {
            selected = null
            quote = null
        } else {
            onClose()
        }
    }

    LaunchedEffect(Unit) {
        val results = StoragePlanTier.all.map { tier ->
            async { tier.id to runCatching { node.quoteStorage(tier.depth, tier.days) }.getOrNull() }
        }.awaitAll()
        results.forEach { (id, q) -> if (q != null) quotes[id] = q }
        loading = false
        if (quotes.isEmpty()) error = "Couldn't reach the network to price plans."
    }

    fun activate(tier: StoragePlanTier, q: StorageQuote) {
        activating = true
        error = null
        scope.launch {
            runCatching { node.buyStorage(tier.depth, q.amountPerChunk) }
                .onSuccess {
                    if (node.hasStorage) onClose() else error = "Storage didn't activate. Please try again."
                }
                .onFailure { error = it.message }
            activating = false
        }
    }

    // Poll the quote until the payment lands, then activate once.
    LaunchedEffect(selected) {
        val tier = selected ?: return@LaunchedEffect
        while (true) {
            val q = quote
            if (q != null && q.sufficientFunds) {
                activate(tier, q)
                return@LaunchedEffect
            }
            delay(6_000)
            runCatching { node.quoteStorage(tier.depth, tier.days) }.getOrNull()?.let {
                quote = it
                quotes[tier.id] = it
            }
        }
    }

    val payTier = selected
    if (payTier != null && !activating) {
        PaymentStep(
            quote = quote,
            address = account?.ethAddress,
            error = error,
            onBack = {
                selected = null
                quote = null
            },
        )
        return
    }

    Column(
        Modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp),
    ) {
        val tier = selected
        val q = quote
        when {
            activating -> {
                CircularProgressIndicator(Modifier.align(Alignment.CenterHorizontally))
                Text("Activating your storage…", style = MaterialTheme.typography.titleLarge)
                Text(
                    "Confirming your payment on the network. This can take up to a minute; please keep the app open.",
                    color = Color.Gray,
                )
            }
            tier == null -> {
                Text("Welcome to AntStream", style = MaterialTheme.typography.headlineMedium, fontWeight = FontWeight.Bold)
                Text(
                    "Pick a storage plan to start broadcasting live video on the decentralized network.",
                    color = Color.Gray,
                )
                StoragePlanTier.all.forEach { t ->
                    SectionCard {
                        Row(
                            Modifier.fillMaxWidth().clickable {
                                selected = t
                                quote = quotes[t.id]
                                error = null
                            },
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            Column(Modifier.weight(1f)) {
                                Text(t.title, style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.Bold)
                                Text("${t.capacityLabel} · ${t.durationLabel}", color = Color.Gray)
                            }
                            val price = quotes[t.id]
                            when {
                                price != null -> Text("${roundedUpXdai(price.xdaiToSendDisplay)} xDAI", fontWeight = FontWeight.Bold)
                                loading -> CircularProgressIndicator()
                                else -> Text("›")
                            }
                        }
                    }
                }
                OutlinedButton(onClick = onClose, modifier = Modifier.fillMaxWidth()) { Text("Close") }
            }
            else -> Unit
        }
        error?.let { Text(it, color = Amber) }
    }
}

/**
 * The payment step, kept to what a payer needs: the amount, a QR code a
 * wallet fills everything in from (address, Gnosis, amount) and a sign
 * that we're waiting. The plan activates by itself once the funds land.
 * Everything else (the address as text, copy buttons, the Gnosis-only
 * warning, what the deposit is for) sits behind the ⓘ.
 */
@Composable
private fun PaymentStep(quote: StorageQuote?, address: String?, error: String?, onBack: () -> Unit) {
    val context = LocalContext.current
    var help by remember { mutableStateOf(false) }
    val send = quote?.let { roundedUpXdai(it.xdaiToSendDisplay) }

    Box(Modifier.fillMaxSize().padding(20.dp)) {
        TextButton(onClick = onBack, modifier = Modifier.align(Alignment.TopStart)) {
            Text("‹", style = MaterialTheme.typography.headlineMedium, color = Color.White)
        }
        TextButton(onClick = { help = true }, modifier = Modifier.align(Alignment.TopEnd)) {
            Text("ⓘ", style = MaterialTheme.typography.headlineSmall, color = Color.Gray)
        }
        Column(
            Modifier.align(Alignment.Center),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(22.dp),
        ) {
            when {
                quote == null || address == null || send == null -> CircularProgressIndicator(color = Color.Gray)
                quote.sufficientFunds -> {
                    Text("✓", style = MaterialTheme.typography.displayLarge, color = Green)
                    Text("Received", color = Color.Gray)
                }
                else -> {
                    // What payment processors (Coinbase Commerce, BitPay)
                    // show: exact amount, the network, the code, the
                    // address with one-tap copy, and a waiting status.
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        Text("Send exactly", color = Color.Gray)
                        Text(
                            "$send xDAI",
                            style = MaterialTheme.typography.displaySmall,
                            fontWeight = FontWeight.SemiBold,
                            modifier = Modifier.clickable { copyToClipboard(context, "Amount", send) },
                        )
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier
                                .padding(top = 8.dp)
                                .background(Color(0xFF1C2B22), RoundedCornerShape(50))
                                .padding(horizontal = 12.dp, vertical = 4.dp),
                        ) {
                            Box(Modifier.size(8.dp).background(Green, CircleShape))
                            Text("  on Gnosis Chain", color = Green, style = MaterialTheme.typography.labelLarge)
                        }
                    }
                    PaymentQr(address = address, xdaiAmount = send, size = 220.dp)
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier
                            .background(Color(0xFF1C1C1E), RoundedCornerShape(12.dp))
                            .clickable { copyToClipboard(context, "Address", address) }
                            .padding(horizontal = 14.dp, vertical = 10.dp),
                    ) {
                        Text(
                            "to ${address.take(8)}…${address.takeLast(6)}",
                            fontFamily = FontFamily.Monospace,
                            style = MaterialTheme.typography.bodyMedium,
                        )
                        Text("   Copy", color = MaterialTheme.colorScheme.primary, style = MaterialTheme.typography.labelLarge)
                    }
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        CircularProgressIndicator(
                            modifier = Modifier.size(16.dp),
                            strokeWidth = 2.dp,
                            color = Color.Gray,
                        )
                        Text("  Waiting for payment", color = Color.Gray, style = MaterialTheme.typography.bodyMedium)
                    }
                }
            }
            error?.let { Text(it, color = Amber, style = MaterialTheme.typography.bodySmall) }
        }
    }

    if (help && address != null && quote != null && send != null) {
        AlertDialog(
            onDismissRequest = { help = false },
            confirmButton = { TextButton(onClick = { help = false }) { Text("Done") } },
            title = { Text("How to pay") },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    Text("Scan the code with your wallet, or send $send xDAI to:")
                    MonoText(address)
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedButton(onClick = { copyToClipboard(context, "Address", address) }) { Text("Copy address") }
                        OutlinedButton(onClick = { copyToClipboard(context, "Amount", send) }) { Text("Copy amount") }
                    }
                    GnosisOnlyNote()
                    if ((quote.settlementDepositPlur.toBigIntegerOrNull()?.signum() ?: 0) > 0) {
                        Text(
                            "Includes a one-time ${quote.settlementDepositBzz} xBZZ network deposit that keeps your broadcast flowing.",
                            color = Color.Gray,
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                    Text(
                        "This screen notices the payment by itself and activates your plan; " +
                            "keep the app open until it does.",
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            },
        )
    }
}
