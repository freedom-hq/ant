package at.vibing.ant.stream.ui

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
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
                                price != null -> Text("${price.xdaiToSendDisplay} xDAI", fontWeight = FontWeight.Bold)
                                loading -> CircularProgressIndicator()
                                else -> Text("›")
                            }
                        }
                    }
                }
                OutlinedButton(onClick = onClose, modifier = Modifier.fillMaxWidth()) { Text("Close") }
            }
            else -> {
                Text(tier.title, style = MaterialTheme.typography.headlineMedium, fontWeight = FontWeight.Bold)
                if (q == null) {
                    CircularProgressIndicator()
                } else if (q.sufficientFunds) {
                    Text("Payment received", color = Green, style = MaterialTheme.typography.titleLarge)
                } else {
                    SectionCard {
                        Text("Send", color = Color.Gray)
                        Text("${q.xdaiToSendDisplay} xDAI", style = MaterialTheme.typography.headlineLarge, fontWeight = FontWeight.Bold)
                        Text("on Gnosis Chain to", color = Color.Gray)
                        account?.let { a ->
                            MonoText(a.ethAddress)
                            OutlinedButton(onClick = { copyToClipboard(context, "Address", a.ethAddress) }) {
                                Text("Copy address")
                            }
                        }
                        if ((q.settlementDepositPlur.toBigIntegerOrNull()?.signum() ?: 0) > 0) {
                            Text(
                                "Includes a one-time ${q.settlementDepositBzz} xBZZ network deposit that keeps your broadcast flowing.",
                                color = Color.Gray,
                                style = MaterialTheme.typography.bodySmall,
                            )
                        }
                    }
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        CircularProgressIndicator(Modifier.padding(end = 12.dp))
                        Text("Waiting for payment… (balance ${q.accountXdaiDisplay} xDAI)", color = Color.Gray)
                    }
                }
                Button(
                    onClick = { activate(tier, q ?: return@Button) },
                    enabled = q?.sufficientFunds == true,
                    modifier = Modifier.fillMaxWidth(),
                ) { Text("Activate") }
                OutlinedButton(onClick = {
                    selected = null
                    quote = null
                }, modifier = Modifier.fillMaxWidth()) { Text("Back") }
            }
        }
        error?.let { Text(it, color = Amber) }
    }
}
