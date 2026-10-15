package at.vibing.ant.stream.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.Credit
import at.vibing.ant.stream.WalletBalance
import at.vibing.ant.stream.bzzInput
import at.vibing.ant.stream.formatBzz
import at.vibing.ant.stream.formatXdai
import at.vibing.ant.stream.parseBzzAmount
import java.math.BigInteger
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/** Below this the chequebook can't cover another cheque (bee's minimum, ~9e9 PLUR). */
private val MIN_CHEQUE_PLUR = BigInteger.valueOf(9_000_000_000L)

/** "Low" once less than 0.0005 xBZZ is left. */
private val LOW_CREDIT_PLUR = BigInteger.valueOf(5_000_000_000_000L)

/** How often the wallet is re-read while the card is on screen. */
private const val WALLET_REFRESH_MS = 30_000L

/** Quick picks for the amount field, in xBZZ ("All" is added per action). */
private val QUICK_PICKS = listOf("0.1", "0.5", "1")

/** Which way xBZZ moves between the account wallet and the chequebook. */
private enum class Move(val title: String, val verb: String, val busy: String) {
    Add("Add to credit", "Add", "Adding"),
    Withdraw("Withdraw to account", "Withdraw", "Withdrawing"),
}

/**
 * The chequebook, live: what it can still pay, whether the node is paying
 * peers (fast) or on the free tier, what this run has paid and how fast,
 * and the latest payments. Fed by [AntNode.credit], which refreshes every
 * 2 s from memory.
 *
 * Below that, the account wallet the chequebook is funded from (re-read
 * every 30 s while the card is on screen, and after every action), with
 * "Add to credit" (wallet → chequebook) and "Withdraw to account"
 * (chequebook → wallet). Both send a real Gnosis transaction, so each
 * asks for an amount and then for confirmation first.
 */
@Composable
fun CreditCard(node: AntNode) {
    val credit by node.credit.collectAsState()
    val wallet by node.wallet.collectAsState()
    val walletError by node.walletError.collectAsState()
    val scope = rememberCoroutineScope()
    var picking by remember { mutableStateOf<Move?>(null) }
    var confirming by remember { mutableStateOf<Confirming?>(null) }
    var busy by remember { mutableStateOf<String?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    var done by remember { mutableStateOf<String?>(null) }

    LaunchedEffect(Unit) {
        while (true) {
            node.refreshWallet()
            delay(WALLET_REFRESH_MS)
        }
    }

    val available = credit?.swap?.availablePlur
    SectionCard {
        CreditFigures(credit)
        HorizontalDivider()
        Text("Account wallet", style = MaterialTheme.typography.titleSmall)
        Figure("xBZZ", wallet?.let { formatBzz(it.bzzPlur) } ?: "–")
        Figure("xDAI for gas", wallet?.let { formatXdai(it.xdaiWei) } ?: "–")
        walletError?.let {
            Text("Couldn't read the wallet: $it", color = Amber, style = MaterialTheme.typography.bodySmall)
        }
        if (wallet?.xdaiWei?.signum() == 0) {
            Text(
                "Moving xBZZ costs a little xDAI for gas. Send some xDAI to your account first.",
                color = Amber,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        val idle = busy == null
        val hasGas = (wallet?.xdaiWei?.signum() ?: 0) > 0
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
            Button(
                onClick = { error = null; done = null; picking = Move.Add },
                enabled = idle && hasGas && (wallet?.bzzPlur?.signum() ?: 0) > 0,
                modifier = Modifier.weight(1f),
            ) { Text(Move.Add.title) }
            OutlinedButton(
                onClick = { error = null; done = null; picking = Move.Withdraw },
                enabled = idle && hasGas && (available?.signum() ?: 0) > 0,
                modifier = Modifier.weight(1f),
            ) { Text(Move.Withdraw.title) }
        }
        busy?.let {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp)
                Text(it, color = Color.Gray, style = MaterialTheme.typography.bodySmall)
            }
        }
        error?.let { Text(it, color = Red, style = MaterialTheme.typography.bodySmall) }
        done?.let { Text(it, color = Green, style = MaterialTheme.typography.bodySmall) }
    }

    picking?.let { move ->
        val max = when (move) {
            Move.Add -> wallet?.bzzPlur
            Move.Withdraw -> available
        } ?: BigInteger.ZERO
        AmountDialog(
            move = move,
            max = max,
            wallet = wallet,
            onDismiss = { picking = null },
            onContinue = { amount ->
                picking = null
                // The account the amount was picked against: an add
                // refuses to send if the wallet is another one by then.
                confirming = Confirming(move, amount, wallet?.address)
            },
        )
    }
    confirming?.let { (move, amount, address) ->
        ConfirmDialog(
            move = move,
            amount = amount,
            wallet = wallet,
            onDismiss = { confirming = null },
            onConfirm = {
                confirming = null
                busy = "${move.busy} ${formatBzz(amount)}… waiting for the transaction to confirm"
                scope.launch {
                    runCatching {
                        when (move) {
                            Move.Add -> node.addCredit(amount, address)
                            Move.Withdraw -> node.withdrawCredit(amount)
                        }
                    }.fold(
                        onSuccess = {
                            done = when (move) {
                                Move.Add -> "Added ${formatBzz(amount)} to credit"
                                Move.Withdraw -> "Withdrew ${formatBzz(amount)} to your account"
                            }
                        },
                        onFailure = { error = it.message ?: "${move.verb} failed" },
                    )
                    busy = null
                }
            },
        )
    }
}

/** A move awaiting confirmation, with the wallet address it was picked against. */
private data class Confirming(val move: Move, val amount: BigInteger, val address: String?)

/** The live chequebook figures: credit left, paying or not, payments. */
@Composable
private fun CreditFigures(credit: Credit?) {
    Text("Credit", style = MaterialTheme.typography.titleMedium)
    val swap = credit?.swap
    val available = swap?.availablePlur
    when {
        swap == null -> Text("Checking…", color = Color.Gray)
        swap.chequebook == null -> Text("No chequebook yet", style = MaterialTheme.typography.headlineSmall)
        available == null -> Text("Reading the deposit…", style = MaterialTheme.typography.headlineSmall)
        else -> Text(
            formatBzz(available),
            style = MaterialTheme.typography.headlineMedium,
            fontWeight = FontWeight.SemiBold,
        )
    }
    if (swap != null) {
        val usedUp = available != null && available < MIN_CHEQUE_PLUR
        val (text, color) = when {
            !swap.swapEnabled -> "Free tier: paying peers is switched off" to Amber
            swap.chequebook == null -> "Free tier: no chequebook" to Amber
            usedUp -> "Free tier: the deposit is used up" to Red
            swap.paying && available != null && available < LOW_CREDIT_PLUR -> "Paying peers · credit is low" to Amber
            swap.paying -> "Paying peers · fast uploads" to Green
            else -> "Free tier for now: waiting for the deposit to be read" to Amber
        }
        Text(text, color = color, style = MaterialTheme.typography.bodyMedium)
        if (swap.swapEnabled && swap.chequebook != null && usedUp) {
            Text(
                "Uploads are on the slow free tier until the chequebook has credit again. " +
                    "Add xBZZ from your account wallet below.",
                color = Color.Gray,
                style = MaterialTheme.typography.bodySmall,
            )
        }
    }
    if (credit != null && swap?.issuedPlur != null) {
        Figure("Paid this session", formatBzz(credit.sessionPlur))
        Figure("Paying now", credit.perMinutePlur?.let { "${formatBzz(it)} / min" } ?: "–")
        Figure("Paid in total", formatBzz(swap.issuedPlur))
        swap.depositedPlur?.let { Figure("Deposited", formatBzz(it)) }
    }
    if (!credit?.recent.isNullOrEmpty()) {
        Text("Latest payments", color = Color.Gray, style = MaterialTheme.typography.bodySmall)
        val now by rememberNow()
        Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
            credit.recent.forEach { p ->
                Row(Modifier.fillMaxWidth()) {
                    Text(
                        "${p.peer.take(6)}…${p.peer.takeLast(4)}",
                        fontFamily = FontFamily.Monospace,
                        style = MaterialTheme.typography.bodySmall,
                        modifier = Modifier.weight(1f),
                    )
                    Text(
                        "${formatBzz(p.plur)} · ${ago(now - p.atMillis)}",
                        style = MaterialTheme.typography.bodySmall,
                        color = Color.Gray,
                    )
                }
            }
        }
    }
}

/**
 * Pick how much to move: an xBZZ amount field and quick picks, checked
 * against [max] (the wallet's xBZZ to add, the free credit to withdraw).
 */
@Composable
private fun AmountDialog(
    move: Move,
    max: BigInteger,
    wallet: WalletBalance?,
    onDismiss: () -> Unit,
    onContinue: (BigInteger) -> Unit,
) {
    var text by remember { mutableStateOf("") }
    val amount = parseBzzAmount(text)
    val problem = when {
        text.isBlank() -> null
        amount == null -> "Enter an amount in xBZZ, like 0.5"
        amount > max -> "That's more than the ${formatBzz(max)} available"
        else -> null
    }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(move.title) },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    when (move) {
                        Move.Add ->
                            "Moves xBZZ from your account wallet (${formatBzz(max)}) into the chequebook " +
                                "that pays peers for your uploads."
                        Move.Withdraw ->
                            "Moves xBZZ from the chequebook back to your account wallet. Up to " +
                                "${formatBzz(max)}: the rest backs payments already sent to peers."
                    },
                    style = MaterialTheme.typography.bodyMedium,
                )
                OutlinedTextField(
                    value = text,
                    onValueChange = { text = it },
                    label = { Text("Amount") },
                    suffix = { Text("xBZZ") },
                    singleLine = true,
                    isError = problem != null,
                    supportingText = problem?.let { { Text(it) } },
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal),
                    modifier = Modifier.fillMaxWidth(),
                )
                Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    QUICK_PICKS.forEach { pick ->
                        QuickPick(pick, enabled = parseBzzAmount(pick)!! <= max) { text = pick }
                    }
                    val all = bzzInput(max)
                    QuickPick("All", enabled = parseBzzAmount(all) != null) { text = all }
                }
                if (wallet != null) {
                    Text(
                        "Gas is paid in xDAI: your account has ${formatXdai(wallet.xdaiWei)}.",
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
        },
        confirmButton = {
            TextButton(
                onClick = { amount?.let(onContinue) },
                enabled = amount != null && problem == null,
            ) { Text("Continue") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
    )
}

@Composable
private fun QuickPick(label: String, enabled: Boolean, onClick: () -> Unit) {
    OutlinedButton(
        onClick = onClick,
        enabled = enabled,
        contentPadding = PaddingValues(horizontal = 10.dp, vertical = 4.dp),
    ) { Text(label) }
}

/** The last word before a real transaction: what moves where, and the gas. */
@Composable
private fun ConfirmDialog(
    move: Move,
    amount: BigInteger,
    wallet: WalletBalance?,
    onDismiss: () -> Unit,
    onConfirm: () -> Unit,
) {
    val what = formatBzz(amount)
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("${move.verb} $what?") },
        text = {
            Text(
                "This sends a real transaction on Gnosis Chain. " +
                    when (move) {
                        Move.Add -> "$what moves from your account wallet into the chequebook. "
                        Move.Withdraw -> "$what moves from the chequebook back to your account wallet. "
                    } +
                    "It costs a little xDAI for gas" +
                    (wallet?.let { " (your account has ${formatXdai(it.xdaiWei)})" } ?: "") +
                    ", and can't be undone once sent.",
            )
        },
        confirmButton = { TextButton(onClick = onConfirm) { Text(move.verb) } },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Cancel") } },
    )
}

/** One line for the live screen: credit left and the pace of payments. */
fun creditSummary(credit: Credit?): String? {
    val swap = credit?.swap ?: return null
    val available = swap.availablePlur ?: return null
    if (!swap.paying) return "Free tier · ${formatBzz(available)} left"
    val pace = credit.perMinutePlur?.let { " · ${formatBzz(it)}/min" }.orEmpty()
    return "Paying · ${formatBzz(available)} left$pace"
}

@Composable
private fun Figure(label: String, value: String) {
    Row(Modifier.fillMaxWidth()) {
        Text(label, color = Color.Gray, modifier = Modifier.weight(1f))
        Text(value)
    }
}

@Composable
private fun rememberNow() = remember { mutableLongStateOf(System.currentTimeMillis()) }.also { state ->
    LaunchedEffect(Unit) {
        while (true) {
            delay(1_000)
            state.longValue = System.currentTimeMillis()
        }
    }
}

private fun ago(ms: Long): String = when {
    ms < 5_000 -> "now"
    ms < 60_000 -> "${ms / 1_000} s ago"
    else -> "${ms / 60_000} min ago"
}
