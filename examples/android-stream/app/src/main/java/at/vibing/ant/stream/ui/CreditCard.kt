package at.vibing.ant.stream.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import at.vibing.ant.stream.Credit
import at.vibing.ant.stream.formatBzz
import java.math.BigInteger
import kotlinx.coroutines.delay

/** Below this the chequebook can't cover another cheque (bee's minimum, ~9e9 PLUR). */
private val MIN_CHEQUE_PLUR = BigInteger.valueOf(9_000_000_000L)

/** "Low" once less than 0.0005 xBZZ is left. */
private val LOW_CREDIT_PLUR = BigInteger.valueOf(5_000_000_000_000L)

/**
 * The chequebook, live: what it can still pay, whether the node is paying
 * peers (fast) or on the free tier, what this run has paid and how fast,
 * and the latest payments. Fed by [at.vibing.ant.stream.AntNode.credit],
 * which refreshes every 2 s from memory.
 */
@Composable
fun CreditCard(credit: Credit?) {
    SectionCard {
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
            val (text, color) = when {
                !swap.swapEnabled -> "Free tier: paying peers is switched off" to Amber
                swap.chequebook == null -> "Free tier: no chequebook" to Amber
                available != null && available < MIN_CHEQUE_PLUR -> "Free tier: the deposit is used up" to Red
                swap.paying && available != null && available < LOW_CREDIT_PLUR -> "Paying peers · credit is low" to Amber
                swap.paying -> "Paying peers · fast uploads" to Green
                else -> "Free tier for now: waiting for the deposit to be read" to Amber
            }
            Text(text, color = color, style = MaterialTheme.typography.bodyMedium)
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
                credit!!.recent.forEach { p ->
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
