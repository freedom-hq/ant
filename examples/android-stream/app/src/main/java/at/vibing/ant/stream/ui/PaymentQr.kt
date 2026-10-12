package at.vibing.ant.stream.ui

import android.graphics.Bitmap
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.FilterQuality
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel
import java.math.BigDecimal
import java.math.RoundingMode

/**
 * QR code for funding the account, as the iOS app draws it: with an
 * amount it is an EIP-681 payment request (`ethereum:<addr>@100?value=<wei>`,
 * 100 being the Gnosis chain id), so a wallet that scans it fills in the
 * recipient, the network and the amount; without one it is the plain
 * address.
 */
@Composable
fun PaymentQr(address: String, xdaiAmount: String? = null, size: Dp = 200.dp) {
    val payload = paymentUri(address, xdaiAmount)
    val bitmap = remember(payload) { qrBitmap(payload) }
    Box(
        Modifier
            .background(Color.White, RoundedCornerShape(12.dp))
            .padding(10.dp),
    ) {
        Image(
            bitmap = bitmap.asImageBitmap(),
            contentDescription = "QR code for $address",
            filterQuality = FilterQuality.None,
            modifier = Modifier.size(size),
        )
    }
}

fun paymentUri(address: String, xdaiAmount: String?): String {
    val wei = xdaiAmount?.let { weiFromDecimal(it) } ?: return address
    return "ethereum:$address@100?value=$wei"
}

/** Exact decimal xDAI → integer wei (18 decimals); null for zero or garbage. */
fun weiFromDecimal(amount: String): String? = runCatching {
    val wei = BigDecimal(amount.trim()).movePointRight(18).setScale(0, RoundingMode.UP)
    if (wei.signum() <= 0) null else wei.toPlainString()
}.getOrNull()

/**
 * The amount to ask for: the quote rounded *up* to whole cents, so a
 * payment of the displayed figure is never a fraction short (iOS
 * `PriceOracle.roundedUpXdai`).
 */
fun roundedUpXdai(xdai: String): String = runCatching {
    BigDecimal(xdai.trim()).setScale(2, RoundingMode.UP).toPlainString()
}.getOrDefault(xdai)

private fun qrBitmap(text: String): Bitmap {
    val matrix = QRCodeWriter().encode(
        text,
        BarcodeFormat.QR_CODE,
        0,
        0,
        mapOf(EncodeHintType.ERROR_CORRECTION to ErrorCorrectionLevel.M, EncodeHintType.MARGIN to 0),
    )
    val bmp = Bitmap.createBitmap(matrix.width, matrix.height, Bitmap.Config.ARGB_8888)
    for (y in 0 until matrix.height) {
        for (x in 0 until matrix.width) {
            bmp.setPixel(x, y, if (matrix[x, y]) android.graphics.Color.BLACK else android.graphics.Color.WHITE)
        }
    }
    return bmp
}
