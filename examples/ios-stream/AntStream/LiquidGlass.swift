import CoreImage.CIFilterBuiltins
import SwiftUI
import UniformTypeIdentifiers

/// Shared visual primitives for the Liquid Glass UI, lifted from
/// `examples/ios-drive` so AntStream looks like the rest of the family.
///
/// iOS 26's `.glassEffect(...)` is the only thing that actually does the
/// lensing / specular work; everything in this file just composes it with
/// the colorful blurred orbs that give the glass something interesting to
/// refract.

/// Multi-orb gradient backdrop. Two palettes — Broadcast (warm, for the
/// camera/live side) and Network (cool, for storage/account) — drawn
/// behind the tab content so the glass cards on top get their colour from
/// somewhere. Each orb is a `RadialGradient` over a soft circle, blurred
/// heavily to produce the diffuse colour fields iOS 26's sample apps use.
struct LiquidGlassBackground: View {
    enum Palette {
        case broadcast
        case network
    }

    let palette: Palette

    var body: some View {
        GeometryReader { geo in
            ZStack {
                Color.black.ignoresSafeArea()

                ForEach(orbs(for: geo.size).indices, id: \.self) { idx in
                    let orb = orbs(for: geo.size)[idx]
                    Circle()
                        .fill(
                            RadialGradient(
                                colors: [orb.color.opacity(0.85), orb.color.opacity(0.0)],
                                center: .center,
                                startRadius: 0,
                                endRadius: orb.radius
                            )
                        )
                        .frame(width: orb.radius * 2, height: orb.radius * 2)
                        .position(orb.center)
                        .blur(radius: orb.blur)
                }
            }
            .ignoresSafeArea()
        }
    }

    private struct Orb {
        let color: Color
        let center: CGPoint
        let radius: CGFloat
        let blur: CGFloat
    }

    private func orbs(for size: CGSize) -> [Orb] {
        let w = size.width
        let h = size.height
        switch palette {
        case .broadcast:
            return [
                Orb(
                    color: Color(red: 0.95, green: 0.45, blue: 0.20),
                    center: CGPoint(x: w * 0.10, y: h * 0.05),
                    radius: max(w, h) * 0.55,
                    blur: 60
                ),
                Orb(
                    color: Color(red: 0.85, green: 0.20, blue: 0.40),
                    center: CGPoint(x: w * 0.95, y: h * 0.18),
                    radius: max(w, h) * 0.40,
                    blur: 70
                ),
                Orb(
                    color: Color(red: 0.50, green: 0.30, blue: 0.95),
                    center: CGPoint(x: w * 0.20, y: h * 0.55),
                    radius: max(w, h) * 0.55,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.95, green: 0.55, blue: 0.65),
                    center: CGPoint(x: w * 0.90, y: h * 0.78),
                    radius: max(w, h) * 0.45,
                    blur: 80
                ),
            ]
        case .network:
            return [
                Orb(
                    color: Color(red: 0.20, green: 0.55, blue: 0.95),
                    center: CGPoint(x: w * 0.15, y: h * 0.10),
                    radius: max(w, h) * 0.45,
                    blur: 70
                ),
                Orb(
                    color: Color(red: 0.30, green: 0.85, blue: 0.55),
                    center: CGPoint(x: w * 0.10, y: h * 0.65),
                    radius: max(w, h) * 0.55,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.95, green: 0.55, blue: 0.20),
                    center: CGPoint(x: w * 0.95, y: h * 0.85),
                    radius: max(w, h) * 0.40,
                    blur: 80
                ),
                Orb(
                    color: Color(red: 0.55, green: 0.40, blue: 0.90),
                    center: CGPoint(x: w * 0.85, y: h * 0.30),
                    radius: max(w, h) * 0.40,
                    blur: 80
                ),
            ]
        }
    }
}

/// Helper: render `content` inside a rounded glass card. Default radius
/// is 28 to match the iOS 26 sample apps' card shape.
struct GlassCard<Content: View>: View {
    var radius: CGFloat = 28
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
            .glassEffect(.regular, in: .rect(cornerRadius: radius))
    }
}

/// The pill-shaped action button used across the Storage and Broadcast
/// tabs. Factored out of `StorageView` so both tabs stay consistent.
struct GlassPillButton: View {
    let title: String
    let icon: String
    var tint: Color = .white.opacity(0.12)
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 16).padding(.vertical, 10)
                .glassEffect(.regular.tint(tint), in: .capsule)
        }
        .buttonStyle(.plain)
    }
}

/// The transient toast the tabs use for "copied" / "connected" style
/// feedback. Pair with ``BannerState``.
struct BannerView: View {
    let message: String?

    var body: some View {
        if let message {
            Text(message)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.white)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 18).padding(.vertical, 12)
                .glassEffect(.regular.tint(.black.opacity(0.4)), in: .capsule)
                .padding(.top, 8).padding(.horizontal, 24)
                .transition(.move(edge: .top).combined(with: .opacity))
        }
    }
}

/// Drives ``BannerView``: shows a message and clears it after 3 s. Each
/// `flash` supersedes the previous one, so a rapid pair of messages
/// doesn't leave the first one's timer clearing the second.
@MainActor
final class BannerState: ObservableObject {
    @Published private(set) var message: String?
    private var clearTask: Task<Void, Never>?

    func flash(_ message: String) {
        clearTask?.cancel()
        withAnimation { self.message = message }
        clearTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            guard !Task.isCancelled else { return }
            withAnimation { self?.message = nil }
        }
    }
}

/// A QR code for a payment request, rendered on a white tile so any
/// wallet camera can pick it up against the dark glass UI. With an
/// `xdaiAmount` it encodes an EIP-681 URI
/// (`ethereum:<address>@100?value=<wei>`) so a scanning wallet pre-fills
/// the recipient, the amount, and the Gnosis chain; without one it falls
/// back to the plain `0x…` address. The address is always shown as
/// copyable text next to the code for wallets that don't parse the URI.
struct AddressQRCode: View {
    let address: String
    /// Decimal xDAI amount to request (e.g. "0.15"); nil for address-only.
    var xdaiAmount: String? = nil
    var size: CGFloat = 160

    var body: some View {
        if let image = Self.generate(payload) {
            Image(uiImage: image)
                .interpolation(.none)
                .resizable()
                .scaledToFit()
                .frame(width: size, height: size)
                .padding(10)
                .background(.white, in: .rect(cornerRadius: 12))
        }
    }

    private var payload: String {
        guard let xdaiAmount, let wei = Self.weiString(fromDecimal: xdaiAmount) else {
            return address
        }
        // 100 is the Gnosis chain id; `value` is the native-token amount
        // in wei, per EIP-681.
        return "ethereum:\(address)@100?value=\(wei)"
    }

    /// Convert a decimal token amount ("0.15") to an exact integer wei
    /// string (18 decimals) without floating-point round-off. Returns nil
    /// for malformed input or a zero amount (a zero-value request would
    /// just confuse the wallet).
    static func weiString(fromDecimal s: String) -> String? {
        let parts = s.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count <= 2, !parts.isEmpty else { return nil }
        let whole = parts[0].isEmpty ? "0" : String(parts[0])
        var frac = parts.count == 2 ? String(parts[1]) : ""
        guard whole.allSatisfy(\.isNumber), frac.allSatisfy(\.isNumber),
              frac.count <= 18 else { return nil }
        frac += String(repeating: "0", count: 18 - frac.count)
        let combined = (whole + frac).drop { $0 == "0" }
        return combined.isEmpty ? nil : String(combined)
    }

    private static func generate(_ text: String) -> UIImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(text.utf8)
        filter.correctionLevel = "M"
        guard let output = filter.outputImage else { return nil }
        // Scale the tiny module grid up before rasterising so the PNG is
        // crisp; `.interpolation(.none)` keeps the edges sharp on screen.
        let scaled = output.transformed(by: CGAffineTransform(scaleX: 10, y: 10))
        guard let cg = CIContext().createCGImage(scaled, from: scaled.extent) else { return nil }
        return UIImage(cgImage: cg)
    }
}

/// Format a byte count using Apple's `.file` style. Wrapped here so every
/// call site is consistent.
func formatBytes(_ count: UInt64) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(min(count, UInt64(Int64.max))), countStyle: .file)
}

/// Put a secret on the pasteboard with a short expiry so a copied account
/// key doesn't sit in the clipboard (and sync to every other Apple device
/// through Universal Clipboard) indefinitely.
func copySecretToPasteboard(_ secret: String, expiresIn seconds: TimeInterval = 60) {
    UIPasteboard.general.setItems(
        [[UTType.plainText.identifier: secret]],
        options: [
            .localOnly: true,
            .expirationDate: Date().addingTimeInterval(seconds),
        ]
    )
}
