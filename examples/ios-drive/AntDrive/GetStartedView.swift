import SwiftUI

/// The Get Started flow: a guided sheet that welcomes the user, lets
/// them pick a storage plan, shows the payment information for that plan
/// (cost vs. their account balance + where to send funds), and activates
/// the storage on-chain. Deliberately Dropbox-flavoured: "plans", not
/// "postage batches"; "activate", not "createBatch".
struct GetStartedView: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @AppStorage("gnosisRpc") private var rpc = ""

    private let defaultRpc = "https://rpc.gnosischain.com"

    private enum Step { case plan, payment, activating }

    @State private var step: Step = .plan
    @State private var selected: StoragePlanTier?
    @State private var quote: StorageQuote?
    @State private var quotes: [String: StorageQuote] = [:]
    @State private var loadingQuotes = false
    @State private var loadingQuote = false
    @State private var error: String?
    /// One-shot guard so funds-detected auto-activation fires only once per
    /// payment session (a failed activation must not re-trigger a loop).
    @State private var didAutoActivate = false

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .network).ignoresSafeArea()
                content
            }
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if step == .payment {
                        Button("Back") { withAnimation { step = .plan; quote = nil; didAutoActivate = false } }
                    } else if step == .plan {
                        Button("Close") { dismiss() }
                    }
                }
            }
        }
        .preferredColorScheme(.dark)
        .interactiveDismissDisabled(step == .activating)
        .task { await loadPlans() }
    }

    @ViewBuilder private var content: some View {
        switch step {
        case .plan: planStep
        case .payment: paymentStep
        case .activating: activatingStep
        }
    }

    // MARK: Step 1 — welcome + plan picker

    private var planStep: some View {
        ScrollView {
            VStack(spacing: 18) {
                VStack(spacing: 8) {
                    Image(systemName: "externaldrive.fill.badge.plus")
                        .font(.system(size: 50))
                        .foregroundStyle(.white)
                    Text("Welcome to AntDrive")
                        .font(.system(.title, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                    Text("Pick a storage plan to start keeping your files safe on the decentralized network.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 12)

                ForEach(StoragePlanTier.all) { tier in
                    planCard(tier)
                }

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(20)
        }
    }

    private func planCard(_ tier: StoragePlanTier) -> some View {
        Button {
            choose(tier)
        } label: {
            GlassCard {
                HStack(spacing: 14) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(tier.title)
                            .font(.title3.weight(.bold))
                            .foregroundStyle(.white)
                        Text("Up to \(formatBytes(tier.safeLimitBytes)) · \(tier.durationLabel)")
                            .font(.caption.weight(.medium))
                            .foregroundStyle(.white.opacity(0.85))
                    }
                    Spacer()
                    priceColumn(for: tier)
                }
            }
            // Make the whole card (incl. the empty spacer gap) tappable,
            // not just the text/price — the default hit area is only the
            // laid-out subviews.
            .contentShape(.rect(cornerRadius: 28))
        }
        .buttonStyle(.plain)
        .disabled(loadingQuote)
    }

    @ViewBuilder private func priceColumn(for tier: StoragePlanTier) -> some View {
        if let q = quotes[tier.id] {
            // USD only here; the payment page shows the same figure in xDAI
            // (both derived from the rounded-up xDAI-to-send, so they agree).
            let price = roundedUpXdai(q.xdaiToSendDisplay)
            Text(usdFromXdai(price) ?? "$\(price)")
                .font(.headline.weight(.bold))
                .foregroundStyle(.white)
        } else if loadingQuotes {
            ProgressView().tint(.white)
        } else {
            Image(systemName: "chevron.right").foregroundStyle(.white.opacity(0.6))
        }
    }

    // MARK: Step 2 — payment information

    /// Stripped to the essentials: how much xDAI to send, where to send it,
    /// a live waiting/received status (driven by the balance poll), and
    /// Activate. No price breakdown, no manual refresh, no prose.
    @ViewBuilder private var paymentStep: some View {
        if let quote {
            VStack(spacing: 22) {
                Spacer(minLength: 0)

                if quote.sufficientFunds {
                    Label("Payment received", systemImage: "checkmark.circle.fill")
                        .font(.title3.weight(.semibold))
                        .foregroundStyle(.green)
                } else {
                    sendCard(quote)
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Waiting for payment…")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }

                Spacer(minLength: 0)

                Button { activate() } label: {
                    Text("Activate")
                        .font(.headline)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .glassEffect(.regular.tint(.blue.opacity(quote.sufficientFunds ? 0.6 : 0.2)),
                                     in: .capsule)
                        .foregroundStyle(.white.opacity(quote.sufficientFunds ? 1 : 0.5))
                }
                .buttonStyle(.plain)
                .disabled(!quote.sufficientFunds)

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(24)
            .task { await pollForPayment() }
        }
    }

    /// The single instruction: send this much xDAI to this address (tap to
    /// copy). Everything else the user needs is already on the plan picker.
    private func sendCard(_ quote: StorageQuote) -> some View {
        let send = roundedUpXdai(quote.xdaiToSendDisplay)
        return GlassCard {
            VStack(spacing: 16) {
                VStack(spacing: 2) {
                    Text("Send")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.6))
                    Text("\(send) xDAI")
                        .font(.system(.largeTitle, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                }
                if let addr = node.account?.ethAddress {
                    AddressQRCode(address: addr, xdaiAmount: send)
                        .frame(maxWidth: .infinity)
                    Button {
                        UIPasteboard.general.string = addr
                    } label: {
                        HStack {
                            Text(addr)
                                .font(.caption.monospaced())
                                .foregroundStyle(.white)
                                .lineLimit(1).truncationMode(.middle)
                            Spacer()
                            Image(systemName: "doc.on.doc")
                                .foregroundStyle(.white.opacity(0.85))
                        }
                        .padding(12)
                        .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                    }
                    .buttonStyle(.plain)
                }
                if includesSettlementDeposit(quote) {
                    // Say what the extra buys. The price also covers a
                    // one-time deposit the node puts behind the account it
                    // pays the network from — without it uploads stall
                    // once peers stop extending credit.
                    Text("Includes a one-time \(quote.settlementDepositBzz) xBZZ network deposit that keeps your uploads flowing.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                        .multilineTextAlignment(.center)
                }
            }
        }
    }

    /// xDAI is a USD stablecoin (~$1), so its USD value is ~1:1.
    private func usdFromXdai(_ xdai: String) -> String? {
        guard let amount = Double(xdai) else { return nil }
        return PriceOracle.formatUsd(amount)
    }

    /// Round the funding amount up to the next whole cent (0.01 xDAI) so
    /// the user sends a clean figure with a hair of headroom — never less
    /// than the quote requires. The small epsilon keeps a value already on
    /// a cent boundary from being bumped a cent by float error.
    private func roundedUpXdai(_ xdai: String) -> String {
        guard let amount = Double(xdai) else { return xdai }
        let cents = (amount * 100 - 1e-6).rounded(.up)
        return String(format: "%.2f", max(0, cents) / 100)
    }

    // MARK: Step 3 — activating

    private var activatingStep: some View {
        VStack(spacing: 18) {
            ProgressView().controlSize(.large).tint(.white)
            Text("Activating your storage…")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white)
            Text("Confirming your payment on the network. This can take up to a minute — please keep the app open.")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
    }

    // MARK: actions

    private var activeRpc: String {
        let r = rpc.trimmingCharacters(in: .whitespaces)
        return r.isEmpty ? defaultRpc : r
    }

    /// Fetch a quote for every plan so the picker can show each plan's
    /// all-in price (xDAI to send) up front. Best-effort per plan.
    private func loadPlans() async {
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = defaultRpc }
        loadingQuotes = true
        defer { loadingQuotes = false }
        let rpcURL = activeRpc
        let drive = node
        await withTaskGroup(of: (String, StorageQuote?).self) { group in
            for tier in StoragePlanTier.all {
                group.addTask {
                    let q = try? await drive.quoteStorage(rpc: rpcURL, depth: tier.depth, days: tier.days)
                    return (tier.id, q)
                }
            }
            for await (id, q) in group {
                if let q { quotes[id] = q }
            }
        }
        if quotes.isEmpty {
            error = "Couldn't reach the network to price plans. Pull to retry."
        }
    }

    private func choose(_ tier: StoragePlanTier) {
        selected = tier
        error = nil
        didAutoActivate = false
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = defaultRpc }
        // Use the price already fetched for the picker when we have it;
        // otherwise fetch it now.
        if let cached = quotes[tier.id] {
            quote = cached
            withAnimation { step = .payment }
        } else {
            reprice()
        }
    }

    private func reprice() {
        guard let tier = selected else { return }
        loadingQuote = true
        error = nil
        Task {
            defer { loadingQuote = false }
            do {
                let q = try await node.quoteStorage(rpc: activeRpc, depth: tier.depth, days: tier.days)
                quote = q
                quotes[tier.id] = q
                withAnimation { step = .payment }
            } catch {
                self.error = error.localizedDescription
            }
        }
    }

    /// While the payment step is on screen and the plan is still unfunded,
    /// re-quote every few seconds. A re-quote re-reads the account's
    /// on-chain xDAI balance, so when an incoming transfer lands we activate
    /// the plan automatically — no tap needed. The `.task` driving this is
    /// cancelled when the step changes (Back / Activate), ending the loop.
    private func pollForPayment() async {
        guard let tier = selected else { return }
        while !Task.isCancelled {
            if quote?.sufficientFunds == true { autoActivateIfFunded(); return }
            try? await Task.sleep(nanoseconds: 6_000_000_000)
            if Task.isCancelled { return }
            guard let fresh = try? await node.quoteStorage(rpc: activeRpc,
                                                           depth: tier.depth,
                                                           days: tier.days)
            else { continue }
            withAnimation {
                quote = fresh
                quotes[tier.id] = fresh
            }
            if fresh.sufficientFunds { autoActivateIfFunded(); return }
        }
    }

    /// Whether this price carries the one-time settlement deposit — i.e.
    /// the chequebook it deploys still needs funding. Zero once it is
    /// funded, and the note is then left off rather than claiming a
    /// charge that isn't there.
    private func includesSettlementDeposit(_ quote: StorageQuote) -> Bool {
        (UInt64(quote.settlementDepositPlur) ?? 0) > 0
    }

    /// Kick off activation the instant funds are detected, but only once per
    /// payment session — so a failed activation (which drops back to the
    /// payment step) doesn't immediately re-trigger an infinite loop. The
    /// Activate button remains as a manual retry.
    private func autoActivateIfFunded() {
        guard !didAutoActivate else { return }
        didAutoActivate = true
        activate()
    }

    private func activate() {
        guard let tier = selected, let quote else { return }
        error = nil
        withAnimation { step = .activating }
        Task {
            do {
                try await node.buyStorage(rpc: activeRpc, depth: tier.depth,
                                          amountPerChunk: quote.amountPerChunk,
                                          immutable: false)
                if node.hasStorage {
                    dismiss()
                } else {
                    error = "Storage didn't activate — please try again."
                    withAnimation { step = .payment }
                }
            } catch {
                self.error = error.localizedDescription
                withAnimation { step = .payment }
            }
        }
    }
}
