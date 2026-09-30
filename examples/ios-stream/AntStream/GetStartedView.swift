import SwiftUI

/// The Get Started flow, ported from `examples/ios-drive`: a guided sheet
/// that welcomes the user, lets them pick a storage plan, shows the
/// payment information for that plan (cost vs. their account balance +
/// where to send funds), and activates the storage on-chain.
///
/// Deliberately consumer-flavoured: "plans", not "postage batches";
/// "activate", not "createBatch". The Rust side does the heavy lifting —
/// `ant_storage_quote` prices a plan against the live chain — including
/// the one-time deposit that backs the node's cheques — and
/// `ant_storage_buy_xdai` swaps the xBZZ shortfall, buys the batch, and
/// deploys *and funds* the chequebook (`ensure_settlement`) in one call.
struct GetStartedView: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @AppStorage("gnosisRpc") private var rpc = ""

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
                        Button("Back") {
                            withAnimation { step = .plan; quote = nil; didAutoActivate = false }
                        }
                    } else if step == .plan {
                        Button("Close") { dismiss() }
                    }
                }
            }
        }
        .preferredColorScheme(.dark)
        .interactiveDismissDisabled(step == .activating)
        .task {
            // antstream-visual: `-antstream-shot-getstarted-pay` opens the
            // flow straight on the payment step with a representative
            // quote, so CI can capture the buy-flow cost breakdown — the
            // "Send xDAI" card and the new one-time settlement-deposit
            // line — without the live quote a fresh runner may not be able
            // to fetch. Live pricing (`loadPlans`) otherwise.
            if RootView.shotArgs.contains("-antstream-shot-getstarted-pay") {
                selected = StoragePlanTier.all.first
                quote = Self.screenshotSampleQuote
                step = .payment
            } else {
                await loadPlans()
            }
        }
    }

    /// A representative Starter-plan quote for the payment-step screenshot
    /// (`-antstream-shot-getstarted-pay`). Its deposit fields are non-zero,
    /// so `includesSettlementDeposit` is true and the deposit line renders
    /// — the whole point of the capture. Sample only; the real flow always
    /// prices against the live chain.
    private static let screenshotSampleQuote = StorageQuote(
        depth: 21,
        days: 30,
        amountPerChunk: "0",
        totalCostBzz: "0.0500",
        settlementDepositPlur: "10000000000000",
        settlementDepositBzz: "0.0010",
        capacityBytes: 2_000_000_000,
        accountBzzDisplay: "0.0000",
        accountXdai: "0",
        accountXdaiDisplay: "0.0000",
        neededBzzDisplay: "0.0510",
        xdaiRequiredDisplay: "0.60",
        xdaiToSendDisplay: "0.60",
        sufficientFunds: false
    )

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
                    Image(systemName: "video.badge.plus")
                        .font(.system(size: 50))
                        .foregroundStyle(.white)
                    Text("Welcome to AntStream")
                        .font(.system(.title, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                    Text("Pick a storage plan to start broadcasting live video on the decentralized network.")
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
                        Text("\(tier.capacityLabel) · \(tier.durationLabel)")
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
            let price = PriceOracle.roundedUpXdai(q.xdaiToSendDisplay)
            Text(PriceOracle.usdFromXdai(price) ?? "$\(price)")
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
        let send = PriceOracle.roundedUpXdai(quote.xdaiToSendDisplay)
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
                    // pays the network from — without it publishing stalls
                    // once peers stop extending credit.
                    Text("Includes a one-time \(quote.settlementDepositBzz) xBZZ network deposit that keeps your broadcast flowing.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                        .multilineTextAlignment(.center)
                }
            }
        }
    }

    /// Whether this price carries the one-time settlement deposit — i.e.
    /// the chequebook it deploys still needs funding. Zero once it is
    /// funded, and the note is then left off rather than claiming a
    /// charge that isn't there.
    private func includesSettlementDeposit(_ quote: StorageQuote) -> Bool {
        (UInt64(quote.settlementDepositPlur) ?? 0) > 0
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
        return r.isEmpty ? AntNode.defaultRpc : r
    }

    /// Fetch a quote for every plan so the picker can show each plan's
    /// all-in price (xDAI to send) up front. Best-effort per plan.
    private func loadPlans() async {
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = AntNode.defaultRpc }
        loadingQuotes = true
        defer { loadingQuotes = false }
        let rpcURL = activeRpc
        let stream = node
        await withTaskGroup(of: (String, StorageQuote?).self) { group in
            for tier in StoragePlanTier.all {
                group.addTask {
                    let q = try? await stream.quoteStorage(rpc: rpcURL, depth: tier.depth,
                                                           days: tier.days)
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
        if rpc.trimmingCharacters(in: .whitespaces).isEmpty { rpc = AntNode.defaultRpc }
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
                let q = try await node.quoteStorage(rpc: activeRpc, depth: tier.depth,
                                                    days: tier.days)
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
