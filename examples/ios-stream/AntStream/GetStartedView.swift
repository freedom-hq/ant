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
    /// The payment step's ⓘ sheet: everything a payer may need beyond
    /// the amount and the code.
    @State private var showPaymentHelp = false
    @StateObject private var banner = BannerState()

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
                        Button {
                            withAnimation { step = .plan; quote = nil; didAutoActivate = false }
                        } label: {
                            Image(systemName: "chevron.left")
                        }
                        .accessibilityLabel("Back")
                    } else if step == .plan {
                        Button("Close") { dismiss() }
                    }
                }
                ToolbarItem(placement: .topBarTrailing) {
                    if step == .payment {
                        Button { showPaymentHelp = true } label: {
                            Image(systemName: "info.circle")
                        }
                        .accessibilityLabel("How to pay")
                    }
                }
            }
            .overlay(alignment: .top) { BannerView(message: banner.message) }
            .sheet(isPresented: $showPaymentHelp) { paymentHelp }
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
                // `-antstream-shot-getstarted-help` also opens the ⓘ sheet,
                // where the deposit line and the Gnosis-only warning live.
                if RootView.shotArgs.contains("-antstream-shot-getstarted-help") {
                    for _ in 0..<30 where node.account == nil {
                        try? await Task.sleep(nanoseconds: 1_000_000_000)
                    }
                    showPaymentHelp = true
                }
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

    /// Kept to what a payer needs, like the Android app's payment step
    /// and what payment processors (Coinbase Commerce, BitPay) show: the
    /// exact amount, the network, a QR code a wallet fills everything in
    /// from (address, Gnosis, amount), the address with one-tap copy and
    /// a sign that we're waiting. The plan activates by itself once the
    /// funds land. Everything else (the full address, copy buttons, the
    /// Gnosis-only warning, what the deposit is for) sits behind the ⓘ.
    @ViewBuilder private var paymentStep: some View {
        if let quote {
            VStack(spacing: 22) {
                Spacer(minLength: 0)

                if quote.sufficientFunds {
                    receivedState
                } else if let addr = node.account?.ethAddress {
                    let send = PriceOracle.roundedUpXdai(quote.xdaiToSendDisplay)
                    amountHeader(send)
                    AddressQRCode(address: addr, xdaiAmount: send, size: 200)
                    Button {
                        UIPasteboard.general.string = addr
                        banner.flash("Address copied")
                    } label: {
                        HStack(spacing: 10) {
                            Text("to \(addr.prefix(8))…\(addr.suffix(6))")
                                .font(.subheadline.monospaced())
                                .foregroundStyle(.white)
                            Text("Copy")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.blue)
                        }
                        .padding(.horizontal, 14).padding(.vertical, 10)
                        .background(.white.opacity(0.08), in: .rect(cornerRadius: 12))
                    }
                    .buttonStyle(.plain)
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white.opacity(0.7))
                        Text("Waiting for payment")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                } else {
                    ProgressView().tint(.white)
                }

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }

                Spacer(minLength: 0)
            }
            .frame(maxWidth: .infinity)
            .padding(24)
            .task { await pollForPayment() }
        }
    }

    /// "Send exactly" + the amount (tap to copy) + the network badge.
    private func amountHeader(_ send: String) -> some View {
        VStack(spacing: 6) {
            Text("Send exactly")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.6))
            Button {
                UIPasteboard.general.string = send
                banner.flash("Amount copied")
            } label: {
                Text("\(send) xDAI")
                    .font(.system(size: 40, weight: .semibold, design: .rounded))
                    .foregroundStyle(.white)
            }
            .buttonStyle(.plain)
            HStack(spacing: 6) {
                Circle().fill(.green).frame(width: 8, height: 8)
                Text("on Gnosis Chain")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.green)
            }
            .padding(.horizontal, 12).padding(.vertical, 5)
            .background(.green.opacity(0.15), in: .capsule)
            .padding(.top, 4)
        }
    }

    /// The funds are in. Activation normally starts by itself; this is
    /// only on screen around it, or after a failed activation, when
    /// Activate is the manual retry.
    private var receivedState: some View {
        VStack(spacing: 14) {
            Image(systemName: "checkmark.circle.fill")
                .font(.system(size: 64))
                .foregroundStyle(.green)
            Text("Received")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white.opacity(0.8))
            if error != nil {
                GlassPillButton(title: "Activate", icon: "bolt.fill",
                                tint: .blue.opacity(0.5)) {
                    activate()
                }
            }
        }
    }

    /// The ⓘ sheet.
    @ViewBuilder private var paymentHelp: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    if let quote, let addr = node.account?.ethAddress {
                        let send = PriceOracle.roundedUpXdai(quote.xdaiToSendDisplay)
                        Text("Scan the code with your wallet, or send \(send) xDAI to:")
                            .font(.subheadline)
                        Text(addr)
                            .font(.footnote.monospaced())
                            .textSelection(.enabled)
                            .padding(12)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                        HStack(spacing: 10) {
                            GlassPillButton(title: "Copy address", icon: "doc.on.doc") {
                                UIPasteboard.general.string = addr
                                banner.flash("Address copied")
                            }
                            GlassPillButton(title: "Copy amount", icon: "doc.on.doc") {
                                UIPasteboard.general.string = send
                                banner.flash("Amount copied")
                            }
                        }
                        Label("Only on Gnosis Chain (chain ID 100). Don't send from Ethereum mainnet or another network: the address is the same there, but those funds won't reach your node.",
                              systemImage: "exclamationmark.triangle.fill")
                            .font(.footnote)
                            .foregroundStyle(.orange)
                        if includesSettlementDeposit(quote) {
                            // Say what the extra buys. The price also covers
                            // a one-time deposit the node puts behind the
                            // account it pays the network from — without it
                            // publishing stalls once peers stop extending
                            // credit.
                            Text("Includes a one-time \(quote.settlementDepositBzz) xBZZ network deposit that keeps your broadcast flowing.")
                                .font(.footnote)
                                .foregroundStyle(.white.opacity(0.65))
                        }
                        Text("This screen notices the payment by itself and activates your plan; keep the app open until it does.")
                            .font(.footnote)
                            .foregroundStyle(.white.opacity(0.65))
                    } else {
                        ProgressView().frame(maxWidth: .infinity)
                    }
                }
                .padding(20)
            }
            .navigationTitle("How to pay")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { showPaymentHelp = false }
                }
            }
        }
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .presentationDetents([.medium, .large])
        .preferredColorScheme(.dark)
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
                // Immutable (#176): once a bucket fills, a broadcast's
                // uploads are refused (and the live screen says so)
                // instead of silently overwriting the oldest video, so a
                // long broadcast can't eat its own beginning.
                try await node.buyStorage(rpc: activeRpc, depth: tier.depth,
                                          amountPerChunk: quote.amountPerChunk,
                                          immutable: true)
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
