import SwiftUI

/// Below this the chequebook can't cover another cheque (bee's minimum,
/// ~9e9 PLUR).
private let minChequePlur = Decimal(9_000_000_000)
/// "Low" once less than 0.0005 xBZZ is left.
private let lowCreditPlur = Decimal(5_000_000_000_000)
/// How often the wallet is re-read while the card is on screen.
private let walletRefreshNanos: UInt64 = 30_000_000_000

/// Which way xBZZ moves between the account wallet and the chequebook.
enum CreditMove: String, Identifiable {
    case add, withdraw

    var id: String { rawValue }
    var title: String { self == .add ? "Add to credit" : "Withdraw to account" }
    var verb: String { self == .add ? "Add" : "Withdraw" }
    var busy: String { self == .add ? "Adding" : "Withdrawing" }
}

/// The chequebook, live: what it can still pay, whether the node is
/// paying peers (fast) or on the free tier, what this run has paid and
/// how fast, and the latest payments. Fed by `AntNode.credit`, which
/// refreshes every 2 s from memory.
///
/// Below that, the account wallet the chequebook is funded from (re-read
/// every 30 s while the card is on screen, and after every action), with
/// "Add to credit" (wallet → chequebook) and "Withdraw to account"
/// (chequebook → wallet). Both send a real Gnosis transaction, so each
/// asks for an amount and then for confirmation first.
struct CreditCard: View {
    @EnvironmentObject var node: AntNode
    let credit: Credit?
    /// The Gnosis RPC the wallet reads and the transactions go through.
    let rpc: String
    /// The Storage tab's banner.
    let flash: (String) -> Void

    @State private var moving: CreditMove?
    @State private var busy: String?
    @State private var error: String?
    @State private var done: String?

    var body: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("CREDIT")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                headline
                if let line = statusLine {
                    Text(line.text)
                        .font(.subheadline)
                        .foregroundStyle(line.color)
                }
                if usedUp {
                    Text("Uploads are on the slow free tier until the chequebook has credit again. Add xBZZ from your account wallet below.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                }
                if let credit, let issued = credit.swap.issuedPlur {
                    VStack(spacing: 6) {
                        figure("Paid this session", formatBzz(credit.sessionPlur))
                        figure("Paying now", credit.perMinutePlur.map { "\(formatBzz($0)) / min" } ?? "–")
                        figure("Paid in total", formatBzz(issued))
                        if let deposited = credit.swap.depositedPlur {
                            figure("Deposited", formatBzz(deposited))
                        }
                    }
                }
                if let recent = credit?.recent, !recent.isEmpty {
                    Text("Latest payments")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.5))
                    TimelineView(.periodic(from: .now, by: 1)) { context in
                        VStack(spacing: 4) {
                            ForEach(recent) { p in
                                HStack {
                                    Text("\(p.peer.prefix(6))…\(p.peer.suffix(4))")
                                        .font(.caption.monospaced())
                                        .foregroundStyle(.white.opacity(0.8))
                                    Spacer()
                                    Text("\(formatBzz(p.plur)) · \(ago(context.date.timeIntervalSince(p.at)))")
                                        .font(.caption)
                                        .foregroundStyle(.white.opacity(0.6))
                                }
                            }
                        }
                    }
                }
                Divider().overlay(.white.opacity(0.2))
                walletSection
            }
        }
        .task {
            // antstream-visual: open the action sheets on the sample
            // state (see `AntNode.installCreditSample`); no tap driver.
            if RootView.shotArgs.contains("-antstream-shot-credit-add") {
                moving = .add
            } else if RootView.shotArgs.contains("-antstream-shot-credit-withdraw") {
                moving = .withdraw
            }
            while !Task.isCancelled {
                await node.refreshWallet(rpc: rpc)
                try? await Task.sleep(nanoseconds: walletRefreshNanos)
            }
        }
        .sheet(item: $moving) { move in
            CreditMoveSheet(move: move, max: max(for: move), wallet: node.wallet) { amount in
                moving = nil
                run(move, amount)
            }
        }
    }

    // MARK: wallet + actions

    @ViewBuilder private var walletSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("ACCOUNT WALLET")
                .font(.system(.caption2, design: .rounded).weight(.semibold))
                .tracking(1.5)
                .foregroundStyle(.white.opacity(0.5))
            figure("xBZZ", node.wallet.map { formatBzz($0.bzzPlur) } ?? "–")
            figure("xDAI for gas", node.wallet.map { formatXdai($0.xdaiWei) } ?? "–")
            if let walletError = node.walletError {
                Text("Couldn't read the wallet: \(walletError)")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
            if let w = node.wallet, w.xdaiWei == 0 {
                Text("Moving xBZZ costs a little xDAI for gas. Send some xDAI to your account first.")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
        }
        HStack(spacing: 10) {
            actionButton(.add, icon: "arrow.down.to.line", enabled: hasGas && (node.wallet?.bzzPlur ?? 0) > 0)
            actionButton(.withdraw, icon: "arrow.up.to.line", enabled: hasGas && (credit?.swap.availablePlur ?? 0) > 0)
        }
        if let busy {
            HStack(spacing: 8) {
                ProgressView().tint(.white)
                Text(busy)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.7))
            }
        }
        if let error {
            Text(error).font(.caption).foregroundStyle(.red)
        }
        if let done {
            Text(done).font(.caption).foregroundStyle(.green)
        }
    }

    private var hasGas: Bool { (node.wallet?.xdaiWei ?? 0) > 0 }

    private func actionButton(_ move: CreditMove, icon: String, enabled: Bool) -> some View {
        GlassPillButton(title: move.title, icon: icon) {
            error = nil
            done = nil
            moving = move
        }
        .disabled(!enabled || busy != nil)
        .opacity(enabled && busy == nil ? 1 : 0.4)
    }

    /// The most a move can take: the wallet's xBZZ to add, the free
    /// credit to withdraw (the node re-checks it against the chain).
    private func max(for move: CreditMove) -> Decimal {
        switch move {
        case .add: return node.wallet?.bzzPlur ?? 0
        case .withdraw: return credit?.swap.availablePlur ?? 0
        }
    }

    private func run(_ move: CreditMove, _ plur: Decimal) {
        busy = "\(move.busy) \(formatBzz(plur))… waiting for the transaction to confirm"
        Task {
            defer { busy = nil }
            do {
                switch move {
                case .add: try await node.addCredit(rpc: rpc, plur: plur)
                case .withdraw: try await node.withdrawCredit(rpc: rpc, plur: plur)
                }
                let message = move == .add
                    ? "Added \(formatBzz(plur)) to credit"
                    : "Withdrew \(formatBzz(plur)) to your account"
                done = message
                flash(message)
            } catch {
                self.error = error.localizedDescription
            }
        }
    }

    // MARK: figures

    private var usedUp: Bool {
        guard let swap = credit?.swap, swap.swapEnabled, swap.chequebook != nil,
              let available = swap.availablePlur else { return false }
        return available < minChequePlur
    }

    @ViewBuilder private var headline: some View {
        let swap = credit?.swap
        if swap == nil {
            Text("Checking…").foregroundStyle(.white.opacity(0.6))
        } else if swap?.chequebook == nil {
            Text("No chequebook yet").font(.title3.weight(.semibold)).foregroundStyle(.white)
        } else if let available = swap?.availablePlur {
            Text(formatBzz(available))
                .font(.system(.title, design: .rounded).weight(.bold))
                .foregroundStyle(.white)
                .contentTransition(.numericText())
        } else {
            Text("Reading the deposit…").font(.title3.weight(.semibold)).foregroundStyle(.white)
        }
    }

    private var statusLine: (text: String, color: Color)? {
        guard let swap = credit?.swap else { return nil }
        let available = swap.availablePlur
        if !swap.swapEnabled { return ("Free tier: paying peers is switched off", .orange) }
        if swap.chequebook == nil { return ("Free tier: no chequebook", .orange) }
        if let available, available < minChequePlur { return ("Free tier: the deposit is used up", .red) }
        if swap.paying, let available, available < lowCreditPlur { return ("Paying peers · credit is low", .orange) }
        if swap.paying { return ("Paying peers · fast uploads", .green) }
        return ("Free tier for now: waiting for the deposit to be read", .orange)
    }

    private func figure(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).font(.subheadline).foregroundStyle(.white.opacity(0.6))
            Spacer()
            Text(value).font(.subheadline.monospacedDigit()).foregroundStyle(.white)
        }
    }

    private func ago(_ seconds: TimeInterval) -> String {
        if seconds < 5 { return "now" }
        if seconds < 60 { return "\(Int(seconds)) s ago" }
        return "\(Int(seconds / 60)) min ago"
    }
}

/// Pick how much xBZZ to move (an amount field and quick picks, checked
/// against `max`), then confirm the real transaction.
private struct CreditMoveSheet: View {
    let move: CreditMove
    let max: Decimal
    let wallet: WalletBalance?
    /// Called with the confirmed amount, in PLUR.
    let onConfirm: (Decimal) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var text = ""
    @State private var confirming = false

    private static let quickPicks = ["0.1", "0.5", "1"]

    private var amount: Decimal? { parseBzzAmount(text) }

    private var problem: String? {
        if text.trimmingCharacters(in: .whitespaces).isEmpty { return nil }
        guard let amount else { return "Enter an amount in xBZZ, like 0.5" }
        if amount > max { return "That's more than the \(formatBzz(max)) available" }
        return nil
    }

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .network).ignoresSafeArea()
                ScrollView {
                    VStack(alignment: .leading, spacing: 16) {
                        Text(explanation)
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.75))
                        GlassCard {
                            VStack(alignment: .leading, spacing: 10) {
                                HStack {
                                    TextField("0.0", text: $text)
                                        .keyboardType(.decimalPad)
                                        .font(.system(.title, design: .rounded).weight(.bold))
                                        .foregroundStyle(.white)
                                    Text("xBZZ")
                                        .font(.headline)
                                        .foregroundStyle(.white.opacity(0.6))
                                }
                                HStack(spacing: 8) {
                                    ForEach(Self.quickPicks, id: \.self) { pick in
                                        quickPick(pick, enabled: (parseBzzAmount(pick) ?? 0) <= max)
                                    }
                                    let all = bzzInput(max)
                                    quickPick("All", value: all, enabled: parseBzzAmount(all) != nil)
                                }
                                if let problem {
                                    Text(problem).font(.caption).foregroundStyle(.orange)
                                }
                            }
                        }
                        if let wallet {
                            Text("Gas is paid in xDAI: your account has \(formatXdai(wallet.xdaiWei)).")
                                .font(.caption)
                                .foregroundStyle(.white.opacity(0.6))
                        }
                        GlassPillButton(title: "Continue", icon: "arrow.right.circle") {
                            confirming = true
                        }
                        .disabled(amount == nil || problem != nil)
                        .opacity(amount == nil || problem != nil ? 0.4 : 1)
                    }
                    .padding(20)
                }
            }
            .navigationTitle(move.title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Close") { dismiss() }
                }
            }
            .alert(confirmTitle, isPresented: $confirming) {
                Button(move.verb) {
                    if let amount { onConfirm(amount) }
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text(confirmMessage)
            }
        }
        .preferredColorScheme(.dark)
        .presentationDetents([.medium, .large])
        .task {
            // antstream-visual: a sample amount, and with
            // `-antstream-shot-credit-confirm` the confirmation itself.
            guard RootView.shotArgs.contains("-antstream-shot-credit") else { return }
            text = move == .add ? "0.5" : bzzInput(max)
            if RootView.shotArgs.contains("-antstream-shot-credit-confirm") {
                try? await Task.sleep(nanoseconds: 800_000_000)
                confirming = true
            }
        }
    }

    private var explanation: String {
        switch move {
        case .add:
            return "Moves xBZZ from your account wallet (\(formatBzz(max))) into the chequebook that pays peers for your uploads."
        case .withdraw:
            return "Moves xBZZ from the chequebook back to your account wallet. Up to \(formatBzz(max)): the rest backs payments already sent to peers."
        }
    }

    private var confirmTitle: String {
        "\(move.verb) \(amount.map(formatBzz) ?? "")?"
    }

    private var confirmMessage: String {
        let what = amount.map(formatBzz) ?? ""
        let route = move == .add
            ? "\(what) moves from your account wallet into the chequebook."
            : "\(what) moves from the chequebook back to your account wallet."
        let gas = wallet.map { " (your account has \(formatXdai($0.xdaiWei)))" } ?? ""
        return "This sends a real transaction on Gnosis Chain. \(route) It costs a little xDAI for gas\(gas), and can't be undone once sent."
    }

    private func quickPick(_ label: String, value: String? = nil, enabled: Bool) -> some View {
        Button {
            text = value ?? label
        } label: {
            Text(label)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.white)
                .padding(.horizontal, 14).padding(.vertical, 8)
                .glassEffect(.regular.tint(.white.opacity(0.12)), in: .capsule)
                .contentShape(.capsule)
        }
        .buttonStyle(.plain)
        .disabled(!enabled)
        .opacity(enabled ? 1 : 0.35)
    }
}

/// One line for the live screen: credit left and the pace of payments.
func creditSummary(_ credit: Credit?) -> String? {
    guard let swap = credit?.swap, let available = swap.availablePlur else { return nil }
    guard swap.paying else { return "Free tier · \(formatBzz(available)) left" }
    let pace = credit?.perMinutePlur.map { " · \(formatBzz($0))/min" } ?? ""
    return "Paying · \(formatBzz(available)) left\(pace)"
}
