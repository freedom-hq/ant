import SwiftUI

/// Below this the chequebook can't cover another cheque (bee's minimum,
/// ~9e9 PLUR).
private let minChequePlur = Decimal(9_000_000_000)
/// "Low" once less than 0.0005 xBZZ is left.
private let lowCreditPlur = Decimal(5_000_000_000_000)

/// The chequebook, live: what it can still pay, whether the node is
/// paying peers (fast) or on the free tier, what this run has paid and
/// how fast, and the latest payments. Fed by `AntNode.credit`, which
/// refreshes every 2 s from memory.
struct CreditCard: View {
    let credit: Credit?

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
            }
        }
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

/// One line for the live screen: credit left and the pace of payments.
func creditSummary(_ credit: Credit?) -> String? {
    guard let swap = credit?.swap, let available = swap.availablePlur else { return nil }
    guard swap.paying else { return "Free tier · \(formatBzz(available)) left" }
    let pace = credit?.perMinutePlur.map { " · \(formatBzz($0))/min" } ?? ""
    return "Paying · \(formatBzz(available)) left\(pace)"
}
