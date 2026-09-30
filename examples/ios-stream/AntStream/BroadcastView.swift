import SwiftUI

/// The Broadcast tab. Today it is the app's readiness surface: it walks a
/// fresh install from "nothing" to "ready to broadcast" — funded account,
/// deployed chequebook, live storage plan, gateway listening — without
/// the user ever touching `antctl`.
///
/// The camera capture pipeline (#65) and the publish loop (#67) attach to
/// the **Go live** button below; everything they need (a light-mode
/// gateway on `AntNode.gatewayURL`, a stamped batch, working settlement)
/// is what this checklist guarantees.
struct BroadcastView: View {
    @EnvironmentObject var node: AntNode
    @StateObject private var banner = BannerState()

    @State private var showGetStarted = false
    /// Opened by the **Run bench** button — and, on launch, by
    /// `-antstreamShowBench YES`, which iOS folds into `UserDefaults`.
    /// That is how `antstream-visual` (issue #70) gets a screenshot of
    /// the bench sheet: the simulator run has no tap driver, and a
    /// screenshot of the button alone is not evidence that the sheet
    /// behind it renders.
    @State private var showBench = UserDefaults.standard.bool(forKey: "antstreamShowBench")

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .broadcast)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    liveCard
                    checklistCard
                    benchCard
                    if !node.hasStorage { setUpCard }
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 130)
            }
            .scrollIndicators(.hidden)
            .refreshable { await node.refreshAll() }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .sheet(isPresented: $showGetStarted) { GetStartedView() }
        .sheet(isPresented: $showBench) { BenchView() }
        .task { await node.refreshAll() }
    }

    // MARK: header

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Broadcast")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text(node.isOffline ? "Waiting for network…" : node.status.label)
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
        }
        .padding(.horizontal, 6)
    }

    // MARK: go live

    private var liveCard: some View {
        GlassCard {
            VStack(spacing: 16) {
                Image(systemName: node.isReadyToBroadcast
                      ? "dot.radiowaves.left.and.right" : "video.slash")
                    .font(.system(size: 46))
                    .foregroundStyle(node.isReadyToBroadcast ? .white : .white.opacity(0.5))
                Text(node.isReadyToBroadcast ? "Ready to broadcast" : "Not ready yet")
                    .font(.system(.title2, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text(node.isReadyToBroadcast
                     ? "Your account, storage plan and network settlement are all set. Camera capture arrives in the next release."
                     : "Finish the steps below and this device can go live.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                    .multilineTextAlignment(.center)

                Button {
                    // Capture + publish land in the follow-up tickets;
                    // until then the button reports the state the
                    // pipeline will start from.
                    banner.flash("Camera capture lands in the next release")
                } label: {
                    Text("Go live")
                        .font(.headline)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .glassEffect(
                            .regular.tint(.red.opacity(node.isReadyToBroadcast ? 0.6 : 0.2)),
                            in: .capsule
                        )
                        .foregroundStyle(.white.opacity(node.isReadyToBroadcast ? 1 : 0.5))
                }
                .buttonStyle(.plain)
                .disabled(!node.isReadyToBroadcast)
            }
            .frame(maxWidth: .infinity)
        }
    }

    // MARK: readiness checklist

    private var checklistCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                Text("BEFORE YOU GO LIVE")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))

                checkRow(
                    done: node.isOnNetwork,
                    title: "Connected to the network",
                    detail: node.isOffline
                        ? "Waiting for network…"
                        : (node.status.isReady
                            ? "\(node.peerCount) peers" : node.status.label)
                )
                checkRow(
                    done: node.keyProtection != nil,
                    title: "Account key secured",
                    detail: node.keyProtection?.label ?? "Creating…"
                )
                checkRow(
                    done: node.hasStorage,
                    title: "Storage plan active",
                    detail: node.plan.map { formatBytes($0.freeBytes) + " free" }
                        ?? "No plan yet"
                )
                checkRow(
                    done: node.settlement?.enabled == true,
                    title: "Network settlement ready",
                    detail: node.settlement?.enabled == true
                        ? "Chequebook deployed" : "Needs a little xDAI for gas"
                )
                checkRow(
                    done: node.gatewayUp,
                    title: "Publishing endpoint up",
                    detail: node.gatewayUp
                        ? (node.gatewayURL?.absoluteString ?? AntNode.gatewayAddress)
                        : "Starting…"
                )
            }
        }
    }

    private func checkRow(done: Bool, title: String, detail: String) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: done ? "checkmark.circle.fill" : "circle")
                .font(.title3)
                .foregroundStyle(done ? .green : .white.opacity(0.4))
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white)
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
            Spacer()
        }
    }

    // MARK: throughput bench (#67 stage 1)

    /// The go/no-go instrument. It answers "which rendition can this
    /// phone, on this network, actually broadcast?" — the question that
    /// gates the whole publisher track — so it lives on the Broadcast
    /// tab next to the checklist it depends on, not behind a debug menu.
    private var benchCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Throughput bench")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text(node.isReadyToBroadcast
                     ? "Publishes synthetic segments through this device's node to measure what it can sustain on \(node.networkLabel)."
                     : "Measures the on-device part of publishing. Finish the checklist above to measure the network too.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                GlassPillButton(title: "Run bench", icon: "gauge.with.needle",
                                tint: .white.opacity(0.18)) {
                    showBench = true
                }
            }
        }
    }

    // MARK: first-run set-up

    private var setUpCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Set up storage")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Live video is stored on Swarm as it's captured, so this device needs a storage plan before it can broadcast.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                GlassPillButton(title: "Get started", icon: "sparkles",
                                tint: .blue.opacity(0.35)) {
                    showGetStarted = true
                }
            }
        }
    }
}
