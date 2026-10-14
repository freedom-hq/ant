import SwiftUI

/// The Broadcast tab. Today it is the app's readiness surface: it walks a
/// fresh install from "nothing" to "ready to broadcast" — funded account,
/// deployed chequebook, live storage plan, gateway listening — without
/// the user ever touching `antctl`.
///
/// The camera capture pipeline (#65) and the publish loop (#67 stage 2)
/// hang off the **Go live** button below, which opens ``LiveView``;
/// everything they need (a light-mode gateway on `AntNode.gatewayURL`, a
/// stamped batch, working settlement) is what this checklist guarantees.
struct BroadcastView: View {
    @EnvironmentObject var node: AntNode
    /// Opens a channel on the Watch tab (the channel card's Watch button).
    private let onWatch: (String) -> Void
    @StateObject private var banner = BannerState()
    @State private var showGetStarted = false
    /// Opened by the **Run bench** button — and, on launch, by
    /// `-antstreamShowBench YES`, which iOS folds into `UserDefaults`.
    /// That is how `antstream-visual` (issue #70) gets a screenshot of
    /// the bench sheet: the simulator run has no tap driver, and a
    /// screenshot of the button alone is not evidence that the sheet
    /// behind it renders.
    @State private var showBench = UserDefaults.standard.bool(forKey: "antstreamShowBench")
    /// The going-live screen. Full-screen rather than a sheet: a
    /// broadcast owns the camera and the screen for its duration, and a
    /// swipe-dismissable sheet would orphan a run in progress.
    @State private var showLive = false

    init(onWatch: @escaping (String) -> Void = { _ in }) {
        self.onWatch = onWatch
    }

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .broadcast)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    liveCard
                    if node.hasStorage { channelCard }
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
        .fullScreenCover(isPresented: $showLive) { LiveView() }
        .task {
            // antstream-visual: `-antstream-shot-channel` shows the "Your
            // channel" card, which needs a plan (a fresh runner account
            // has none) and a channel link; sample state supplies both.
            if RootView.shotArgs.contains("-antstream-shot-channel") {
                for _ in 0..<60 where !node.status.isReady {
                    try? await Task.sleep(nanoseconds: 1_000_000_000)
                }
                node.installChannelSample()
                return
            }
            await node.refreshAll()
        }
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
                     ? "Your account, storage plan and network settlement are all set. Segments publish straight to Swarm and the channel's feed follows the live edge."
                     : "Finish the steps below and this device can go live.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                    .multilineTextAlignment(.center)

                Button {
                    showLive = true
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

    // MARK: permanent channel

    /// The account's permanent channel link, shareable before the first
    /// broadcast: viewers who open it join whenever this device is live.
    private var channelCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Your channel")
                    .font(.headline)
                    .foregroundStyle(.white)
                if let ref = node.channelReference {
                    Text("One link for all your broadcasts. Share it any time: viewers who open it join whenever you're live, and see your last broadcast otherwise.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                    Button {
                        UIPasteboard.general.string = ref
                        banner.flash("Channel reference copied")
                    } label: {
                        HStack {
                            Text(ref)
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
                    HStack(spacing: 10) {
                        ShareLink(item: Channel.watchLinks(ref)) {
                            Label("Share", systemImage: "square.and.arrow.up")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.white)
                                .padding(.horizontal, 16).padding(.vertical, 10)
                                .glassEffect(.regular.tint(.blue.opacity(0.35)), in: .capsule)
                        }
                        .buttonStyle(.plain)
                        GlassPillButton(title: "Watch", icon: "play.fill",
                                        tint: .white.opacity(0.18)) {
                            onWatch(ref)
                        }
                    }
                } else {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Creating your channel link…")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }
            }
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
                            ? (node.peerCount >= AntNode.minPeersToBroadcast
                                ? "\(node.peerCount) peers"
                                : "\(node.peerCount) of \(AntNode.minPeersToBroadcast) peers…")
                            : node.status.label)
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
                    detail: gatewayDetail
                )
            }
        }
    }

    /// Where the gateway listens, or why it doesn't: the bind error stays
    /// on screen while the node retries every 10 s.
    private var gatewayDetail: String {
        if node.gatewayUp { return node.gatewayURL?.absoluteString ?? node.gatewayAddress }
        if let error = node.gatewayError { return "Not running: \(error) (retrying)" }
        return "Starting…"
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
