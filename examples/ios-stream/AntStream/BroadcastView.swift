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
    /// The going-live screen opens to resume the unfinished broadcast
    /// (#175) rather than start a new one.
    @State private var resumeLive = false
    /// "End broadcast" on the unfinished card is publishing its closing
    /// window and recording.
    @State private var endingUnfinished = false
    /// What ending it said, shown on the card until it goes.
    @State private var unfinishedOutcome: String?
    @State private var confirmDiscard = false
    /// The stream info editor (#220). Opened on launch by
    /// `-antstream-shot-info-edit` for `antstream-visual`.
    @State private var showInfoEditor = false

    init(onWatch: @escaping (String) -> Void = { _ in }) {
        self.onWatch = onWatch
    }

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .broadcast)

            ScrollViewReader { proxy in
                ScrollView {
                    VStack(spacing: 18) {
                        header
                        if let unfinished = node.unfinishedBroadcast {
                            // Re-read every few seconds: "Resume" goes once
                            // the reap window has passed.
                            TimelineView(.periodic(from: .now, by: 5)) { context in
                                unfinishedCard(unfinished, now: context.date)
                            }
                        }
                        liveCard
                        if let address = node.channel?.address { channelCard(address) }
                        if node.channel != nil {
                            streamInfoCard
                                .id(Self.streamInfoID)
                        }
                        checklistCard
                            .id(Self.checklistID)
                        benchCard
                        if !node.hasStorage { setUpCard }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 12)
                    .padding(.bottom, 130)
                }
                .scrollIndicators(.hidden)
                .refreshable { await node.refreshAll() }
                .task {
                    // antstream-visual: `-antstream-shot-info` (#220) is
                    // about the stream info card, below the channel card:
                    // bring it into the shot once the sample is in.
                    // `-antstream-shot-info-announcing` is about the held
                    // Go live at the top, so it stays there.
                    if RootView.shotArgs.contains("-antstream-shot-info"),
                       !RootView.shotArgs.contains("-antstream-shot-info-edit"),
                       !RootView.shotArgs.contains("-antstream-shot-info-announcing") {
                        for _ in 0..<60 where node.streamInfo.title.isEmpty {
                            try? await Task.sleep(nanoseconds: 1_000_000_000)
                        }
                        try? await Task.sleep(nanoseconds: 500_000_000)
                        withAnimation { proxy.scrollTo(Self.streamInfoID, anchor: .top) }
                        return
                    }
                    // antstream-visual: `-antstream-shot-low-capacity` and
                    // `-antstream-shot-unusable-plan` (#176) are about the
                    // checklist's "Room for a broadcast" row, which sits
                    // below the fold: bring it into the shot once the
                    // sample plan is in.
                    guard RootView.shotArgs.contains("-antstream-shot-low-capacity")
                        || RootView.shotArgs.contains("-antstream-shot-unusable-plan")
                    else { return }
                    for _ in 0..<60 where node.plan == nil {
                        try? await Task.sleep(nanoseconds: 1_000_000_000)
                    }
                    try? await Task.sleep(nanoseconds: 500_000_000)
                    withAnimation { proxy.scrollTo(Self.checklistID, anchor: .top) }
                }
            }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .sheet(isPresented: $showGetStarted) { GetStartedView() }
        .sheet(isPresented: $showBench) { BenchView() }
        .sheet(isPresented: $showInfoEditor) {
            StreamInfoEditor(live: node.broadcastRunning, channelName: channelName)
        }
        .fullScreenCover(isPresented: $showLive, onDismiss: {
            resumeLive = false
            node.refreshUnfinishedBroadcast()
        }) { LiveView(resume: resumeLive) }
        .task {
            // antstream-visual: `-antstream-shot-info` shows the stream
            // info card filled in (#220), `-antstream-shot-info-edit` its
            // editor. A fresh runner account has no stream info, so sample
            // info stands in (nothing saved or uploaded).
            if RootView.shotArgs.contains("-antstream-shot-info") {
                for _ in 0..<60 where !node.status.isReady || node.channel == nil {
                    try? await Task.sleep(nanoseconds: 1_000_000_000)
                }
                node.installStreamInfoSample()
                if RootView.shotArgs.contains("-antstream-shot-info-edit") {
                    showInfoEditor = true
                }
                return
            }
            // antstream-visual: `-antstream-shot-unfinished` shows the
            // card a relaunch after a killed broadcast opens on (#175). A
            // runner never has a broadcast killed under it, so a sample
            // entry stands in (nothing on disk).
            if RootView.shotArgs.contains("-antstream-shot-unfinished") {
                for _ in 0..<60 where !node.status.isReady || node.channel == nil {
                    try? await Task.sleep(nanoseconds: 1_000_000_000)
                }
                node.installUnfinishedSample()
                return
            }
            // antstream-visual: `-antstream-shot-channel` shows the "Your
            // channel" card next to a ready checklist, which needs a plan
            // (a fresh runner account has none); sample state supplies it.
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

                if node.unfinishedBroadcast != nil {
                    Text("Resume or end your unfinished broadcast first: a new one would leave it without its recording.")
                        .font(.caption)
                        .foregroundStyle(.yellow.opacity(0.9))
                        .multilineTextAlignment(.center)
                        .fixedSize(horizontal: false, vertical: true)
                }
                if node.announcingBroadcast {
                    Text(AntNode.announcingHoldNote)
                        .font(.caption)
                        .foregroundStyle(.yellow.opacity(0.9))
                        .multilineTextAlignment(.center)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Button {
                    resumeLive = false
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
                        .contentShape(.capsule)
                }
                .buttonStyle(.plain)
                .disabled(!node.isReadyToBroadcast || node.unfinishedBroadcast != nil
                          || node.announcingBroadcast)
            }
            .frame(maxWidth: .infinity)
        }
    }

    // MARK: unfinished broadcast (#175)

    /// The broadcast the app was killed or crashed in the middle of. Its
    /// viewers are on a live playlist that stopped advancing; carrying it
    /// on (within the reap window, the minute a viewer would wait anyway)
    /// or ending it closes that and leaves the channel's head the
    /// recording of every segment that landed.
    private func unfinishedCard(_ unfinished: UnfinishedBroadcast, now: Date) -> some View {
        let resumable = unfinished.canResume(now: now, reapMs: AntNode.broadcastReapMs)
        let since: String = Self.ago(unfinished.savedAt, now: now)
        let stopped: String = unfinished.ended ? "was ending" : "stopped"
        let what: String = "\"\(unfinished.channel)\" \(stopped) \(since) when the app closed, "
            + "with \(unfinished.segments) segments (\(unfinished.durationLabel)) on Swarm. "
        let next: String = resumable
            ? "Resume it to carry on in the same broadcast, or end it so viewers get its recording."
            : "End it so viewers get its recording instead of a stalled live stream."
        let detail: String = what + next
        return GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Unfinished broadcast", systemImage: "exclamationmark.arrow.circlepath")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text(detail)
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                    .fixedSize(horizontal: false, vertical: true)
                if let outcome = unfinishedOutcome {
                    Text(outcome)
                        .font(.caption)
                        .foregroundStyle(.orange.opacity(0.9))
                        .fixedSize(horizontal: false, vertical: true)
                }
                if endingUnfinished {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Publishing its recording…")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                } else {
                    HStack(spacing: 10) {
                        if resumable {
                            GlassPillButton(title: "Resume", icon: "dot.radiowaves.left.and.right",
                                            tint: .red.opacity(0.6)) {
                                resumeLive = true
                                showLive = true
                            }
                        }
                        GlassPillButton(title: "End broadcast", icon: "stop.circle",
                                        tint: .white.opacity(0.18)) {
                            Task { await endUnfinished() }
                        }
                    }
                    .disabled(!node.isReadyToBroadcast)
                    Button { confirmDiscard = true } label: {
                        Text("Discard it")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.white.opacity(0.6))
                            .padding(.horizontal, 12).padding(.vertical, 8)
                            .contentShape(.rect)
                    }
                    .buttonStyle(.plain)
                }
            }
        }
        .confirmationDialog("Discard the unfinished broadcast?",
                            isPresented: $confirmDiscard, titleVisibility: .visible) {
            Button("Discard", role: .destructive) {
                unfinishedOutcome = nil
                node.discardUnfinishedBroadcast()
            }
        } message: {
            Text("Its segments stay on Swarm, but it is never closed: viewers keep its last live window and get no recording.")
        }
    }

    private func endUnfinished() async {
        endingUnfinished = true
        unfinishedOutcome = nil
        defer { endingUnfinished = false }
        do {
            let report = try await node.finishUnfinishedBroadcast()
            // The entry is gone only once the recording is the channel's
            // head (and, with stream info, the catalog's `vod` entry
            // landed): landed just now, or already before (the update had
            // landed ahead of the kill, so the node wrote at most the
            // catalog entry and the report carries no new recording).
            // Either way the card is gone, so the outcome goes in the
            // banner.
            if node.unfinishedBroadcast == nil {
                let landedNow = !(report.recordingReference ?? "").isEmpty
                banner.flash(landedNow
                    ? "Broadcast ended: its recording is on the channel"
                    : "Broadcast ended: its recording was already on the channel")
            } else if let error = report.errors.first {
                unfinishedOutcome = error
            } else if let catalog = report.catalogError, !catalog.isEmpty,
                      report.catalogState != "vod" {
                // The recording is on the channel, but the account's
                // catalog still lists it live (#220): the entry is kept
                // so this can be tried again.
                unfinishedOutcome = "Its recording is on the channel, but the stream info "
                    + "still says live (\(catalog)); try again in a moment."
            } else {
                unfinishedOutcome = "Its recording did not go up; try again in a moment."
            }
        } catch {
            unfinishedOutcome = error.localizedDescription
        }
    }

    /// "20 s ago", "3 min ago", "2 h ago".
    private static func ago(_ date: Date, now: Date) -> String {
        let s = max(0, Int(now.timeIntervalSince(date)))
        if s < 90 { return "\(s) s ago" }
        if s < 90 * 60 { return "\(s / 60) min ago" }
        if s < 48 * 3600 { return "\(s / 3600) h ago" }
        return "\(s / 86_400) days ago"
    }

    // MARK: permanent channel

    /// The account's permanent channel: its address and the `antstream`
    /// topic, shareable before the first broadcast (there is nothing to
    /// create first): viewers who open it join whenever this device is
    /// live, and see the last broadcast otherwise.
    private func channelCard(_ address: ChannelAddress) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Your channel")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Your account address and the topic \u{201C}\(address.topicLabel)\u{201D}: one channel for all your broadcasts. Share it any time: viewers who open it join whenever you're live, and see your last broadcast otherwise.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                copyRow("Web player link", address.playerLink)
                if let uri = address.swarmURI {
                    copyRow("Solar Punk players", uri)
                }
                HStack(spacing: 10) {
                    ShareLink(item: address.watchLinks) {
                        Label("Share", systemImage: "square.and.arrow.up")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.white)
                            .padding(.horizontal, 16).padding(.vertical, 10)
                            .glassEffect(.regular.tint(.blue.opacity(0.35)), in: .capsule)
                            .contentShape(.capsule)
                    }
                    .buttonStyle(.plain)
                    GlassPillButton(title: "Watch", icon: "play.fill",
                                    tint: .white.opacity(0.18)) {
                        onWatch(address.key)
                    }
                }
            }
        }
    }

    /// One of the channel's links, labelled; a tap copies it.
    private func copyRow(_ label: String, _ value: String) -> some View {
        Button {
            UIPasteboard.general.string = value
            banner.flash("\(label) copied")
        } label: {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 3) {
                    Text(label)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.6))
                    Text(value)
                        .font(.caption.monospaced())
                        .foregroundStyle(.white)
                        .multilineTextAlignment(.leading)
                }
                Spacer()
                Image(systemName: "doc.on.doc")
                    .foregroundStyle(.white.opacity(0.85))
            }
            .padding(12)
            .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
            .contentShape(.rect(cornerRadius: 12))
        }
        .buttonStyle(.plain)
    }

    // MARK: stream info (#220)

    private static let streamInfoID = "streamInfo"

    /// The channel's name: the last broadcast's, else the device's.
    private var channelName: String {
        node.channel?.name ?? UIDevice.current.name
    }

    /// What the channel's catalog entry tells players (#220): the title,
    /// description and thumbnail the next broadcast goes out with, and an
    /// announced start. Edited in ``StreamInfoEditor``.
    private var streamInfoCard: some View {
        let info = node.streamInfo
        return GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Stream info")
                    .font(.headline)
                    .foregroundStyle(.white)
                HStack(alignment: .top, spacing: 12) {
                    StreamThumbnailView(jpeg: node.thumbnailJPEG, width: 112)
                    VStack(alignment: .leading, spacing: 4) {
                        Text(info.displayTitle(channel: channelName))
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.white)
                            .fixedSize(horizontal: false, vertical: true)
                        Text(info.description.isEmpty ? "No description" : info.description)
                            .font(.caption)
                            .foregroundStyle(.white.opacity(info.description.isEmpty ? 0.45 : 0.7))
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                if let announced = info.announcedStart, announced > Date() {
                    Label("Announced: starts \(announced.formatted(date: .abbreviated, time: .shortened))",
                          systemImage: "megaphone.fill")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.green)
                } else if let scheduled = info.scheduledStart, scheduled > Date() {
                    Label("Scheduled for \(scheduled.formatted(date: .abbreviated, time: .shortened)), not announced yet",
                          systemImage: "calendar")
                        .font(.caption)
                        .foregroundStyle(.yellow.opacity(0.9))
                }
                if node.thumbnailUploading {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Uploading the thumbnail…")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }
                if let problem = node.streamInfoError {
                    Text(problem)
                        .font(.caption)
                        .foregroundStyle(.orange.opacity(0.9))
                        .fixedSize(horizontal: false, vertical: true)
                }
                Text(info.thumbnailPicked || node.thumbnailJPEG == nil
                     ? "Players list your channel with these."
                     : "Players list your channel with these. The thumbnail is a frame from the camera, taken a few seconds after you go live.")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
                    .fixedSize(horizontal: false, vertical: true)
                GlassPillButton(title: "Edit", icon: "square.and.pencil", tint: .white.opacity(0.18)) {
                    showInfoEditor = true
                }
            }
        }
    }

    // MARK: readiness checklist

    private static let checklistID = "checklist"

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
                if node.hasStorage {
                    capacityRow
                }
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

    /// How long a broadcast at the last-picked quality fits on the plan
    /// (#176), counting the plan's fullest bucket. A warning, not a gate:
    /// a nearly full plan can still carry a short broadcast, so going
    /// live stays open and the going-live screen asks first.
    @ViewBuilder
    private var capacityRow: some View {
        let quality = LiveBroadcast.storedQuality
        if let estimate = node.capacity(for: quality) {
            checkRow(
                done: !estimate.needsWarning,
                warn: estimate.needsWarning,
                title: "Room for a broadcast",
                detail: [estimate.summary(quality: quality.label), estimate.warning]
                    .compactMap { $0 }
                    .joined(separator: ". ")
            )
        }
    }

    /// Where the gateway listens, or why it doesn't: the bind error stays
    /// on screen while the node retries every 10 s.
    private var gatewayDetail: String {
        if node.gatewayUp { return node.gatewayURL?.absoluteString ?? node.gatewayAddress }
        if let error = node.gatewayError { return "Not running: \(error) (retrying)" }
        return "Starting…"
    }

    /// `warn` marks a row that is usable but needs reading: an orange
    /// triangle, and the detail wraps instead of being cut to one line.
    private func checkRow(done: Bool, warn: Bool = false, title: String, detail: String) -> some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: warn
                  ? "exclamationmark.triangle.fill"
                  : (done ? "checkmark.circle.fill" : "circle"))
                .font(.title3)
                .foregroundStyle(warn ? Color.orange : (done ? Color.green : Color.white.opacity(0.4)))
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white)
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(warn ? Color.orange.opacity(0.9) : Color.white.opacity(0.6))
                    .lineLimit(warn ? nil : 1)
                    .truncationMode(.middle)
                    .fixedSize(horizontal: false, vertical: warn)
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
