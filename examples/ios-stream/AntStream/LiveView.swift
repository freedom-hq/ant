import AVFoundation
import SwiftUI
import UIKit

/// The going-live screen: camera preview, the publish-lag indicator, and
/// the counters that say whether the uplink is keeping up.
///
/// The lag indicator is the load-bearing element and is deliberately
/// prominent. Stage 1 established that a phone-direct broadcast can
/// publish 360p and *not* keep up — a publisher that quietly drifts
/// minutes behind the live edge looks identical to a healthy one from
/// the inside — so the number a viewer actually experiences (capture →
/// the feed update that makes the segment playable) is on screen the
/// whole time, next to how many segments the live-edge discipline had to
/// drop to hold it there.
///
/// The camera runs from the moment the screen opens (#193), so the shot
/// can be framed, the camera switched and the quality picked before Go
/// live; the preview is drawn in the broadcast's own frame. The interface
/// turns with the device (``OrientationLock``) and the controls re-lay
/// out for landscape; the broadcast's orientation follows the phone until
/// Go live and then stays as it was for the whole broadcast.
struct LiveView: View {
    @EnvironmentObject var node: AntNode
    @StateObject private var broadcast = LiveBroadcast()
    @StateObject private var banner = BannerState()
    @Environment(\.dismiss) private var dismiss

    /// Channel name, seeded on appear from the last broadcast's name (or
    /// the device's, the first time) so going live needs no typing but
    /// stays editable.
    @State private var channel = ""
    /// The first go-live here carries on the broadcast the app was killed
    /// in the middle of (#175) rather than starting a new one. Cleared
    /// once used: a later go-live from this screen is a new broadcast.
    @State private var resumePending: Bool
    /// The run started from here last was a resume: once it ends with the
    /// unfinished broadcast's entry still on disk (the node refused it,
    /// or it failed), the button stays a resume, so the next tap never
    /// starts a new broadcast that would replace that entry.
    @State private var lastRunWasResume = false
    /// Going live was tapped on a plan with under an hour of room at the
    /// picked quality, or one Swarm won't stamp with (#176): ask first.
    @State private var confirmLowCapacity = false
    /// The interface is laid out wider than tall (the phone is turned).
    @State private var landscape = false

    init(resume: Bool = false) {
        _resumePending = State(initialValue: resume)
    }

    var body: some View {
        GeometryReader { geo in
            let wide = geo.size.width > geo.size.height
            ZStack {
                LiquidGlassBackground(palette: .broadcast)
                preview
                if wide {
                    landscapeOverlay
                } else {
                    portraitOverlay
                }
            }
            .onAppear { followLayout(wide: wide) }
            .onChange(of: wide) { _, wide in followLayout(wide: wide) }
        }
        .preferredColorScheme(.dark)
        .onAppear {
            OrientationLock.allowRotation(true)
            if RootView.shotArgs.contains("-antstream-shot-landscape") {
                // A runner's simulator can't be turned from the workflow:
                // turn the interface instead, for the landscape shots.
                OrientationLock.requestLandscape()
            }
        }
        .onDisappear {
            OrientationLock.allowRotation(false)
            broadcast.closePreview()
        }
        .task { await broadcast.openPreview() }
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .alert(lowCapacityTitle, isPresented: $confirmLowCapacity) {
            Button("Go live anyway", role: .destructive) {
                Task { await goLive() }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text(capacity?.warning ?? "")
        }
        .onChange(of: broadcast.state) { _, state in
            guard !state.isActive else { return }
            // Back to the preview after a broadcast that took the camera
            // down with it (a capture failure); a no-op while it runs. The
            // next broadcast's orientation follows the phone again (#193).
            Task { await broadcast.openPreview() }
            broadcast.portrait = !landscape
            // A resumed run that ended with its entry still on disk (the
            // node refused it at the feed head, or it failed): the next
            // tap resumes again rather than replacing the entry with a
            // new broadcast, as long as the entry can still be resumed
            // (the same `canResume` the Broadcast tab's card offers
            // Resume on). One a stop had begun closing, or older than the
            // reap window, can only be ended: the button is then held
            // (see `blockedByUnfinished`) and the card ends it. Any run,
            // not only a resume, can leave such an entry behind (a stop
            // whose recording upload failed).
            guard lastRunWasResume else { return }
            resumePending = resumable(now: Date())
        }
        .task {
            if resumePending, let unfinished = node.unfinishedBroadcast {
                channel = unfinished.channel
            }
            if channel.isEmpty { channel = node.channel?.name ?? UIDevice.current.name }
            // `-antstream-shot-live-idle` shows the screen before going
            // live (the quality picker), with the sample plan installed
            // so it reads as a device that could go live.
            if RootView.shotArgs.contains("-antstream-shot-live-idle") {
                for _ in 0..<60 where !node.status.isReady {
                    try? await Task.sleep(nanoseconds: 1_000_000_000)
                }
                node.installBroadcastSample()
                return
            }
            // `-antstream-shot-live` drives the whole pipeline from the
            // generated test pattern, which is the only way a runner (no
            // camera, no tap driver) can reach this screen's live state.
            guard RootView.shotArgs.contains("-antstream-shot-live") else { return }
            // Broadcasts go out on the account's permanent channel, which
            // exists once the account has been read after `ant_init`.
            for _ in 0..<60 where !node.status.isReady || node.channel == nil {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
            // A runner account has no storage plan, and going live is
            // refused without one. The sample plan gets past that gate;
            // its batch is not real, so the uploads themselves still
            // fail honestly at the gateway's batch check.
            node.installBroadcastSample()
            await broadcast.start(node: node, channel: "CI test pattern")
            // `-antstream-shot-live-paused` then drops the "network" the
            // way `AntNode`'s path monitor does when the device goes
            // offline (#172), so CI captures the paused state. The
            // "network" comes back 65 s later, past the minute's reap
            // window: the reap held for the return then ends the
            // broadcast (it never ends one while offline, since its
            // closing uploads need the uplink).
            if RootView.shotArgs.contains("-antstream-shot-live-paused") {
                try? await Task.sleep(nanoseconds: 5_000_000_000)
                await node.pausePublisher()
                try? await Task.sleep(nanoseconds: 65_000_000_000)
                await node.resumePublisher()
            }
        }
    }

    // MARK: layout

    /// The controls stacked under the picture, as before #193.
    private var portraitOverlay: some View {
        VStack(spacing: 14) {
            topBar
            Spacer()
            card
            controls
        }
        .padding(.horizontal, 16)
        .padding(.top, 12)
        .padding(.bottom, 24)
    }

    /// Turned to landscape there is no room for the card above the
    /// button: they sit side by side along the bottom, and the card
    /// scrolls if it is taller than the screen is high.
    private var landscapeOverlay: some View {
        VStack(spacing: 10) {
            topBar
            HStack(alignment: .bottom, spacing: 16) {
                ScrollView {
                    card
                }
                .defaultScrollAnchor(.bottom, for: .alignment)
                .scrollIndicators(.hidden)
                .frame(maxWidth: 440)
                Spacer(minLength: 0)
                controls
                    .frame(width: 260)
            }
        }
        .padding(.horizontal, 16)
        .padding(.top, 8)
        .padding(.bottom, 12)
    }

    @ViewBuilder
    private var card: some View {
        if let report = broadcast.report, !broadcast.state.isActive {
            summaryCard(report)
        } else {
            statusCard
        }
    }

    /// Until Go live the next broadcast takes the phone's orientation;
    /// once live it keeps the one it started with.
    private func followLayout(wide: Bool) {
        landscape = wide
        if !broadcast.state.isActive { broadcast.portrait = !wide }
    }

    // MARK: preview

    /// The broadcast's frame (9:16 or 16:9), with the camera in it as
    /// viewers will see it, or a placeholder that says why there is no
    /// picture.
    private var preview: some View {
        ZStack {
            previewContent
        }
        .aspectRatio(broadcast.portrait ? 9.0 / 16.0 : 16.0 / 9.0, contentMode: .fit)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .ignoresSafeArea()
    }

    @ViewBuilder
    private var previewContent: some View {
        if let session = broadcast.engine?.previewSession {
            CameraPreview(session: session, camera: broadcast.previewCamera)
        } else if let problem = broadcast.previewProblem {
            previewPlaceholder(icon: "video.slash", text: problem) {
                Button {
                    if let url = URL(string: UIApplication.openSettingsURLString) {
                        UIApplication.shared.open(url)
                    }
                } label: {
                    Text("Open Settings")
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.white)
                        .padding(.horizontal, 16).padding(.vertical, 9)
                        .glassEffect(.regular, in: .capsule)
                        .contentShape(.capsule)
                }
                .buttonStyle(.plain)
            }
        } else if LiveBroadcast.defaultSource == .testPattern {
            // Test-pattern mode (the simulator, CI) has nothing to
            // preview. Say so plainly rather than showing an empty frame
            // that reads as a broken camera.
            previewPlaceholder(
                icon: "waveform.badge.magnifyingglass",
                text: "Test pattern — no camera on this device"
            ) { EmptyView() }
        } else {
            // The camera is starting (or access is being asked for).
            Color.black.opacity(0.35)
        }
    }

    /// The broadcast frame's outline with an icon and a line of text in
    /// it, so the frame's shape still shows when there is no picture.
    private func previewPlaceholder<Extra: View>(
        icon: String,
        text: String,
        @ViewBuilder extra: () -> Extra
    ) -> some View {
        ZStack {
            Rectangle().fill(.black.opacity(0.3))
            Rectangle().strokeBorder(.white.opacity(0.18), lineWidth: 1)
            VStack(spacing: 10) {
                Image(systemName: icon)
                    .font(.system(size: 40))
                Text(text)
                    .font(.footnote)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
                extra()
            }
            .foregroundStyle(.white.opacity(0.55))
            .padding(32)
            // In landscape the card and the button cover the middle of
            // the frame: the words go under the top bar instead.
            .padding(.top, landscape ? 40 : 0)
            .frame(maxHeight: .infinity, alignment: landscape ? .top : .center)
        }
    }

    // MARK: top bar

    private var topBar: some View {
        HStack(spacing: 10) {
            liveBadge
            Spacer()
            // Before going live too (#193): the camera is picked while
            // framing the shot.
            if broadcast.engine?.previewSession != nil {
                Button { broadcast.engine?.flipCamera() } label: {
                    Image(systemName: "arrow.triangle.2.circlepath.camera")
                        .font(.title3)
                        .foregroundStyle(.white)
                        .padding(10)
                        .glassEffect(.regular, in: .circle)
                        .contentShape(.circle)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Switch camera")
            }
            Button { dismiss() } label: {
                Image(systemName: "xmark")
                    .font(.headline)
                    .foregroundStyle(.white)
                    .padding(10)
                    .glassEffect(.regular, in: .circle)
                    .contentShape(.circle)
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Close")
            .disabled(broadcast.state.isActive)
            .opacity(broadcast.state.isActive ? 0.3 : 1)
        }
    }

    /// LIVE + the live-edge lag, in one badge. Colour is the verdict:
    /// the node's own `keeping_up` (lag inside three segment durations,
    /// the same budget the throughput bench uses), so the badge can
    /// never disagree with the report. The lag it shows is the age of
    /// the newest playable segment, so a broadcast whose feed updates
    /// stop landing goes amber instead of sitting green on a stale
    /// figure.
    private var liveBadge: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(lagTint)
                .frame(width: 10, height: 10)
            Text(broadcast.state.isActive ? "LIVE" : "OFF AIR")
                .font(.caption.weight(.heavy))
                .tracking(1.5)
                .foregroundStyle(.white)
            if let progress = broadcast.progress, broadcast.state.isActive {
                Text(progress.lagLabel)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.white.opacity(0.75))
            }
        }
        .padding(.horizontal, 14).padding(.vertical, 9)
        .glassEffect(.regular.tint(lagTint.opacity(0.28)), in: .capsule)
    }

    private var lagTint: Color {
        guard broadcast.state.isActive else { return .white.opacity(0.4) }
        guard let progress = broadcast.progress, progress.playlistsPublished > 0 else {
            return .yellow
        }
        return progress.keepingUp ? .green : .orange
    }

    // MARK: status

    private var statusCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                if !broadcast.state.isActive {
                    TextField("Channel name", text: $channel)
                        .textFieldStyle(.plain)
                        .font(.headline)
                        .foregroundStyle(.white)
                        .padding(.vertical, 6)
                }
                if case .failed(let message) = broadcast.state {
                    Label(message, systemImage: "exclamationmark.triangle.fill")
                        .font(.footnote)
                        .foregroundStyle(.orange)
                }
                if case .interrupted(let reason) = broadcast.capture {
                    Label(reason, systemImage: "pause.circle.fill")
                        .font(.footnote)
                        .foregroundStyle(.yellow)
                }
                if case .failed(let message) = broadcast.capture, broadcast.state == .idle {
                    // The preview's camera failed to start (#193). A
                    // broadcast it ended says so as its own failure.
                    Label(message, systemImage: "video.slash")
                        .font(.footnote)
                        .foregroundStyle(.orange)
                }
                if broadcast.state.isActive, broadcast.engine?.previewSession != nil,
                   broadcast.portrait == landscape {
                    // Turned away from the orientation the broadcast went
                    // live in: the frame keeps its shape (#193).
                    Label("Broadcasting in \(orientationName) as you went live: "
                          + "turned this way the picture goes out with black bars. "
                          + "Turn the phone back to fill the frame.",
                          systemImage: broadcast.portrait
                              ? "rectangle.portrait.rotate" : "rectangle.landscape.rotate")
                        .font(.footnote)
                        .foregroundStyle(.yellow)
                        .fixedSize(horizontal: false, vertical: true)
                }
                if let progress = broadcast.progress, progress.paused == true,
                   broadcast.state.isActive {
                    // Paused, not over (#172): viewers stay on the
                    // broadcast and it carries on when the network does.
                    Label("No connection — paused. Viewers stay on the broadcast; "
                          + "it carries on if the network is back within a "
                          + "minute, and ends when it comes back after that.",
                          systemImage: "wifi.slash")
                        .font(.footnote)
                        .foregroundStyle(.yellow)
                        .fixedSize(horizontal: false, vertical: true)
                }
                if let problem = broadcast.progress?.batchProblem, broadcast.state.isActive {
                    // The plan, not the network, is refusing the uploads
                    // (#176): no retry or better signal brings them back.
                    batchProblemNote(problem)
                }
                if let progress = broadcast.progress {
                    metrics(progress)
                } else if broadcast.state.isActive {
                    Text("Starting the encoder…")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                } else {
                    qualityPicker
                    if let ref = node.channelReference {
                        channelLinkRow(ref)
                    }
                }
            }
        }
    }

    private func metrics(_ progress: PublisherSnapshot) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 18) {
                metric("PLAYABLE", "\(progress.segmentsListed)")
                metric("DROPPED", "\(progress.segmentsDropped)",
                       tint: progress.segmentsDropped > 0 ? .orange : nil)
                metric("FAILED", "\(progress.segmentsFailed)",
                       tint: progress.segmentsFailed > 0 ? .orange : nil)
                metric("PEERS", "\(progress.peers)")
            }
            HStack(spacing: 18) {
                metric("RATE", String(format: "%.2f Mbit/s", progress.sustainedMbitS))
                metric("UPLOAD p95", "\(progress.publishMsP95) ms")
                metric("SENT", formatBytes(progress.bytesPublished))
            }
            if let summary = creditSummary(node.credit) {
                HStack(spacing: 18) {
                    metric("CREDIT", summary, tint: node.credit?.swap.paying == true ? nil : .orange)
                    if let paid = node.credit?.sessionPlur, node.credit?.swap.issuedPlur != nil {
                        metric("PAID", formatBzz(paid))
                    }
                }
            }
            if let ref = liveReference(progress) {
                channelLinkRow(ref)
            }
            if !progress.lastError.isEmpty {
                Text(progress.lastError)
                    .font(.caption2)
                    .foregroundStyle(.orange.opacity(0.9))
                    .lineLimit(2)
            }
            if let recovery = progress.recoveryError, !recovery.isEmpty {
                // The crash-recovery entry (#175) isn't saving: a kill now
                // could not be resumed. Viewers are unaffected.
                Text("Crash recovery off: \(recovery)")
                    .font(.caption2)
                    .foregroundStyle(.yellow.opacity(0.9))
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    private func batchProblemNote(_ problem: BatchProblem) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Label(problem.title, systemImage: "externaldrive.badge.xmark")
                .font(.footnote.weight(.semibold))
                .foregroundStyle(.orange)
            Text(problem.detail)
                .font(.caption)
                .foregroundStyle(.orange.opacity(0.9))
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    private func metric(_ label: String, _ value: String, tint: Color? = nil) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .font(.system(size: 9, weight: .semibold))
                .tracking(1)
                .foregroundStyle(.white.opacity(0.5))
            Text(value)
                .font(.system(.subheadline, design: .rounded).weight(.bold))
                .foregroundStyle(tint ?? .white)
        }
    }

    // MARK: summary

    private func summaryCard(_ report: PublisherReport) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text(report.verdictLabel)
                    .font(.headline)
                    .foregroundStyle(verdictTint(report))
                    // Wraps rather than ellipsising: the reaped verdict
                    // is longer than one line on a phone.
                    .fixedSize(horizontal: false, vertical: true)
                Text("\(report.durationLabel) · \(report.segmentsListed) playable segments · "
                     + "\(report.feedUpdates) feed updates · "
                     + String(format: "%.2f Mbit/s", report.sustainedMbitS))
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                if let withheld = report.segmentsWithheld, withheld > 0 {
                    Text("\(withheld) segments shot while the connection was down "
                         + "were never sent.")
                        .font(.caption)
                        .foregroundStyle(.yellow.opacity(0.9))
                }
                if let problem = report.batchProblem {
                    batchProblemNote(problem)
                }
                if report.segmentsDropped > 0 || report.segmentsFailed > 0 {
                    Text("\(report.segmentsDropped) dropped at the live edge, "
                         + "\(report.segmentsFailed) failed to publish.")
                        .font(.caption)
                        .foregroundStyle(.orange.opacity(0.9))
                }
                if let first = report.errors.first {
                    Text(first)
                        .font(.caption2)
                        .foregroundStyle(.white.opacity(0.55))
                        .lineLimit(3)
                }
                if !report.channelReference.isEmpty {
                    channelLinkRow(report.channelReference)
                }
                if let recording = report.recordingReference, !recording.isEmpty {
                    recordingRow(recording)
                }
            }
        }
    }

    /// The recording's own reference (the whole broadcast, issue #168),
    /// for keeping a past broadcast. The channel link above plays it too
    /// until the channel's next broadcast starts.
    private func recordingRow(_ reference: String) -> some View {
        Button {
            UIPasteboard.general.string = reference
            banner.flash("Recording reference copied")
        } label: {
            Label("Copy recording reference", systemImage: "film")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.white.opacity(0.85))
                .padding(.vertical, 6)
                .contentShape(.rect)
        }
        .buttonStyle(.plain)
    }

    // MARK: quality

    /// The rendition, picked before going live (the encoder and the
    /// playlist are set up for one). Remembered by ``LiveBroadcast``.
    private var qualityPicker: some View {
        let quality = broadcast.quality
        let mbit = Double(quality.bitrateKbps) / 1000
        let mbPerHour = quality.bitrateKbps * 450 / 1000
        return VStack(alignment: .leading, spacing: 8) {
            Picker("Quality", selection: $broadcast.quality) {
                ForEach(CaptureEngine.Quality.allCases) { q in
                    Text(q.label).tag(q)
                }
            }
            .pickerStyle(.segmented)
            Text("\(quality.label) uses about \(String(format: "%.1f", mbit)) Mbit/s "
                 + "(\(mbPerHour) MB per hour) in \(Int(broadcast.settings.segmentSeconds)) s "
                 + "segments. Lower is more reliable on mobile data.")
                .font(.caption)
                .foregroundStyle(.white.opacity(0.7))
                .fixedSize(horizontal: false, vertical: true)
            // The orientation is picked by holding the phone (#193).
            Label("\(orientationName.capitalized) broadcast, as the phone is held. "
                  + "Turn it to switch; it stays \(orientationName) once you go live.",
                  systemImage: broadcast.portrait ? "rectangle.portrait" : "rectangle")
                .font(.caption)
                .foregroundStyle(.white.opacity(0.7))
                .fixedSize(horizontal: false, vertical: true)
            if let estimate = capacity {
                // The plan's room at this quality (#176), counted from
                // its fullest bucket: the same estimate and the same
                // warning test as the Broadcast tab's checklist.
                Label {
                    Text([estimate.summary(quality: quality.label), estimate.warning]
                        .compactMap { $0 }
                        .joined(separator: ". "))
                        .fixedSize(horizontal: false, vertical: true)
                } icon: {
                    Image(systemName: estimate.needsWarning
                          ? "exclamationmark.triangle.fill" : "externaldrive")
                }
                .font(.caption)
                .foregroundStyle(estimate.needsWarning ? Color.orange : Color.white.opacity(0.7))
            }
        }
    }

    private var orientationName: String { broadcast.portrait ? "portrait" : "landscape" }

    /// The plan's room for a broadcast at the picked quality (#176).
    private var capacity: CapacityEstimate? {
        node.capacity(for: broadcast.quality)
    }

    /// Going live needs a yes first: the plan runs out within the hour at
    /// this quality, or won't be stamped at all. A mutable plan with room
    /// is only warned about on screen.
    private var needsCapacityConfirmation: Bool {
        guard let estimate = capacity else { return false }
        return estimate.low || !estimate.usable
    }

    private var lowCapacityTitle: String {
        guard let estimate = capacity else { return "Storage plan" }
        if !estimate.usable { return "Storage plan refused" }
        return estimate.minutesLeft == 0
            ? "No room left on your storage plan"
            : "\(estimate.timeLabel) left on your storage plan"
    }

    // MARK: channel link

    /// The reference the running broadcast reports, or the permanent
    /// channel's before the publisher has created its manifest.
    private func liveReference(_ progress: PublisherSnapshot) -> String? {
        progress.channelReference.isEmpty ? node.channelReference : progress.channelReference
    }

    /// Copy the reference, or share the web player links for it (the
    /// same text the Broadcast tab's channel card sends).
    private func channelLinkRow(_ reference: String) -> some View {
        HStack(spacing: 10) {
            Button {
                UIPasteboard.general.string = reference
                banner.flash("Channel reference copied")
            } label: {
                Label("Copy", systemImage: "doc.on.doc")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.85))
                    .padding(.vertical, 6)
                    .contentShape(.rect)
            }
            .buttonStyle(.plain)
            ShareLink(item: Channel.watchLinks(reference)) {
                Label("Share channel link", systemImage: "square.and.arrow.up")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.85))
                    .padding(.vertical, 6)
                    .contentShape(.rect)
            }
            .buttonStyle(.plain)
            Spacer()
        }
    }

    /// Three-state, like the throughput bench's: a broadcast that
    /// published nothing has no verdict to give, so it is neither green
    /// nor an accusation that the uplink fell behind.
    private func verdictTint(_ report: PublisherReport) -> Color {
        guard report.hasVerdict else { return .white.opacity(0.8) }
        return report.keptUp ? .green : .orange
    }

    // MARK: controls

    /// The unfinished broadcast (#175) can be carried on from here: the
    /// Broadcast tab's own test for offering Resume on its card.
    private func resumable(now: Date) -> Bool {
        node.unfinishedBroadcast?.canResume(now: now, reapMs: AntNode.broadcastReapMs) == true
    }

    /// An unfinished broadcast's entry is on disk that this screen can't
    /// carry on (it was ending, it is past the reap window, or this
    /// screen was opened to go live, not to resume it). A new broadcast
    /// would replace that entry with its first feed update and leave it
    /// without its recording, so nothing starts from here: the Broadcast
    /// tab's card ends it (or discards it, with confirmation).
    private func blockedByUnfinished(now: Date) -> Bool {
        guard !broadcast.state.isActive, node.unfinishedBroadcast != nil else { return false }
        return !(resumePending && resumable(now: now))
    }

    private var controls: some View {
        // Re-read every few seconds, as the Broadcast tab's card does:
        // Resume goes once the reap window has passed.
        TimelineView(.periodic(from: .now, by: 5)) { context in
            controlStack(now: context.date)
        }
    }

    private func controlStack(now: Date) -> some View {
        let blocked = blockedByUnfinished(now: now)
        return VStack(spacing: 8) {
            if blocked {
                Text("End your unfinished broadcast on the Broadcast tab first: a new one would leave it without its recording.")
                    .font(.caption)
                    .foregroundStyle(.yellow.opacity(0.9))
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
            }
            controlButton(blocked: blocked)
        }
    }

    private func controlButton(blocked: Bool) -> some View {
        Button {
            Task {
                if broadcast.state.isActive {
                    await broadcast.stop(node: node)
                } else if needsCapacityConfirmation {
                    confirmLowCapacity = true
                } else {
                    await goLive()
                }
            }
        } label: {
            Text(controlTitle(blocked: blocked))
                .font(.headline)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 16)
                .glassEffect(
                    .regular.tint(broadcast.state.isActive ? .white.opacity(0.2) : .red.opacity(0.6)),
                    in: .capsule
                )
                .foregroundStyle(.white)
                .contentShape(.capsule)
        }
        .buttonStyle(.plain)
        .disabled(broadcast.state == .starting || broadcast.state == .stopping || blocked)
        .opacity(blocked ? 0.5 : 1)
    }

    private func goLive() async {
        guard !broadcast.state.isActive else { return }
        // Checked again at the tap (and after the capacity question): the
        // entry may have aged past the reap window since the last redraw.
        guard !blockedByUnfinished(now: Date()) else { return }
        let resume = resumePending
        resumePending = false
        lastRunWasResume = resume
        await broadcast.start(
            node: node,
            channel: channel.isEmpty ? "AntStream" : channel,
            resume: resume
        )
        // Refused at start (no plan, the camera, an entry the node won't
        // resume): still a resume, not a new broadcast that would replace
        // the unfinished one's entry. A refusal at the feed head comes
        // later, when the run ends: see `.onChange(of: broadcast.state)`.
        if case .failed = broadcast.state, resume { resumePending = true }
    }

    private func controlTitle(blocked: Bool) -> String {
        switch broadcast.state {
        case .starting: return "Starting…"
        case .stopping: return "Publishing the last segments…"
        case .live: return "Stop broadcast"
        case .idle, .failed:
            if blocked { return "Go live" }
            return resumePending ? "Resume broadcast" : "Go live"
        }
    }
}

/// `AVCaptureVideoPreviewLayer` as a SwiftUI view.
///
/// Shown inside the broadcast's frame, aspect-fit, so the picture is
/// what viewers get: the whole frame, with the black bars a phone turned
/// away from the broadcast's orientation puts in (#193). The picture is
/// kept upright whichever way the interface faces, by the camera's own
/// rotation coordinator.
struct CameraPreview: UIViewRepresentable {
    let session: AVCaptureSession
    /// The camera the session captures from; `nil` until it is set up.
    let camera: AVCaptureDevice?

    func makeUIView(context: Context) -> PreviewView {
        let view = PreviewView()
        view.previewLayer.session = session
        view.previewLayer.videoGravity = .resizeAspect
        view.follow(camera)
        return view
    }

    func updateUIView(_ view: PreviewView, context: Context) {
        if view.previewLayer.session !== session {
            view.previewLayer.session = session
        }
        view.follow(camera)
    }

    final class PreviewView: UIView {
        private var rotationCoordinator: AVCaptureDevice.RotationCoordinator?
        private var rotationObservation: NSKeyValueObservation?
        private var followedCamera: String?

        /// Keep the preview upright for `camera`, as the interface turns.
        /// A camera flip brings a new device and a new coordinator.
        func follow(_ camera: AVCaptureDevice?) {
            guard let camera, camera.uniqueID != followedCamera else { return }
            followedCamera = camera.uniqueID
            rotationObservation?.invalidate()
            let coordinator = AVCaptureDevice.RotationCoordinator(
                device: camera,
                previewLayer: previewLayer
            )
            rotationCoordinator = coordinator
            applyRotation(coordinator.videoRotationAngleForHorizonLevelPreview)
            rotationObservation = coordinator.observe(
                \.videoRotationAngleForHorizonLevelPreview,
                options: [.new]
            ) { [weak self] _, change in
                guard let angle = change.newValue else { return }
                Task { @MainActor in self?.applyRotation(angle) }
            }
        }

        private func applyRotation(_ angle: CGFloat) {
            guard let connection = previewLayer.connection,
                  connection.isVideoRotationAngleSupported(angle)
            else { return }
            connection.videoRotationAngle = angle
        }

        override static var layerClass: AnyClass { AVCaptureVideoPreviewLayer.self }
        /// Safe by construction: `layerClass` above pins the layer type.
        var previewLayer: AVCaptureVideoPreviewLayer {
            // swiftlint is not used here; the force cast is the documented
            // `layerClass` idiom from Apple's own AVCam sample.
            guard let layer = layer as? AVCaptureVideoPreviewLayer else {
                preconditionFailure("layerClass guarantees AVCaptureVideoPreviewLayer")
            }
            return layer
        }
    }
}
