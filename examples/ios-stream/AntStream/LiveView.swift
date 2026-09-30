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
struct LiveView: View {
    @EnvironmentObject var node: AntNode
    @StateObject private var broadcast = LiveBroadcast()
    @StateObject private var banner = BannerState()
    @Environment(\.dismiss) private var dismiss

    /// Channel name, seeded from the device on appear so a first
    /// broadcast needs no typing but stays editable.
    @State private var channel = ""

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .broadcast)
            preview
            VStack(spacing: 14) {
                topBar
                Spacer()
                if let report = broadcast.report, !broadcast.state.isActive {
                    summaryCard(report)
                } else {
                    statusCard
                }
                controls
            }
            .padding(.horizontal, 16)
            .padding(.top, 12)
            .padding(.bottom, 24)
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .task {
            if channel.isEmpty { channel = UIDevice.current.name }
            // `-antstream-shot-live` drives the whole pipeline from the
            // generated test pattern, which is the only way a runner (no
            // camera, no tap driver) can reach this screen's live state.
            guard RootView.shotArgs.contains("-antstream-shot-live") else { return }
            for _ in 0..<60 where !node.status.isReady {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
            // A runner account has no storage plan, and going live is
            // refused without one. The sample plan gets past that gate;
            // its batch is not real, so the uploads themselves still
            // fail honestly at the gateway's batch check.
            node.installBroadcastSample()
            await broadcast.start(node: node, channel: "CI test pattern")
        }
    }

    // MARK: preview

    @ViewBuilder
    private var preview: some View {
        if let session = broadcast.engine?.previewSession {
            CameraPreview(session: session)
                .ignoresSafeArea()
        } else if broadcast.state.isActive {
            // Test-pattern mode has nothing to preview. Say so plainly
            // rather than showing an empty frame that reads as a broken
            // camera.
            VStack(spacing: 10) {
                Image(systemName: "waveform.badge.magnifyingglass")
                    .font(.system(size: 40))
                Text("Test pattern — no camera on this device")
                    .font(.footnote)
            }
            .foregroundStyle(.white.opacity(0.55))
        }
    }

    // MARK: top bar

    private var topBar: some View {
        HStack(spacing: 10) {
            liveBadge
            Spacer()
            if broadcast.state.isActive, broadcast.engine?.previewSession != nil {
                Button { broadcast.engine?.flipCamera() } label: {
                    Image(systemName: "arrow.triangle.2.circlepath.camera")
                        .font(.title3)
                        .foregroundStyle(.white)
                        .padding(10)
                        .glassEffect(.regular, in: .circle)
                }
                .buttonStyle(.plain)
            }
            Button { dismiss() } label: {
                Image(systemName: "xmark")
                    .font(.headline)
                    .foregroundStyle(.white)
                    .padding(10)
                    .glassEffect(.regular, in: .circle)
            }
            .buttonStyle(.plain)
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
                if let progress = broadcast.progress {
                    metrics(progress)
                } else if broadcast.state.isActive {
                    Text("Starting the encoder…")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                } else {
                    Text("Publishes \(Int(broadcast.settings.segmentSeconds)) s segments at "
                         + "\(broadcast.settings.bitrateKbps) kbit/s straight to Swarm, and updates "
                         + "the channel's feed so viewers follow the live edge.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
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
            if !progress.channelReference.isEmpty {
                Button {
                    UIPasteboard.general.string = progress.channelReference
                    banner.flash("Channel link copied")
                } label: {
                    Label("Copy channel link", systemImage: "link")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.85))
                }
                .buttonStyle(.plain)
            }
            if !progress.lastError.isEmpty {
                Text(progress.lastError)
                    .font(.caption2)
                    .foregroundStyle(.orange.opacity(0.9))
                    .lineLimit(2)
            }
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
                Text("\(report.durationLabel) · \(report.segmentsListed) playable segments · "
                     + "\(report.feedUpdates) feed updates · "
                     + String(format: "%.2f Mbit/s", report.sustainedMbitS))
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
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
            }
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

    private var controls: some View {
        Button {
            Task {
                if broadcast.state.isActive {
                    await broadcast.stop(node: node)
                } else {
                    await broadcast.start(
                        node: node,
                        channel: channel.isEmpty ? "AntStream" : channel
                    )
                }
            }
        } label: {
            Text(controlTitle)
                .font(.headline)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 16)
                .glassEffect(
                    .regular.tint(broadcast.state.isActive ? .white.opacity(0.2) : .red.opacity(0.6)),
                    in: .capsule
                )
                .foregroundStyle(.white)
        }
        .buttonStyle(.plain)
        .disabled(broadcast.state == .starting || broadcast.state == .stopping)
    }

    private var controlTitle: String {
        switch broadcast.state {
        case .starting: return "Starting…"
        case .stopping: return "Publishing the last segments…"
        case .live: return "Stop broadcast"
        case .idle, .failed: return "Go live"
        }
    }
}

/// `AVCaptureVideoPreviewLayer` as a SwiftUI view.
struct CameraPreview: UIViewRepresentable {
    let session: AVCaptureSession

    func makeUIView(context: Context) -> PreviewView {
        let view = PreviewView()
        view.previewLayer.session = session
        view.previewLayer.videoGravity = .resizeAspectFill
        return view
    }

    func updateUIView(_ view: PreviewView, context: Context) {
        if view.previewLayer.session !== session {
            view.previewLayer.session = session
        }
    }

    final class PreviewView: UIView {
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
