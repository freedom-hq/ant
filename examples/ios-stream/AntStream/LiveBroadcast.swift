import AVFoundation
import Foundation
import SwiftUI
import UIKit

/// Drives one broadcast: capture (#65) → publisher (#67 stage 2).
///
/// The two halves meet here and nowhere else. ``CaptureEngine`` produces
/// finished fMP4 segments on its own schedule; every one is handed
/// straight to `ant_publisher_push_segment`, which never blocks — a
/// capture pipeline stalled on the uplink drops frames, so backpressure
/// is the node's drop-oldest backlog, not this class.
///
/// The hand-off is a single ordered queue rather than a task per
/// segment, because the publisher numbers segments by the order the
/// pushes arrive and that number decides both playlist order and which
/// `#EXT-X-MAP` each segment is listed under.
///
/// It also owns the foreground keep-alive a broadcast needs: the idle
/// timer is held off for the duration (a screen that sleeps mid-stream
/// takes the camera with it) and the audio session is configured for
/// recording. Both are undone on stop, including on the failure paths —
/// leaving the idle timer disabled would quietly drain the battery long
/// after the broadcast ended.
@MainActor
final class LiveBroadcast: ObservableObject {
    enum State: Equatable {
        case idle
        case starting
        case live
        case stopping
        case failed(String)

        var isActive: Bool {
            switch self {
            case .starting, .live, .stopping: return true
            case .idle, .failed: return false
            }
        }
    }

    @Published private(set) var state: State = .idle
    @Published private(set) var capture: CaptureEngine.State = .idle
    @Published private(set) var progress: PublisherSnapshot?
    @Published private(set) var report: PublisherReport?
    /// How many segments the *capture* side produced and this class has
    /// handed on. Distinct from the publisher's counters: the gap
    /// between them is exactly what the drop-oldest discipline shed.
    @Published private(set) var capturedSegments: UInt64 = 0

    private(set) var engine: CaptureEngine?
    private var pollTask: Task<Void, Never>?
    /// The capture → publisher hand-off queue. The writer's delivery
    /// queue enqueues into it synchronously and ``pumpTask`` is its only
    /// consumer, so segments reach `ant_publisher_push_segment` in
    /// exactly the order the writer finished them.
    private var segmentFeed: AsyncStream<CaptureEngine.Segment>.Continuation?
    private var pumpTask: Task<Void, Never>?
    /// Set by the pump once the feed is closed *and* drained, so ``stop``
    /// knows the last segment has reached the publisher.
    private var pumpDrained = true
    private var idleTimerHeld = false

    /// The rendition. Defaults are the stage-1 go/no-go row — 360p at
    /// ~900 kbit/s with 2 s segments — and the same values are handed to
    /// both halves so the encoder and the playlist agree.
    var settings = CaptureEngine.Settings()

    /// Which source a broadcast started here would use. The simulator
    /// has no capture device, and CI drives the whole pipeline with the
    /// generated pattern via `-antstream-shot-live`.
    static var defaultSource: CaptureEngine.Source {
        #if targetEnvironment(simulator)
        return .testPattern
        #else
        return RootView.shotArgs.contains("-antstream-shot-live") ? .testPattern : .camera
        #endif
    }

    // MARK: - Lifecycle

    func start(node: AntNode, channel: String, source: CaptureEngine.Source? = nil) async {
        guard !state.isActive else { return }
        report = nil
        progress = nil
        capturedSegments = 0
        state = .starting

        guard let batchId = node.plan?.batchId, node.plan?.enabled == true else {
            state = .failed("Connect a storage plan before going live.")
            return
        }
        let source = source ?? Self.defaultSource
        if let problem = await CaptureEngine.permissionProblem(for: source) {
            state = .failed(problem)
            return
        }

        do {
            try await node.startPublisher(
                channel: channel,
                batchId: batchId,
                bitrateKbps: UInt32(settings.bitrateKbps),
                segmentMs: UInt32(settings.segmentSeconds * 1000),
                notes: Self.deviceNotes(network: node.networkLabel, source: source)
            )
        } catch {
            state = .failed(error.localizedDescription)
            return
        }

        configureAudioSession(for: source)
        holdIdleTimer()

        let engine = CaptureEngine(source: source, settings: settings)
        engine.onStateChange = { [weak self] newState in
            guard let self else { return }
            self.capture = newState
            if case .failed(let message) = newState {
                Task { await self.stop(node: node, failure: message) }
            }
        }
        // Capture → publisher is one ordered queue, not a task per
        // segment. The publisher numbers segments by the order its
        // `push_segment` calls arrive, and that number is both the
        // playlist order and the `#EXT-X-MAP` a segment is listed
        // under — so at a writer restart the old writer's last media
        // segment and the new writer's initialization segment must not
        // race. An unbounded buffer keeps the enqueue non-blocking:
        // backpressure stays the node's drop-oldest backlog.
        let (feed, sink) = AsyncStream<CaptureEngine.Segment>.makeStream(
            bufferingPolicy: .unbounded
        )
        segmentFeed = sink
        engine.onSegment = { segment in
            // Arrives on the writer's delivery queue; enqueueing is
            // synchronous and FIFO, so the order is settled right here.
            sink.yield(segment)
        }
        pumpDrained = false
        pumpTask = Task { @MainActor [weak self] in
            for await segment in feed {
                guard let self else { return }
                self.capturedSegments += 1
                // Awaited one at a time: the next segment is not handed
                // over until this one has reached the FFI.
                await node.pushSegment(segment)
            }
            self?.pumpDrained = true
        }
        self.engine = engine
        engine.start()
        state = .live
        startPolling(node: node)
    }

    /// End the broadcast. Capture stops first so the segment in flight
    /// is finished and published, then the publisher closes the playlist
    /// with `#EXT-X-ENDLIST`.
    func stop(node: AntNode, failure: String? = nil) async {
        guard state.isActive else { return }
        state = .stopping
        pollTask?.cancel()
        pollTask = nil
        // Wait for the writer to emit its final segment *and* for that
        // segment to reach the publisher before closing the publisher's
        // queue — otherwise the last seconds of the broadcast are
        // captured, encoded and then thrown away.
        if let engine {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                engine.stop { continuation.resume() }
            }
        }
        engine = nil
        // The writer has delivered its last segment, so the hand-off
        // queue now holds everything capture produced. Close it and let
        // the pump drain: every queued segment must reach the publisher
        // before `ant_publisher_stop` closes its queue, or the last
        // seconds of the broadcast are captured, encoded and then thrown
        // away. The drain is finite — each push returns a disposition,
        // failures included — but it is still bounded here so a wedged
        // push can't hang the stop button.
        segmentFeed?.finish()
        segmentFeed = nil
        for _ in 0..<40 {
            if pumpDrained { break }
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        pumpTask?.cancel()
        pumpTask = nil
        releaseIdleTimer()
        deactivateAudioSession()
        do {
            report = try await node.stopPublisher()
        } catch {
            if failure == nil {
                state = .failed(error.localizedDescription)
                return
            }
        }
        state = failure.map(State.failed) ?? .idle
    }

    // MARK: - Progress

    private func startPolling(node: AntNode) {
        pollTask?.cancel()
        pollTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                guard let self, self.state.isActive else { return }
                if let snapshot = await node.publisherProgress() {
                    self.progress = snapshot
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }

    // MARK: - Keep-alive

    /// A broadcast has to survive the screen wanting to sleep. Held for
    /// the duration and released on every exit path.
    private func holdIdleTimer() {
        guard !idleTimerHeld else { return }
        UIApplication.shared.isIdleTimerDisabled = true
        idleTimerHeld = true
    }

    private func releaseIdleTimer() {
        guard idleTimerHeld else { return }
        UIApplication.shared.isIdleTimerDisabled = false
        idleTimerHeld = false
    }

    /// Recording category, so the mic is ours and a broadcast is not cut
    /// short by another app's audio. Skipped for the generated pattern,
    /// which captures no audio at all.
    private func configureAudioSession(for source: CaptureEngine.Source) {
        guard source == .camera else { return }
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(
            .playAndRecord,
            mode: .videoRecording,
            options: [.defaultToSpeaker]
        )
        try? session.setActive(true)
    }

    private func deactivateAudioSession() {
        try? AVAudioSession.sharedInstance().setActive(
            false,
            options: [.notifyOthersOnDeactivation]
        )
    }

    private static func deviceNotes(network: String, source: CaptureEngine.Source) -> String {
        let device = UIDevice.current
        let thermal: String
        switch ProcessInfo.processInfo.thermalState {
        case .nominal: thermal = "nominal"
        case .fair: thermal = "fair"
        case .serious: thermal = "serious"
        case .critical: thermal = "critical"
        @unknown default: thermal = "unknown"
        }
        let sourceLabel = source == .camera ? "camera" : "test pattern"
        return "\(device.systemName) \(device.systemVersion) / \(network) / "
            + "\(sourceLabel) / thermal \(thermal)"
    }
}
