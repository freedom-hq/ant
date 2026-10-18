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
/// `#EXT-X-MAP` each segment is listed under. A turn of the phone (#228)
/// travels the same queue, between the old writer's last segment and the
/// new writer's initialization segment, so the publisher hears the new
/// size before any segment of it.
///
/// It also owns the camera preview the live screen shows before Go live
/// (#193): ``openPreview()`` starts a ``CaptureEngine`` that only runs
/// the camera, ``start(node:channel:source:resume:)`` records with that
/// same engine (the camera is not restarted), a stop goes back to the
/// preview, and ``closePreview()`` turns the camera off when the screen
/// goes away.
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

    @Published private(set) var state: State = .idle {
        didSet { if !state.isActive { followOrientation() } }
    }
    @Published private(set) var capture: CaptureEngine.State = .idle
    @Published private(set) var progress: PublisherSnapshot?
    @Published private(set) var report: PublisherReport?
    /// How many segments the *capture* side produced and this class has
    /// handed on. Distinct from the publisher's counters: the gap
    /// between them is exactly what the drop-oldest discipline shed.
    @Published private(set) var capturedSegments: UInt64 = 0

    private(set) var engine: CaptureEngine?
    /// The camera the preview shows, for its rotation coordinator.
    @Published private(set) var previewCamera: AVCaptureDevice?
    /// Why there is no preview (camera access refused), in words.
    @Published private(set) var previewProblem: String?
    /// The live screen is open: a stopped broadcast goes back to the
    /// preview rather than turning the camera off.
    private var previewOpen = false
    private var pollTask: Task<Void, Never>?
    /// Takes the stream's thumbnail a few seconds into the broadcast (#220).
    private var thumbnailTask: Task<Void, Never>?
    /// How long into a broadcast its thumbnail is captured: past the
    /// camera's first, often dark or blurred frames.
    private static let thumbnailDelay: UInt64 = 5_000_000_000
    /// The capture → publisher hand-off queue. The writer's delivery
    /// queue enqueues into it synchronously and ``pumpTask`` is its only
    /// consumer, so segments reach `ant_publisher_push_segment` in
    /// exactly the order the writer finished them.
    private var segmentFeed: AsyncStream<FeedItem>.Continuation?
    /// What the capture side hands the publisher, in capture order.
    private enum FeedItem {
        case segment(CaptureEngine.Segment)
        /// The recording turned (#228): the new writer's settings.
        case rendition(CaptureEngine.Settings)
    }
    private var pumpTask: Task<Void, Never>?
    /// Set by the pump once the feed is closed *and* drained, so ``stop``
    /// knows the last segment has reached the publisher.
    private var pumpDrained = true
    private var idleTimerHeld = false
    /// The running broadcast carries on an unfinished one (#175): the
    /// node can still refuse it once it reads the feed head (the channel
    /// moved on), which ends the run by itself with the reason in its
    /// errors, and ``stop`` shows that reason as the failure.
    private var resumedRun = false

    /// The quality preset the next broadcast encodes, picked on the live
    /// screen before going live and remembered across launches. 360p by
    /// default: the uplink, not the camera, is what limits a phone.
    @Published var quality: CaptureEngine.Quality {
        didSet { UserDefaults.standard.set(quality.rawValue, forKey: Self.qualityKey) }
    }

    private static let qualityKey = "antstream.quality"

    /// The quality the next broadcast will use, as last picked. The
    /// Broadcast tab's checklist estimates the plan's room at it (#176).
    static var storedQuality: CaptureEngine.Quality {
        let stored = UserDefaults.standard.string(forKey: qualityKey) ?? ""
        return CaptureEngine.Quality(rawValue: stored) ?? .low
    }

    /// The broadcast is vertical. Before Go live it follows the way the
    /// picture comes off the camera (``picturePortrait``); a resume (#175)
    /// takes the orientation the unfinished broadcast last had instead
    /// (``pinForResume(owner:)``). While live it is the shape the
    /// recording encodes: the capture engine turns the recording once the
    /// phone has held a new orientation for
    /// ``CaptureEngine/orientationHold`` (#228), and the turn reaches here
    /// through the segment queue.
    @Published private(set) var portrait = true
    /// How the camera turns the picture: the physical device's
    /// orientation (its rotation coordinator's capture angle), which is
    /// what the encoded frames follow. Not the interface's layout: with
    /// Portrait Orientation Lock on, or in a narrow iPad window, the two
    /// disagree, and the broadcast must take the picture's shape or it
    /// letterboxes from the first frame. `nil` with no camera running.
    @Published private(set) var capturePortrait: Bool?
    /// The live screen's layout is taller than wide. Only the fallback
    /// for ``picturePortrait`` when there is no camera (the test pattern
    /// in the simulator and CI), set by ``LiveView``.
    var layoutPortrait = true {
        didSet { followOrientation() }
    }
    /// The orientation the unfinished broadcast the next go-live resumes
    /// last had, which it starts in (one feed, one recording, carrying on
    /// where it stopped); once live it follows the phone again (#228).
    @Published private(set) var resumePortrait: Bool?

    /// The shape the picture currently has.
    var picturePortrait: Bool { capturePortrait ?? shotPortrait ?? layoutPortrait }

    /// `-antstream-shot-portrait` makes the test pattern a portrait
    /// broadcast whatever the layout (#194's CI geometry check), the shape
    /// a phone held upright produces. Only where there is no camera
    /// reading: ``capturePortrait`` still wins. `-antstream-shot-turn`
    /// turns it to landscape ``shotTurnDelay`` into the broadcast, as a
    /// phone turned while live (#228's CI check).
    private var shotPortrait: Bool? =
        RootView.shotArgs.contains("-antstream-shot-portrait") ? true : nil
    private static let shotTurnDelay: UInt64 = 16_000_000_000
    private var shotTurnTask: Task<Void, Never>?

    /// Pin the next broadcast to the orientation the unfinished broadcast
    /// of `owner` went out in (#175), or release it (`nil`).
    func pinForResume(owner: String?) {
        resumePortrait = owner.map(Self.storedPortrait(owner:))
        followOrientation()
    }

    /// Re-derive ``portrait`` while no broadcast runs. While one does, the
    /// recording turns itself (#228): the camera's engine reads the phone
    /// itself, the test pattern is told the picture's shape.
    private func followOrientation() {
        guard !state.isActive else {
            if let engine, engine.source == .testPattern {
                engine.followPicture(portrait: picturePortrait)
            }
            return
        }
        let next = resumePortrait ?? picturePortrait
        if portrait != next { portrait = next }
    }

    /// The orientation each account's broadcast last had, so a resume
    /// after a kill (#175) continues in it: stored at Go live and again
    /// at every turn (#228). The recovery entry carries the size too
    /// (the node's own resume reads it), but the app needs the shape
    /// before it starts the run. One per account, as the entry is.
    private static func portraitKey(owner: String) -> String {
        "antstream.broadcastPortrait." + owner.lowercased()
            .replacingOccurrences(of: "0x", with: "")
    }

    /// A broadcast with nothing stored went out before #193, when every
    /// broadcast was landscape (640x360).
    static func storedPortrait(owner: String) -> Bool {
        UserDefaults.standard.object(forKey: portraitKey(owner: owner)) as? Bool ?? false
    }

    init() {
        quality = Self.storedQuality
    }

    /// The rendition for ``quality`` and ``portrait``, with 2 s segments.
    /// The same values are handed to both halves so the encoder and the
    /// playlist agree.
    var settings: CaptureEngine.Settings {
        var settings = CaptureEngine.Settings(quality: quality)
        settings.portrait = portrait
        return settings
    }

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

    // MARK: - Preview

    /// Show the camera before going live (#193): ask for camera access
    /// (only the camera: the microphone is asked for at Go live) and run
    /// the capture session without encoding. Nothing to show for the test
    /// pattern (the simulator, CI), which the screen says in words.
    func openPreview() async {
        previewOpen = true
        guard engine == nil, !state.isActive, Self.defaultSource == .camera else { return }
        guard await CaptureEngine.cameraAccess() else {
            previewProblem = "AntStream needs camera access to show the preview and broadcast. "
                + "Enable it in Settings › AntStream."
            return
        }
        // The screen may have gone while the permission prompt was up.
        guard previewOpen, engine == nil, !state.isActive else { return }
        previewProblem = nil
        let engine = makeEngine(source: .camera)
        self.engine = engine
        engine.startPreview()
    }

    /// The live screen went away: turn the camera off, unless a broadcast
    /// is still running on it.
    func closePreview() {
        previewOpen = false
        guard !state.isActive, let engine else { return }
        engine.stop()
        detachEngine()
    }

    /// Let go of the engine: its own late state changes (a stop's
    /// `.idle`) no longer reach this class (see ``makeEngine``), so what
    /// it showed is cleared here.
    private func detachEngine() {
        engine = nil
        previewCamera = nil
        capturePortrait = nil
        capture = .idle
        followOrientation()
    }

    /// A capture engine wired to publish its state, camera and picture
    /// orientation here, for as long as it is *the* engine: one that was
    /// replaced or closed still finishes its stop asynchronously, and its
    /// late `.idle` must not overwrite the current engine's state. The
    /// recording's own hooks (segments, failures) are added by ``start``.
    private func makeEngine(source: CaptureEngine.Source) -> CaptureEngine {
        let engine = CaptureEngine(source: source, settings: settings)
        engine.onStateChange = { [weak self, weak engine] newState in
            guard let self, let engine, self.engine === engine else { return }
            self.capture = newState
        }
        engine.onCameraChange = { [weak self, weak engine] device in
            guard let self, let engine, self.engine === engine else { return }
            self.previewCamera = device
        }
        engine.onOrientationChange = { [weak self, weak engine] portrait in
            guard let self, let engine, self.engine === engine else { return }
            self.capturePortrait = portrait
            self.followOrientation()
        }
        return engine
    }

    // MARK: - Lifecycle

    /// `resume` carries on the broadcast the app was killed in the middle
    /// of (#175, ``AntNode/unfinishedBroadcast``) instead of starting a
    /// new one: same feed, same recording, the camera's first segment
    /// behind one seam.
    func start(
        node: AntNode,
        channel: String,
        source: CaptureEngine.Source? = nil,
        resume: Bool = false
    ) async {
        guard !state.isActive else { return }
        report = nil
        progress = nil
        capturedSegments = 0
        resumedRun = resume
        state = .starting

        guard let batchId = node.plan?.batchId, node.plan?.enabled == true else {
            state = .failed("Connect a storage plan before going live.")
            return
        }
        // Every broadcast goes out on the account's permanent channel, so
        // the link shared before going live keeps working.
        guard let permanent = node.channel else {
            state = .failed("The account is still loading; try again in a moment.")
            return
        }
        let source = source ?? Self.defaultSource
        if let problem = await CaptureEngine.permissionProblem(for: source) {
            state = .failed(problem)
            return
        }

        // A resume keeps the orientation its broadcast went out in; a new
        // broadcast takes the picture's (``followOrientation`` kept it
        // current up to here). With no camera reading yet (the preview's
        // engine never got the camera, or Go live came before it was
        // made), the picture's shape is read from the camera once here
        // rather than taken from the layout, which disagrees with it under
        // Portrait Orientation Lock or in a narrow iPad window.
        if resume {
            portrait = Self.storedPortrait(owner: permanent.owner)
        } else if capturePortrait == nil, source == .camera,
                  let held = CaptureEngine.defaultCameraPortrait() {
            portrait = held
        }
        let settings = self.settings
        do {
            permanent.name = channel
            try await node.startPublisher(
                channel: channel,
                owner: permanent.owner,
                batchId: batchId,
                bitrateKbps: UInt32(settings.bitrateKbps),
                segmentMs: UInt32(settings.segmentSeconds * 1000),
                notes: Self.deviceNotes(network: node.networkLabel, source: source),
                resume: resume,
                // The account's stream info (#220): the broadcast keeps the
                // channel's catalog entry with it.
                info: node.streamInfo.forBroadcast,
                width: settings.encodedWidth,
                height: settings.encodedHeight
            )
        } catch {
            state = .failed(error.localizedDescription)
            return
        }
        node.streamInfoWentLive()
        UserDefaults.standard.set(settings.portrait, forKey: Self.portraitKey(owner: permanent.owner))

        configureAudioSession(for: source)
        holdIdleTimer()

        // Record with the engine the preview runs (#193), so going live
        // doesn't restart the camera. One for another source, or one that
        // failed, is replaced.
        let engine: CaptureEngine
        if let running = self.engine, running.source == source, !Self.isFailed(capture) {
            engine = running
        } else {
            self.engine?.stop()
            detachEngine()
            engine = makeEngine(source: source)
        }
        // The engine is current before anything it publishes arrives.
        self.engine = engine
        engine.onStateChange = { [weak self, weak engine] newState in
            guard let self, let engine, self.engine === engine else { return }
            self.capture = newState
            if case .failed(let message) = newState, self.state.isActive {
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
        let (feed, sink) = AsyncStream<FeedItem>.makeStream(
            bufferingPolicy: .unbounded
        )
        segmentFeed = sink
        engine.onSegment = { segment in
            // Arrives on the writer's delivery queue; enqueueing is
            // synchronous and FIFO, so the order is settled right here.
            sink.yield(.segment(segment))
        }
        engine.onRenditionChange = { turned in
            // On the capture queue, after the old writer's last segment
            // was enqueued above and before the new one's init segment.
            sink.yield(.rendition(turned))
        }
        let owner = permanent.owner
        pumpDrained = false
        pumpTask = Task { @MainActor [weak self] in
            for await item in feed {
                guard let self else { return }
                switch item {
                case .segment(let segment):
                    self.capturedSegments += 1
                    // Awaited one at a time: the next segment is not
                    // handed over until this one has reached the FFI.
                    await node.pushSegment(segment)
                case .rendition(let turned):
                    // The phone turned (#228): the shape shown, the
                    // shape a resume continues in, and the size the
                    // node names, before the new writer's segments.
                    self.portrait = turned.portrait
                    UserDefaults.standard.set(turned.portrait, forKey: Self.portraitKey(owner: owner))
                    await node.setPublisherRendition(
                        width: turned.encodedWidth,
                        height: turned.encodedHeight
                    )
                }
            }
            self?.pumpDrained = true
        }
        engine.start(settings: settings)
        // The test pattern's picture may already have another shape than
        // the one a resume starts in.
        if source == .testPattern { engine.followPicture(portrait: picturePortrait) }
        state = .live
        if source == .testPattern, RootView.shotArgs.contains("-antstream-shot-turn") {
            shotTurnTask = Task { @MainActor [weak self] in
                try? await Task.sleep(nanoseconds: Self.shotTurnDelay)
                guard !Task.isCancelled, let self, self.state == .live else { return }
                self.shotPortrait = false
                self.followOrientation()
            }
        }
        startPolling(node: node)
        captureThumbnail(node: node, engine: engine)
    }

    /// A few seconds into the broadcast, one frame becomes the stream's
    /// thumbnail (#220): scaled down, uploaded and handed to the
    /// broadcast's catalog entry. Not when the broadcaster chose a photo.
    private func captureThumbnail(node: AntNode, engine: CaptureEngine) {
        thumbnailTask?.cancel()
        guard !node.streamInfo.thumbnailPicked else { return }
        thumbnailTask = Task { @MainActor [weak self, weak engine] in
            try? await Task.sleep(nanoseconds: Self.thumbnailDelay)
            guard !Task.isCancelled, let self, self.state == .live,
                  let engine, self.engine === engine,
                  !node.streamInfo.thumbnailPicked
            else { return }
            guard let jpeg = await engine.thumbnailJPEG(),
                  !Task.isCancelled, self.state.isActive
            else { return }
            await node.useCapturedThumbnail(jpeg)
        }
    }

    /// End the broadcast. Capture stops first so the segment in flight
    /// is finished and published, then the publisher closes the playlist
    /// with `#EXT-X-ENDLIST` and publishes the whole broadcast as the
    /// recording.
    ///
    /// The only other end is the node's own (#172): a broadcast whose
    /// network is gone is paused, not stopped (``AntNode`` pauses and
    /// resumes it with the network), and only a minute with nothing
    /// reaching the playlist ends it (a minute that ran out offline, at
    /// the network's return); the progress poll then calls this.
    func stop(node: AntNode, failure: String? = nil) async {
        guard state.isActive else { return }
        state = .stopping
        pollTask?.cancel()
        pollTask = nil
        thumbnailTask?.cancel()
        thumbnailTask = nil
        shotTurnTask?.cancel()
        shotTurnTask = nil
        // Wait for the writer to emit its final segment *and* for that
        // segment to reach the publisher before closing the publisher's
        // queue — otherwise the last seconds of the broadcast are
        // captured, encoded and then thrown away.
        //
        // With the live screen still open the camera carries on as the
        // preview (#193); a failed engine, or a closed screen, stops it.
        if let engine {
            let keepPreview = previewOpen && failure == nil && engine.source == .camera
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                if keepPreview {
                    engine.stopRecording { continuation.resume() }
                } else {
                    engine.stop { continuation.resume() }
                }
            }
            if !keepPreview { detachEngine() }
        }
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
        let wasResume = resumedRun
        resumedRun = false
        do {
            report = try await node.stopPublisher()
        } catch {
            if failure == nil {
                state = .failed(error.localizedDescription)
                return
            }
        }
        // A resume the node refused at the feed head ended the run with
        // nothing written: say why rather than read as a clean stop.
        if failure == nil, wasResume,
           let refusal = report?.errors.first(where: { $0.hasPrefix("resume:") }) {
            state = .failed(refusal)
            return
        }
        state = failure.map(State.failed) ?? .idle
    }

    private static func isFailed(_ capture: CaptureEngine.State) -> Bool {
        if case .failed = capture { return true }
        return false
    }

    // MARK: - Progress

    private func startPolling(node: AntNode) {
        pollTask?.cancel()
        pollTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                guard let self, self.state.isActive else { return }
                if let snapshot = await node.publisherProgress() {
                    self.progress = snapshot
                    // A minute with nothing reaching the playlist (the
                    // network, or the app, gone that long) ends the
                    // broadcast node-side (#172): finish the stop here
                    // so the camera goes off and the report shows.
                    if snapshot.reaped == true || !snapshot.running {
                        Task { await self.stop(node: node) }
                        return
                    }
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
