import AVFoundation
import CoreMedia
import Foundation
import UIKit
import UniformTypeIdentifiers

/// Camera → H.264 → HLS fMP4 segments (issue #65).
///
/// `AVCaptureSession` (camera + mic) feeds an `AVAssetWriter` running in
/// `.mpeg4AppleHLS` mode, which hands back one *initialization* segment
/// (`ftyp` + `moov`) followed by a stream of *separable* media segments
/// (`moof` + `mdat`) on a wall clock — exactly the shape the publisher
/// (#67 stage 2) uploads and an `AVPlayer` plays back from a playlist.
///
/// Two things this class is careful about, because they are what a
/// 30-minute broadcast actually runs into:
///
/// * **Interruptions end a segment cleanly.** An incoming call, the app
///   backgrounding, a camera flip, an orientation change or a thermal
///   downshift all finish the current writer (so its last segment is a
///   complete, playable one) and start a fresh writer on recovery. The
///   first segment of the new writer carries a fresh initialization
///   segment and is flagged `discontinuity`, which is what lets the
///   playlist mark it `#EXT-X-DISCONTINUITY` instead of handing a player
///   a timeline that silently jumps.
/// * **Bitrate and segment duration are runtime-configurable.** #65 and
///   #67 were built in parallel, so the rendition is a parameter, not a
///   constant. Changing either restarts the writer (the encoder's
///   bitrate and keyframe interval are set at input creation), which is
///   the same clean-cut path an interruption takes.
///
/// * **The preview runs before the broadcast does (#193).** On the
///   camera path ``startPreview()`` runs the capture session with no
///   writer and no microphone, so the live screen shows the picture
///   before Go live; ``start(settings:)`` then adds the microphone and a
///   writer to that same running session (the camera is not restarted),
///   and ``stopRecording(completion:)`` takes them away again, leaving
///   the preview running.
/// * **The frame's shape is fixed per broadcast.** ``Settings/portrait``
///   is chosen at Go live and sets the encoded width and height for the
///   whole stream. Turning the phone afterwards still keeps the picture
///   upright (the capture rotation follows the device), and the encoder
///   fits the turned picture into the fixed frame with black bars rather
///   than switching the stream between landscape and portrait.
///
/// Segments are also kept in a small on-disk ring buffer. They are
/// uploaded from memory, so the ring is not on the publish path; it
/// exists so a segment can be inspected, replayed locally with
/// `AVPlayer`, or (stage 3) rolled into the VOD finalize pass.
final class CaptureEngine: NSObject {
    // MARK: configuration

    /// The camera's AAC track, on top of the video bitrate in every
    /// segment. The storage estimate (#176) counts it too.
    static let audioBitrateKbps = 64

    /// The rendition. Defaults are the stage-1 go/no-go row: 360p at
    /// ~900 kbit/s with 2 s segments (`crates/ant-ffi/ANTSTREAM_BENCH.md`).
    struct Settings: Equatable {
        var width = 640
        var height = 360
        var bitrateKbps = 900
        var segmentSeconds = 2.0
        var frameRate = 30
        /// Segments kept on disk. 60 × 2 s = the last two minutes.
        var ringCapacity = 60
        /// A vertical broadcast: the encoded frame is `height` wide and
        /// `width` tall (the presets are named by their short side, like
        /// any phone camera's 720p). Chosen at Go live, fixed for the
        /// broadcast (#193).
        var portrait = false

        /// The encoded frame's size, after ``portrait``.
        var encodedWidth: Int { portrait ? height : width }
        var encodedHeight: Int { portrait ? width : height }

        /// The rendition the thermal guard falls back to. Halving the
        /// bitrate is the cheapest lever that keeps a broadcast alive on
        /// a hot phone; dropping resolution as well would need a new
        /// session configuration, which is a longer interruption.
        func downshifted() -> Settings {
            var next = self
            next.bitrateKbps = max(200, bitrateKbps / 2)
            return next
        }
    }

    /// The renditions the live screen offers, as on Android
    /// (`CaptureEngine.Quality`). Mobile uplinks are what limits a
    /// broadcast (the stage-1 bench, #74), so the default is the
    /// lightest.
    enum Quality: String, CaseIterable, Identifiable {
        case low, medium, high

        var id: String { rawValue }

        var label: String {
            switch self {
            case .low: return "360p"
            case .medium: return "540p"
            case .high: return "720p"
            }
        }

        var width: Int {
            switch self {
            case .low: return 640
            case .medium: return 960
            case .high: return 1280
            }
        }

        var height: Int {
            switch self {
            case .low: return 360
            case .medium: return 540
            case .high: return 720
            }
        }

        var bitrateKbps: Int {
            switch self {
            case .low: return 600
            case .medium: return 1000
            case .high: return 1800
            }
        }
    }

    /// Where the frames come from.
    enum Source: Equatable {
        /// The device camera and microphone.
        case camera
        /// A generated test pattern, no camera or microphone. The
        /// simulator has no capture device at all, so this is how the
        /// encode → segment → publish path is exercised in CI (and how
        /// `antstream-visual` can screenshot a live broadcast). Always
        /// labelled as such on screen — it must never be mistaken for
        /// real capture.
        case testPattern
    }

    enum State: Equatable {
        case idle
        /// The camera is running for the preview only: no writer, no
        /// microphone, nothing is encoded (#193).
        case previewing
        case starting
        case running
        /// Capture is paused by something outside our control (call,
        /// backgrounding, another app took the camera). Recovery starts
        /// a new segment automatically.
        case interrupted(String)
        case failed(String)

        var isRunning: Bool { self == .running }
    }

    /// One finished fMP4 segment.
    struct Segment {
        let data: Data
        let isInitialization: Bool
        let durationMs: UInt32
        /// This segment does not continue the previous one's timeline.
        let discontinuity: Bool
        /// Ring-buffer copy, when it could be written.
        let fileURL: URL?
    }

    // MARK: callbacks

    /// Called for every finished segment, on the writer's own delivery
    /// queue (not ``queue``) — hop to your own actor before touching
    /// anything shared.
    var onSegment: ((Segment) -> Void)?
    /// Called on the main actor whenever ``state`` changes — the type
    /// carries that isolation so a SwiftUI observer can publish straight
    /// from it.
    var onStateChange: (@MainActor (State) -> Void)?
    /// Called on the main actor with the camera the session now captures
    /// from (after the session is set up and after every flip), so the
    /// preview can keep its own rotation coordinator on the same device.
    var onCameraChange: (@MainActor (AVCaptureDevice?) -> Void)?
    /// Called on the main actor with whether the camera's picture is
    /// upright-portrait (`true`) or landscape, from the capture rotation
    /// angle — the *physical* device's orientation, which the encoded
    /// frames follow even when the interface doesn't turn (Portrait
    /// Orientation Lock, a narrow iPad window). The broadcast takes its
    /// shape from this at Go live (#193).
    var onOrientationChange: (@MainActor (Bool) -> Void)?

    private(set) var state: State = .idle {
        didSet {
            guard state != oldValue else { return }
            let newState = state
            Task { @MainActor [onStateChange] in onStateChange?(newState) }
        }
    }

    private(set) var settings: Settings
    let source: Source

    /// Everything below runs here: capture callbacks, writer lifecycle,
    /// and the test-pattern generator. One serial queue means the writer
    /// can never be torn down underneath an in-flight `append`.
    private let queue = DispatchQueue(label: "antstream.capture", qos: .userInitiated)

    private let session = AVCaptureSession()
    private var videoOutput: AVCaptureVideoDataOutput?
    private var audioOutput: AVCaptureAudioDataOutput?
    private var videoDeviceInput: AVCaptureDeviceInput?
    private var rotationCoordinator: AVCaptureDevice.RotationCoordinator?
    private var rotationObservation: NSKeyValueObservation?

    private var writer: AVAssetWriter?
    private var videoInput: AVAssetWriterInput?
    private var audioInput: AVAssetWriterInput?
    private var pixelAdaptor: AVAssetWriterInputPixelBufferAdaptor?
    private var sessionStarted = false
    /// Set while a writer is being torn down, so late samples from the
    /// capture outputs are dropped instead of appended to a finished
    /// writer (which throws).
    private var restarting = false
    /// A broadcast is being recorded: segments are written. Off while the
    /// session only feeds the preview, so no interruption-recovery path
    /// starts a writer behind the preview's back.
    private var recording = false
    /// The camera session has its video input and output.
    private var sessionConfigured = false
    private var audioDeviceInput: AVCaptureDeviceInput?
    /// The rendition the operator asked for, before any thermal
    /// downshift — so recovery restores it rather than the reduced one.
    private var baseSettings: Settings

    /// Guards the fields the `AVAssetWriterDelegate` callback touches.
    /// Those callbacks arrive on the writer's own queue, not ``queue``,
    /// and they must not hop onto ``queue``: `finishWriter` blocks there
    /// waiting for `finishWriting`, whose final segment callbacks would
    /// then deadlock behind it.
    private let delegateLock = NSLock()
    private var pendingDiscontinuityLocked = false
    private var segmentIndexLocked = 0
    /// Media the current writer has handed over, exactly (a `CMTime`
    /// keeps the track's own timescale), and the whole milliseconds
    /// already reported for it. Each segment's `durationMs` is the
    /// rounded total less what was already reported, so per-segment
    /// rounding never accumulates: the publisher dates every segment by
    /// adding these up (`#EXT-X-PROGRAM-DATE-TIME`, #170), and truncating
    /// each one on its own would let the dates fall behind the media by
    /// up to a millisecond a segment. Reset by each writer's
    /// initialization segment.
    private var writerMediaLocked = CMTime.zero
    private var writerReportedMsLocked: UInt64 = 0
    private var ringFilesLocked: [URL] = []
    /// Snapshot of the settings the delegate needs, republished under
    /// ``delegateLock`` whenever the rendition changes.
    private var delegateSettings: (segmentSeconds: Double, ringCapacity: Int)

    private var observingSystemEvents = false
    private var testPatternTimer: DispatchSourceTimer?
    private var testPatternFrame = 0
    private var pixelBufferPool: CVPixelBufferPool?

    private let ringDirectory: URL

    /// The preview layer's session — `nil` in test-pattern mode, where
    /// there is nothing to preview.
    var previewSession: AVCaptureSession? { source == .camera ? session : nil }

    init(source: Source, settings: Settings = Settings()) {
        self.source = source
        self.settings = settings
        self.baseSettings = settings
        self.delegateSettings = (settings.segmentSeconds, settings.ringCapacity)
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        ringDirectory = base.appendingPathComponent("antstream/segments", isDirectory: true)
        super.init()
        try? FileManager.default.createDirectory(at: ringDirectory, withIntermediateDirectories: true)
    }

    // MARK: - Lifecycle

    /// Ask for the permissions this source needs. Returns the reason it
    /// can't run, or `nil` when it can.
    static func permissionProblem(for source: Source) async -> String? {
        guard source == .camera else { return nil }
        guard await requestAccess(.video) else {
            return "AntStream needs camera access to broadcast. Enable it in Settings › AntStream."
        }
        guard await requestAccess(.audio) else {
            return "AntStream needs microphone access to broadcast. Enable it in Settings › AntStream."
        }
        return nil
    }

    /// Camera access alone, asked for when the live screen opens its
    /// preview (#193). The microphone waits for Go live.
    static func cameraAccess() async -> Bool {
        await requestAccess(.video)
    }

    private static func requestAccess(_ media: AVMediaType) async -> Bool {
        switch AVCaptureDevice.authorizationStatus(for: media) {
        case .authorized: return true
        case .notDetermined: return await AVCaptureDevice.requestAccess(for: media)
        default: return false
        }
    }

    /// Run the camera for the preview only (#193): the capture session,
    /// without a writer or the microphone, so nothing is encoded and the
    /// mic indicator stays off. The test pattern has nothing to preview.
    /// The caller asks for camera access first.
    func startPreview() {
        queue.async { [weak self] in
            guard let self, self.source == .camera, self.state == .idle else { return }
            self.observeSystemEvents()
            do {
                try self.configureSession()
            } catch {
                self.state = .failed(error.localizedDescription)
                return
            }
            if !self.session.isRunning { self.session.startRunning() }
            self.state = .previewing
        }
    }

    /// Start recording, in `settings` when given. On the camera path this
    /// joins a session the preview already runs (adding the microphone),
    /// so going live does not restart the camera.
    func start(settings newSettings: Settings? = nil) {
        queue.async { [weak self] in
            guard let self, !self.recording else { return }
            if let newSettings, newSettings != self.settings {
                self.baseSettings = newSettings
                self.publish(settings: newSettings)
            }
            self.recording = true
            self.state = .starting
            self.observeSystemEvents()
            switch self.source {
            case .camera:
                do {
                    try self.configureSession()
                } catch {
                    self.recording = false
                    self.state = .failed(error.localizedDescription)
                    return
                }
                self.attachAudio()
                self.startWriter()
                if !self.session.isRunning { self.session.startRunning() }
            case .testPattern:
                self.startWriter()
                self.startTestPattern()
            }
            self.state = .running
        }
    }

    /// End the recording but keep the preview (#193): flush the segment in
    /// progress like ``stop(completion:)`` does, drop the microphone, and
    /// leave the camera session running. The test pattern, which has no
    /// preview, stops altogether.
    func stopRecording(completion: (() -> Void)? = nil) {
        queue.async { [weak self] in
            guard let self else {
                completion?()
                return
            }
            guard self.source == .camera, self.sessionConfigured else {
                self.stopNow()
                completion?()
                return
            }
            self.finishWriter()
            self.recording = false
            self.detachAudio()
            if !self.session.isRunning { self.session.startRunning() }
            // Still interrupted (a call, another app on the camera): keep
            // saying so; the interruption's end puts the preview back.
            if case .interrupted = self.state {} else { self.state = .previewing }
            completion?()
        }
    }

    /// Stop capturing and flush the segment in progress, so the last
    /// thing published is a complete segment rather than a truncated one.
    ///
    /// `completion` runs after the final segment has been delivered to
    /// ``onSegment``. Callers that stop the publisher next must wait for
    /// it, or the last segment of the broadcast is handed to a publisher
    /// that has already closed its queue.
    func stop(completion: (() -> Void)? = nil) {
        queue.async { [weak self] in
            guard let self else {
                completion?()
                return
            }
            self.stopNow()
            completion?()
        }
    }

    /// ``stop(completion:)``'s body, on ``queue``.
    private func stopNow() {
        stopTestPattern()
        if session.isRunning { session.stopRunning() }
        finishWriter()
        recording = false
        NotificationCenter.default.removeObserver(self)
        observingSystemEvents = false
        rotationObservation?.invalidate()
        rotationObservation = nil
        rotationCoordinator = nil
        state = .idle
    }

    /// Change the rendition mid-broadcast. Restarts the writer, so the
    /// current segment is finished cleanly and the next one starts a new
    /// (discontinuous) timeline.
    func apply(settings newSettings: Settings) {
        queue.async { [weak self] in
            guard let self, self.settings != newSettings else { return }
            self.baseSettings = newSettings
            self.publish(settings: newSettings)
            guard self.state.isRunning || self.state == .starting else { return }
            self.restartWriter()
        }
    }

    /// Flip between the front and back camera. A different device means
    /// a different encoder session, so this takes the same clean-cut
    /// path an interruption does.
    func flipCamera() {
        queue.async { [weak self] in
            guard let self, self.source == .camera,
                  let current = self.videoDeviceInput else { return }
            let wantFront = current.device.position != .front
            guard let device = Self.captureDevice(front: wantFront),
                  let input = try? AVCaptureDeviceInput(device: device) else { return }
            self.session.beginConfiguration()
            self.session.removeInput(current)
            if self.session.canAddInput(input) {
                self.session.addInput(input)
                self.videoDeviceInput = input
            } else {
                self.session.addInput(current)
            }
            self.session.commitConfiguration()
            self.observeRotation(of: self.videoDeviceInput?.device)
            self.announceCamera()
            // Only a recording has a writer to cut; flipping in the
            // preview must not start one.
            if self.recording { self.restartWriter() }
        }
    }

    // MARK: - Capture session

    private static func captureDevice(front: Bool) -> AVCaptureDevice? {
        AVCaptureDevice.default(
            .builtInWideAngleCamera,
            for: .video,
            position: front ? .front : .back
        )
    }

    /// Set up the camera input and the video output, once: the preview
    /// sets the session up and a recording joins it. The microphone is
    /// added separately (``attachAudio()``), only while recording.
    private func configureSession() throws {
        guard !sessionConfigured else { return }
        session.beginConfiguration()
        defer { session.commitConfiguration() }
        session.sessionPreset = .high

        guard let camera = Self.captureDevice(front: false) ?? Self.captureDevice(front: true)
        else {
            throw CaptureError.noDevice("This device has no camera.")
        }
        let videoIn = try AVCaptureDeviceInput(device: camera)
        guard session.canAddInput(videoIn) else {
            throw CaptureError.noDevice("The camera is unavailable.")
        }
        session.addInput(videoIn)
        videoDeviceInput = videoIn

        let vOut = AVCaptureVideoDataOutput()
        vOut.alwaysDiscardsLateVideoFrames = true
        vOut.videoSettings = [
            kCVPixelBufferPixelFormatTypeKey as String:
                Int(kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange),
        ]
        vOut.setSampleBufferDelegate(self, queue: queue)
        guard session.canAddOutput(vOut) else {
            throw CaptureError.noDevice("The camera output is unavailable.")
        }
        session.addOutput(vOut)
        videoOutput = vOut
        sessionConfigured = true
        observeRotation(of: camera)
        announceCamera()
    }

    /// Add the microphone for a recording. Into the running session, so
    /// the camera carries on; best effort, as before (a broadcast without
    /// a mic is still a broadcast).
    private func attachAudio() {
        guard audioDeviceInput == nil,
              let mic = AVCaptureDevice.default(for: .audio),
              let audioIn = try? AVCaptureDeviceInput(device: mic)
        else { return }
        session.beginConfiguration()
        defer { session.commitConfiguration() }
        guard session.canAddInput(audioIn) else { return }
        session.addInput(audioIn)
        audioDeviceInput = audioIn
        if audioOutput == nil {
            let aOut = AVCaptureAudioDataOutput()
            aOut.setSampleBufferDelegate(self, queue: queue)
            if session.canAddOutput(aOut) {
                session.addOutput(aOut)
                audioOutput = aOut
            }
        }
    }

    /// Take the microphone away again once the recording ends, so the
    /// preview doesn't keep it (and its privacy indicator) on.
    private func detachAudio() {
        guard let audioIn = audioDeviceInput else { return }
        session.beginConfiguration()
        session.removeInput(audioIn)
        if let aOut = audioOutput { session.removeOutput(aOut) }
        session.commitConfiguration()
        audioDeviceInput = nil
        audioOutput = nil
    }

    /// Tell the preview which camera the session captures from.
    private func announceCamera() {
        let device = videoDeviceInput?.device
        Task { @MainActor [onCameraChange] in onCameraChange?(device) }
    }

    /// Keep the encoded frames upright as the device turns. A rotation
    /// changes the shape of the frames the camera delivers, so it also
    /// restarts the writer — #65's "orientation" interruption case. The
    /// encoded frame keeps the shape chosen at Go live (#193): the new
    /// writer fits the turned picture into it (`AVVideoScalingModeKey`).
    private func observeRotation(of device: AVCaptureDevice?) {
        rotationObservation?.invalidate()
        guard let device else { return }
        let coordinator = AVCaptureDevice.RotationCoordinator(device: device, previewLayer: nil)
        rotationCoordinator = coordinator
        applyRotation(coordinator.videoRotationAngleForHorizonLevelCapture)
        rotationObservation = coordinator.observe(
            \.videoRotationAngleForHorizonLevelCapture,
            options: [.new]
        ) { [weak self] _, change in
            guard let angle = change.newValue else { return }
            self?.queue.async { self?.applyRotation(angle) }
        }
    }

    /// A capture angle of 90° or 270° turns the sensor's landscape frames
    /// upright: the picture is portrait.
    static func isPortrait(captureAngle angle: CGFloat) -> Bool {
        Int(angle.rounded()).quotientAndRemainder(dividingBy: 180).remainder != 0
    }

    private func applyRotation(_ angle: CGFloat) {
        let portrait = Self.isPortrait(captureAngle: angle)
        Task { @MainActor [onOrientationChange] in onOrientationChange?(portrait) }
        guard let connection = videoOutput?.connection(with: .video),
              connection.isVideoRotationAngleSupported(angle),
              connection.videoRotationAngle != angle
        else { return }
        connection.videoRotationAngle = angle
        if state.isRunning { restartWriter() }
    }

    // MARK: - Writer

    private func startWriter() {
        // HLS mode: no output file, segments arrive through the delegate.
        let writer = AVAssetWriter(contentType: UTType.mpeg4Movie)
        writer.outputFileTypeProfile = .mpeg4AppleHLS
        writer.preferredOutputSegmentInterval = CMTime(
            seconds: settings.segmentSeconds,
            preferredTimescale: 600
        )
        // `initialSegmentStartTime` is set in `beginSession` — it must
        // equal the source time passed to `startSession`, and that time
        // is the first sample's own clock (seconds since boot on the
        // camera path, the running frame counter on the test pattern),
        // which is not known until the sample arrives.
        writer.delegate = self

        var compression: [String: Any] = [
            AVVideoAverageBitRateKey: settings.bitrateKbps * 1000,
            AVVideoProfileLevelKey: AVVideoProfileLevelH264MainAutoLevel,
            // One keyframe per segment: a segment that does not start on
            // an IDR frame is not independently playable, which is the
            // whole point of segmenting.
            AVVideoMaxKeyFrameIntervalDurationKey: settings.segmentSeconds,
            AVVideoExpectedSourceFrameRateKey: settings.frameRate,
        ]
        compression[AVVideoAllowFrameReorderingKey] = false
        let vInput = AVAssetWriterInput(
            mediaType: .video,
            outputSettings: [
                AVVideoCodecKey: AVVideoCodecType.h264,
                AVVideoWidthKey: settings.encodedWidth,
                AVVideoHeightKey: settings.encodedHeight,
                // A phone turned away from the broadcast's orientation
                // delivers frames of the other shape: fit them in with
                // black bars rather than stretch them (#193).
                AVVideoScalingModeKey: AVVideoScalingModeResizeAspect,
                AVVideoCompressionPropertiesKey: compression,
            ]
        )
        vInput.expectsMediaDataInRealTime = true
        guard writer.canAdd(vInput) else {
            state = .failed("The video encoder rejected this rendition.")
            return
        }
        writer.add(vInput)
        videoInput = vInput

        if source == .camera {
            let aInput = AVAssetWriterInput(
                mediaType: .audio,
                outputSettings: [
                    AVFormatIDKey: kAudioFormatMPEG4AAC,
                    AVNumberOfChannelsKey: 1,
                    AVSampleRateKey: 44_100,
                    AVEncoderBitRateKey: CaptureEngine.audioBitrateKbps * 1000,
                ]
            )
            aInput.expectsMediaDataInRealTime = true
            if writer.canAdd(aInput) {
                writer.add(aInput)
                audioInput = aInput
            }
        } else {
            audioInput = nil
            pixelAdaptor = AVAssetWriterInputPixelBufferAdaptor(
                assetWriterInput: vInput,
                sourcePixelBufferAttributes: [
                    kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA),
                    kCVPixelBufferWidthKey as String: settings.encodedWidth,
                    kCVPixelBufferHeightKey as String: settings.encodedHeight,
                ]
            )
        }

        sessionStarted = false
        self.writer = writer
    }

    /// Start writing at the first video sample's own timestamp.
    ///
    /// Apple's fragmented-MP4 authoring contract anchors segmentation
    /// boundaries at `initialSegmentStartTime`, so it and the
    /// `startSession` source time must be the same instant — a `.zero`
    /// start against capture-clock timestamps (hours since boot) risks
    /// a wrong first-segment duration or an immediate cut. Setting it
    /// here, just before `startWriting`, keeps the two in agreement on
    /// every path, including a writer restarted mid-broadcast whose
    /// first sample is nowhere near time zero.
    private func beginSession(of writer: AVAssetWriter, at time: CMTime) -> Bool {
        writer.initialSegmentStartTime = time
        guard writer.startWriting() else {
            state = .failed(writer.error?.localizedDescription ?? "The video encoder did not start.")
            return false
        }
        writer.startSession(atSourceTime: time)
        sessionStarted = true
        return true
    }

    /// Finish the writer so the segment in progress is emitted complete.
    private func finishWriter() {
        guard let writer, writer.status == .writing else {
            // A writer that never saw a video sample never started
            // writing (see `beginSession`); nothing to flush.
            self.writer = nil
            videoInput = nil
            audioInput = nil
            pixelAdaptor = nil
            sessionStarted = false
            return
        }
        restarting = true
        videoInput?.markAsFinished()
        audioInput?.markAsFinished()
        let group = DispatchGroup()
        group.enter()
        writer.finishWriting { group.leave() }
        // Bounded: the delegate callbacks for the final segment are
        // delivered before `finishWriting` completes, and a writer that
        // hangs must not wedge the capture queue for good.
        _ = group.wait(timeout: .now() + 5)
        self.writer = nil
        videoInput = nil
        audioInput = nil
        pixelAdaptor = nil
        sessionStarted = false
        restarting = false
    }

    /// End the current segment cleanly and open a new timeline. Every
    /// interruption path funnels through here.
    private func restartWriter() {
        finishWriter()
        flagDiscontinuity()
        startWriter()
    }

    /// The next media segment starts a new timeline.
    private func flagDiscontinuity() {
        delegateLock.lock()
        pendingDiscontinuityLocked = true
        delegateLock.unlock()
    }

    // MARK: - System events

    private func observeSystemEvents() {
        guard !observingSystemEvents else { return }
        observingSystemEvents = true
        let center = NotificationCenter.default
        center.addObserver(
            self, selector: #selector(sessionInterrupted(_:)),
            name: AVCaptureSession.wasInterruptedNotification, object: session
        )
        center.addObserver(
            self, selector: #selector(sessionInterruptionEnded(_:)),
            name: AVCaptureSession.interruptionEndedNotification, object: session
        )
        center.addObserver(
            self, selector: #selector(sessionRuntimeError(_:)),
            name: AVCaptureSession.runtimeErrorNotification, object: session
        )
        center.addObserver(
            self, selector: #selector(didEnterBackground),
            name: UIApplication.didEnterBackgroundNotification, object: nil
        )
        center.addObserver(
            self, selector: #selector(willEnterForeground),
            name: UIApplication.willEnterForegroundNotification, object: nil
        )
        center.addObserver(
            self, selector: #selector(thermalStateChanged),
            name: ProcessInfo.thermalStateDidChangeNotification, object: nil
        )
    }

    @objc private func sessionInterrupted(_ note: Notification) {
        let raw = note.userInfo?[AVCaptureSessionInterruptionReasonKey] as? Int
        let reason = AVCaptureSession.InterruptionReason(rawValue: raw ?? 0)
        queue.async { [weak self] in
            guard let self else { return }
            // Finish the segment in flight *now*: an interrupted session
            // stops delivering samples, and a half-written segment is
            // not playable.
            self.finishWriter()
            self.flagDiscontinuity()
            self.state = .interrupted(Self.label(for: reason))
        }
    }

    private static func label(for reason: AVCaptureSession.InterruptionReason?) -> String {
        switch reason {
        case .audioDeviceInUseByAnotherClient, .videoDeviceInUseByAnotherClient:
            return "Paused — another app is using the camera or microphone"
        case .videoDeviceNotAvailableInBackground:
            return "Paused — AntStream is in the background"
        case .videoDeviceNotAvailableWithMultipleForegroundApps:
            return "Paused — Split View is using the camera"
        case .videoDeviceNotAvailableDueToSystemPressure:
            return "Paused — the device is too warm"
        default:
            return "Paused — capture was interrupted"
        }
    }

    @objc private func sessionInterruptionEnded(_ note: Notification) {
        queue.async { [weak self] in
            guard let self, self.state != .idle else { return }
            // Recovery starts a *new* segment on a new timeline; the
            // preview alone has no writer to start.
            guard self.recording else {
                self.state = .previewing
                return
            }
            if self.writer == nil { self.startWriter() }
            self.state = .running
        }
    }

    @objc private func sessionRuntimeError(_ note: Notification) {
        let error = note.userInfo?[AVCaptureSessionErrorKey] as? NSError
        queue.async { [weak self] in
            guard let self else { return }
            self.finishWriter()
            self.flagDiscontinuity()
            self.state = .interrupted(error?.localizedDescription ?? "Capture error")
            if self.source == .camera, !self.session.isRunning {
                self.session.startRunning()
            }
            guard self.recording else {
                self.state = .previewing
                return
            }
            self.startWriter()
            self.state = .running
        }
    }

    @objc private func didEnterBackground() {
        queue.async { [weak self] in
            guard let self, self.state != .idle else { return }
            self.stopTestPattern()
            self.finishWriter()
            self.flagDiscontinuity()
            self.state = .interrupted("Paused — AntStream is in the background")
        }
    }

    @objc private func willEnterForeground() {
        queue.async { [weak self] in
            guard let self, case .interrupted = self.state else { return }
            guard self.recording else {
                self.state = .previewing
                return
            }
            if self.writer == nil { self.startWriter() }
            if self.source == .testPattern { self.startTestPattern() }
            self.state = .running
        }
    }

    /// Thermal downshift: a phone that throttles mid-broadcast drops
    /// frames long before it stops, so halve the bitrate once it is
    /// seriously warm and restore the configured one when it cools.
    @objc private func thermalStateChanged() {
        let thermal = ProcessInfo.processInfo.thermalState
        queue.async { [weak self] in
            guard let self, self.state.isRunning else { return }
            let hot = thermal == .serious || thermal == .critical
            let wanted = hot ? self.baseSettings.downshifted() : self.baseSettings
            guard wanted != self.settings else { return }
            self.publish(settings: wanted)
            self.restartWriter()
        }
    }

    private func publish(settings newSettings: Settings) {
        settings = newSettings
        pixelBufferPool = nil
        delegateLock.lock()
        delegateSettings = (newSettings.segmentSeconds, newSettings.ringCapacity)
        delegateLock.unlock()
    }

    // MARK: - Test pattern

    private func startTestPattern() {
        guard source == .testPattern, testPatternTimer == nil else { return }
        let interval = 1.0 / Double(max(1, settings.frameRate))
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now(), repeating: interval)
        timer.setEventHandler { [weak self] in self?.emitTestFrame() }
        testPatternTimer = timer
        timer.resume()
    }

    private func stopTestPattern() {
        testPatternTimer?.cancel()
        testPatternTimer = nil
    }

    private func emitTestFrame() {
        guard let writer, !restarting else { return }
        let time = CMTime(
            value: CMTimeValue(testPatternFrame),
            timescale: CMTimeScale(max(1, settings.frameRate))
        )
        if !sessionStarted {
            guard beginSession(of: writer, at: time) else { return }
        }
        guard writer.status == .writing,
              let adaptor = pixelAdaptor, let input = videoInput, input.isReadyForMoreMediaData,
              let buffer = makeTestPixelBuffer()
        else { return }
        adaptor.append(buffer, withPresentationTime: time)
        testPatternFrame += 1
    }

    /// A moving bar over a slowly-shifting background. Deliberately not
    /// a static image: identical frames encode to nothing, which would
    /// make the segments unrepresentative of real video.
    private func makeTestPixelBuffer() -> CVPixelBuffer? {
        if pixelBufferPool == nil {
            let attributes: [String: Any] = [
                kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA),
                kCVPixelBufferWidthKey as String: settings.encodedWidth,
                kCVPixelBufferHeightKey as String: settings.encodedHeight,
                kCVPixelBufferCGImageCompatibilityKey as String: true,
            ]
            var pool: CVPixelBufferPool?
            CVPixelBufferPoolCreate(nil, nil, attributes as CFDictionary, &pool)
            pixelBufferPool = pool
        }
        guard let pool = pixelBufferPool else { return nil }
        var buffer: CVPixelBuffer?
        guard CVPixelBufferPoolCreatePixelBuffer(nil, pool, &buffer) == kCVReturnSuccess,
              let pixels = buffer
        else { return nil }
        CVPixelBufferLockBaseAddress(pixels, [])
        defer { CVPixelBufferUnlockBaseAddress(pixels, []) }
        guard let base = CVPixelBufferGetBaseAddress(pixels) else { return nil }
        let bytesPerRow = CVPixelBufferGetBytesPerRow(pixels)
        let width = CVPixelBufferGetWidth(pixels)
        let height = CVPixelBufferGetHeight(pixels)
        let phase = Double(testPatternFrame)
        let barX = Int((sin(phase / 18.0) * 0.4 + 0.5) * Double(width - 40))
        let shade = UInt8(24 + 24 * (sin(phase / 40.0) * 0.5 + 0.5))
        for y in 0..<height {
            let row = base.advanced(by: y * bytesPerRow).assumingMemoryBound(to: UInt8.self)
            for x in 0..<width {
                let inBar = x >= barX && x < barX + 40
                let p = row.advanced(by: x * 4)
                p[0] = inBar ? 200 : shade                       // B
                p[1] = inBar ? 90 : UInt8(shade / 2)             // G
                p[2] = inBar ? 40 : UInt8(y * 200 / max(1, height)) // R
                p[3] = 255
            }
        }
        return pixels
    }

    // MARK: - Ring buffer

    /// Write a segment into the on-disk ring and evict the oldest past
    /// `capacity`. Called with ``delegateLock`` held.
    private func storeLocked(_ data: Data, name: String, capacity: Int) -> URL? {
        let url = ringDirectory.appendingPathComponent(name)
        do {
            try data.write(to: url, options: .atomic)
        } catch {
            return nil
        }
        ringFilesLocked.append(url)
        while ringFilesLocked.count > capacity {
            let victim = ringFilesLocked.removeFirst()
            try? FileManager.default.removeItem(at: victim)
        }
        return url
    }

    enum CaptureError: LocalizedError {
        case noDevice(String)

        var errorDescription: String? {
            switch self {
            case .noDevice(let m): return m
            }
        }
    }
}

// MARK: - Sample delivery

extension CaptureEngine: AVCaptureVideoDataOutputSampleBufferDelegate,
                         AVCaptureAudioDataOutputSampleBufferDelegate {
    func captureOutput(
        _ output: AVCaptureOutput,
        didOutput sampleBuffer: CMSampleBuffer,
        from connection: AVCaptureConnection
    ) {
        guard !restarting, let writer else { return }
        let time = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        if !sessionStarted {
            // Video first: starting the session on an audio sample can
            // leave the first video frames before the session start and
            // the writer rejects them.
            guard output is AVCaptureVideoDataOutput else { return }
            guard beginSession(of: writer, at: time) else { return }
        }
        guard writer.status == .writing else { return }
        let input = output is AVCaptureVideoDataOutput ? videoInput : audioInput
        guard let input, input.isReadyForMoreMediaData else { return }
        input.append(sampleBuffer)
    }
}

// MARK: - Segment delivery

extension CaptureEngine: AVAssetWriterDelegate {
    func assetWriter(
        _ writer: AVAssetWriter,
        didOutputSegmentData segmentData: Data,
        segmentType: AVAssetSegmentType,
        segmentReport: AVAssetSegmentReport?
    ) {
        let isInit = segmentType == .initialization
        delegateLock.lock()
        let (segmentSeconds, ringCapacity) = delegateSettings
        segmentIndexLocked += 1
        let name = isInit ? "init-\(segmentIndexLocked).mp4" : "seg-\(segmentIndexLocked).m4s"
        let url = storeLocked(segmentData, name: name, capacity: ringCapacity)
        let discontinuity = !isInit && pendingDiscontinuityLocked
        if discontinuity { pendingDiscontinuityLocked = false }
        var durationMs: UInt32 = 0
        if isInit {
            writerMediaLocked = .zero
            writerReportedMsLocked = 0
        } else {
            // The video track's: segments are cut on its keyframes.
            let reports = segmentReport?.trackReports ?? []
            let reported = (reports.first { $0.mediaType == .video } ?? reports.first)?.duration
            let media = reported.flatMap { $0.isNumeric && $0 > .zero ? $0 : nil }
                ?? CMTime(seconds: segmentSeconds, preferredTimescale: 1000)
            writerMediaLocked = CMTimeAdd(writerMediaLocked, media)
            let totalMs = UInt64((writerMediaLocked.seconds * 1000).rounded())
            durationMs = UInt32(clamping: totalMs - min(totalMs, writerReportedMsLocked))
            writerReportedMsLocked = max(totalMs, writerReportedMsLocked)
        }
        delegateLock.unlock()

        onSegment?(
            Segment(
                data: segmentData,
                isInitialization: isInit,
                durationMs: durationMs,
                discontinuity: discontinuity,
                fileURL: url
            )
        )
    }
}

extension CaptureEngine.Settings {
    /// The rendition for a quality preset; segment length, frame rate and
    /// the ring keep their defaults. In an extension so the memberwise
    /// and default initializers stay available.
    init(quality: CaptureEngine.Quality) {
        self.init()
        width = quality.width
        height = quality.height
        bitrateKbps = quality.bitrateKbps
    }
}
