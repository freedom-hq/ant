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
/// Segments are also kept in a small on-disk ring buffer. They are
/// uploaded from memory, so the ring is not on the publish path; it
/// exists so a segment can be inspected, replayed locally with
/// `AVPlayer`, or (stage 3) rolled into the VOD finalize pass.
final class CaptureEngine: NSObject {
    // MARK: configuration

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

    private static func requestAccess(_ media: AVMediaType) async -> Bool {
        switch AVCaptureDevice.authorizationStatus(for: media) {
        case .authorized: return true
        case .notDetermined: return await AVCaptureDevice.requestAccess(for: media)
        default: return false
        }
    }

    func start() {
        queue.async { [weak self] in
            guard let self, !self.state.isRunning else { return }
            self.state = .starting
            self.observeSystemEvents()
            switch self.source {
            case .camera:
                do {
                    try self.configureSession()
                } catch {
                    self.state = .failed(error.localizedDescription)
                    return
                }
                self.startWriter()
                self.session.startRunning()
            case .testPattern:
                self.startWriter()
                self.startTestPattern()
            }
            self.state = .running
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
            self.stopTestPattern()
            if self.session.isRunning { self.session.stopRunning() }
            self.finishWriter()
            NotificationCenter.default.removeObserver(self)
            self.observingSystemEvents = false
            self.rotationObservation?.invalidate()
            self.rotationObservation = nil
            self.rotationCoordinator = nil
            self.state = .idle
            completion?()
        }
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
            self.restartWriter()
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

    private func configureSession() throws {
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

        if let mic = AVCaptureDevice.default(for: .audio),
           let audioIn = try? AVCaptureDeviceInput(device: mic),
           session.canAddInput(audioIn) {
            session.addInput(audioIn)
        }

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

        let aOut = AVCaptureAudioDataOutput()
        aOut.setSampleBufferDelegate(self, queue: queue)
        if session.canAddOutput(aOut) {
            session.addOutput(aOut)
            audioOutput = aOut
        }
        observeRotation(of: camera)
    }

    /// Keep the encoded frames upright as the device turns. A rotation
    /// changes the encoded dimensions, so it also restarts the writer —
    /// #65's "orientation" interruption case.
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

    private func applyRotation(_ angle: CGFloat) {
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
                AVVideoWidthKey: settings.width,
                AVVideoHeightKey: settings.height,
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
                    AVEncoderBitRateKey: 64_000,
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
                    kCVPixelBufferWidthKey as String: settings.width,
                    kCVPixelBufferHeightKey as String: settings.height,
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
            // Recovery starts a *new* segment on a new timeline.
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
                kCVPixelBufferWidthKey as String: settings.width,
                kCVPixelBufferHeightKey as String: settings.height,
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
        delegateLock.unlock()

        let reported = segmentReport?.trackReports.first?.duration
        let durationMs = reported.map { UInt32(max(0, $0.seconds) * 1000) }
            ?? UInt32(segmentSeconds * 1000)
        onSegment?(
            Segment(
                data: segmentData,
                isInitialization: isInit,
                durationMs: isInit ? 0 : durationMs,
                discontinuity: discontinuity,
                fileURL: url
            )
        )
    }
}
