import CoreGraphics

/// The shape the live screen draws the broadcast's frame in, around the
/// camera preview (#193, #214). Pure: no AVFoundation, so
/// `ci/preview-geometry-tests.swift` runs it on the CI runner without a
/// camera, a simulator or the app.
///
/// The preview keeps the picture level with the horizon relative to the
/// *interface* (its rotation coordinator's preview angle), while the
/// encoded frames follow the *device* as held (the capture angle). Most
/// of the time the two stand the picture the same way. With Portrait
/// Orientation Lock on and the phone sideways (or a narrow iPad window)
/// they don't: the preview then shows the picture turned a quarter
/// against the interface, and the frame is turned the same way, so on
/// the screen it has the shape viewers get and the picture fills it.
///
/// Both angles must come from **one** coordinator, for the camera the
/// preview shows, read at the same moment. Mixing the preview's angle for
/// one camera with a capture reading for another (or one that hasn't
/// caught up with a flip yet) turns the frame against the picture, and
/// `.resizeAspect` then letterboxes twice: (9/16)² × (9/16)² ≈ 10% of the
/// screen, what #214 saw on the selfie camera.
enum PreviewGeometry {
    /// Which camera the angles were read for.
    enum Facing: Equatable {
        case front
        case back
    }

    /// A rotation angle of 90° or 270° (in either direction) turns a
    /// camera's landscape sensor frames upright: the picture is portrait.
    /// 0° and 180° leave it landscape. The front camera mirrors, and its
    /// coordinator can answer 270° where the back camera answers 90° (and
    /// 180° for 0° in landscape): those are the same shape, only turned
    /// over, so only the quarter turns count.
    static func isPortrait(angle: CGFloat) -> Bool {
        Int(angle.rounded()).quotientAndRemainder(dividingBy: 180).remainder != 0
    }

    /// Whether to draw the broadcast's frame taller than wide in the
    /// interface.
    ///
    /// - Parameters:
    ///   - broadcastPortrait: the broadcast's encoded frame is portrait
    ///     (``LiveBroadcast/portrait``).
    ///   - previewAngle: the rotation the preview's connection actually
    ///     has (read back, not the request), `nil` until the preview has
    ///     one for this camera.
    ///   - captureAngle: the same coordinator's horizon-level capture
    ///     angle: how the encoded frames stand the picture.
    ///   - facing: the camera both angles were read for. The shape is the
    ///     same for both cameras; it is part of the decision so the tests
    ///     pin exactly that (a mirrored 270° is a portrait 90°).
    /// - Returns: `broadcastPortrait` itself when either angle is unknown
    ///   (no preview yet, or a reading from another camera or session
    ///   that the caller dropped), so the frame never turns on half a
    ///   reading.
    static func frameTall(
        broadcastPortrait: Bool,
        previewAngle: CGFloat?,
        captureAngle: CGFloat?,
        facing: Facing
    ) -> Bool {
        guard let previewAngle, let captureAngle else { return broadcastPortrait }
        _ = facing
        // The preview shows the picture turned a quarter against how it
        // is captured: the frame turns with it.
        let turned = isPortrait(angle: previewAngle) != isPortrait(angle: captureAngle)
        return broadcastPortrait != turned
    }
}
