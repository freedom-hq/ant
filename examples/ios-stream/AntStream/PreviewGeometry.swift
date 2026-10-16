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
    /// A rotation angle of 90° or 270° (in either direction) turns a
    /// camera's landscape sensor frames upright: the picture is portrait.
    /// 0° and 180° leave it landscape. The front camera mirrors, and its
    /// coordinator can answer 270° where the back camera answers 90° (and
    /// 180° for 0° in landscape): those are the same shape, only turned
    /// over, so only the quarter turns count. Nothing here depends on
    /// which camera answered, which is why no camera is passed in.
    static func isPortrait(angle: CGFloat) -> Bool {
        Int(angle.rounded()).quotientAndRemainder(dividingBy: 180).remainder != 0
    }

    /// Whether to draw the broadcast's frame taller than wide in the
    /// interface.
    ///
    /// - Parameters:
    ///   - broadcastPortrait: the broadcast's encoded frame is portrait
    ///     (``LiveBroadcast/portrait``).
    ///   - pinned: that shape is fixed (live since Go live, or a resume's
    ///     shape, #193/#175). When it isn't, it follows the picture, and
    ///     `broadcastPortrait` is the capture engine's reading from its
    ///     *own* coordinator, which may not have caught up yet: the frame
    ///     then takes the shape the preview shows the picture in, from
    ///     the preview's angle alone, with nothing from another
    ///     coordinator mixed in (#214).
    ///   - previewAngle: the rotation the preview's connection actually
    ///     has (read back, not the request), `nil` until the preview has
    ///     one for this camera.
    ///   - captureAngle: the same coordinator's horizon-level capture
    ///     angle: how the encoded frames stand the picture. Only used
    ///     while `pinned`.
    /// - Returns: `broadcastPortrait` itself when an angle the decision
    ///   needs is unknown (no preview yet, or a reading from another
    ///   camera or session that the caller dropped), so the frame never
    ///   turns on half a reading.
    static func frameTall(
        broadcastPortrait: Bool,
        pinned: Bool,
        previewAngle: CGFloat?,
        captureAngle: CGFloat?
    ) -> Bool {
        guard let previewAngle else { return broadcastPortrait }
        // Not pinned: the broadcast takes the picture's shape, the
        // picture fills the preview, and the frame is that shape.
        guard pinned else { return isPortrait(angle: previewAngle) }
        guard let captureAngle else { return broadcastPortrait }
        // The preview shows the picture turned a quarter against how it
        // is captured: the pinned frame turns with it.
        let turned = isPortrait(angle: previewAngle) != isPortrait(angle: captureAngle)
        return broadcastPortrait != turned
    }
}
