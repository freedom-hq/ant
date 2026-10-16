// Unit tests for the live screen's frame-shape decision (#214),
// `AntStream/PreviewGeometry.swift`. The app has no test target and the
// simulator has no camera, so the antstream-visual workflow compiles the
// pure decision on its own with these tests and runs them on the macOS
// runner:
//
//     swiftc -parse-as-library AntStream/PreviewGeometry.swift \
//         ci/preview-geometry-tests.swift -o build/preview-geometry-tests
//     build/preview-geometry-tests
//
// The angles below are what `AVCaptureDevice.RotationCoordinator` answers
// for a phone (horizon-level preview angle with the preview layer, and
// capture angle): the back camera stands a portrait picture up at 90°,
// the mirrored front camera at 90° or 270°; a landscape picture is 0° or
// 180° (the front camera's landscape is the back camera's turned over).
// Under Portrait Orientation Lock with the phone sideways the preview
// angle stays a quarter turn (the interface didn't turn) while the
// capture angle goes to 0°/180° (the device did).
import CoreGraphics
import Foundation

@main
struct PreviewGeometryTests {
    static var failures = 0
    static var checks = 0

    static func expect(_ got: Bool, _ want: Bool, _ what: String) {
        checks += 1
        if got != want {
            failures += 1
            print("FAIL: \(what): got \(got), want \(want)")
        }
    }

    static func tall(
        _ broadcastPortrait: Bool,
        preview: CGFloat?,
        capture: CGFloat?,
        _ facing: PreviewGeometry.Facing
    ) -> Bool {
        PreviewGeometry.frameTall(
            broadcastPortrait: broadcastPortrait,
            previewAngle: preview,
            captureAngle: capture,
            facing: facing
        )
    }

    static func main() {
        let cameras: [PreviewGeometry.Facing] = [.back, .front]
        let quarter: [CGFloat] = [90, 270, -90]
        let level: [CGFloat] = [0, 180, 360, -180]

        // isPortrait: a quarter turn either way is portrait, a half turn
        // or none is landscape. Values a coordinator can hand back as
        // floating point near the whole degree still count.
        for a in quarter { expect(PreviewGeometry.isPortrait(angle: a), true, "isPortrait(\(a))") }
        for a in level { expect(PreviewGeometry.isPortrait(angle: a), false, "isPortrait(\(a))") }
        expect(PreviewGeometry.isPortrait(angle: 89.9999), true, "isPortrait(89.9999)")
        expect(PreviewGeometry.isPortrait(angle: 270.0001), true, "isPortrait(270.0001)")
        expect(PreviewGeometry.isPortrait(angle: 0.0001), false, "isPortrait(0.0001)")

        for facing in cameras {
            // Phone upright, interface upright: preview and capture both a
            // quarter turn, in any mix of 90° and 270° (the selfie camera's
            // mirrored angle must not read as turned). A portrait
            // broadcast gets a tall frame, the #214 case: the selfie
            // preview filling it exactly like the back camera's does.
            for p in quarter {
                for c in quarter {
                    expect(tall(true, preview: p, capture: c, facing), true,
                           "\(facing) upright, portrait broadcast, preview \(p) capture \(c)")
                    // A landscape broadcast (pinned at Go live, or a
                    // resume) with the phone upright: wide frame, the
                    // portrait picture letterboxed in it as viewers get it.
                    expect(tall(false, preview: p, capture: c, facing), false,
                           "\(facing) upright, landscape broadcast, preview \(p) capture \(c)")
                }
            }

            // Phone and interface both turned landscape: neither angle is
            // a quarter turn, nothing is turned.
            for p in level {
                for c in level {
                    expect(tall(false, preview: p, capture: c, facing), false,
                           "\(facing) landscape, landscape broadcast, preview \(p) capture \(c)")
                    expect(tall(true, preview: p, capture: c, facing), true,
                           "\(facing) landscape, portrait broadcast, preview \(p) capture \(c)")
                }
            }

            // Portrait Orientation Lock, phone sideways (#193): the
            // preview still turns a quarter, the capture doesn't. The
            // landscape broadcast's frame is turned with the picture, so
            // it is drawn tall in the portrait interface; a portrait
            // broadcast's frame the other way.
            for p in quarter {
                for c in level {
                    expect(tall(false, preview: p, capture: c, facing), true,
                           "\(facing) locked sideways, landscape broadcast, preview \(p) capture \(c)")
                    expect(tall(true, preview: p, capture: c, facing), false,
                           "\(facing) locked sideways, portrait broadcast, preview \(p) capture \(c)")
                }
            }

            // The mirror image of the case above (an interface left in
            // landscape while the device is held upright): the preview
            // level, the capture a quarter turn. Turned the other way.
            for p in level {
                for c in quarter {
                    expect(tall(true, preview: p, capture: c, facing), false,
                           "\(facing) preview level capture turned, portrait broadcast, preview \(p) capture \(c)")
                    expect(tall(false, preview: p, capture: c, facing), true,
                           "\(facing) preview level capture turned, landscape broadcast, preview \(p) capture \(c)")
                }
            }

            // Half a reading is no reading: the broadcast's own shape.
            for portrait in [true, false] {
                expect(tall(portrait, preview: nil, capture: 0, facing), portrait,
                       "\(facing) no preview angle, broadcast portrait \(portrait)")
                expect(tall(portrait, preview: 90, capture: nil, facing), portrait,
                       "\(facing) no capture angle, broadcast portrait \(portrait)")
                expect(tall(portrait, preview: nil, capture: nil, facing), portrait,
                       "\(facing) no reading, broadcast portrait \(portrait)")
            }
        }

        // The camera doesn't change the shape: front and back agree on
        // every pair of angles a coordinator can report.
        let all = quarter + level
        for portrait in [true, false] {
            for p in all {
                for c in all {
                    expect(tall(portrait, preview: p, capture: c, .front),
                           tall(portrait, preview: p, capture: c, .back),
                           "front == back, broadcast portrait \(portrait), preview \(p) capture \(c)")
                }
            }
        }

        print("preview geometry: \(checks - failures)/\(checks) checks passed")
        exit(failures == 0 ? 0 : 1)
    }
}
