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
        pinned: Bool = true,
        preview: CGFloat?,
        capture: CGFloat?
    ) -> Bool {
        PreviewGeometry.frameTall(
            broadcastPortrait: broadcastPortrait,
            pinned: pinned,
            previewAngle: preview,
            captureAngle: capture
        )
    }

    static func main() {
        // The back camera's portrait angle is 90°; the mirrored front
        // camera's can be 270° (or -90°). The decision takes no camera at
        // all, so these sets are where the front camera is covered: every
        // check below runs the front camera's angles as well as the back
        // camera's, against a fixed expected shape.
        let back: CGFloat = 90
        let frontQuarter: [CGFloat] = [270, -90]
        let quarter: [CGFloat] = [back] + frontQuarter
        let level: [CGFloat] = [0, 180, 360, -180]

        // isPortrait: a quarter turn either way is portrait, a half turn
        // or none is landscape. Values a coordinator can hand back as
        // floating point near the whole degree still count.
        for a in quarter { expect(PreviewGeometry.isPortrait(angle: a), true, "isPortrait(\(a))") }
        for a in level { expect(PreviewGeometry.isPortrait(angle: a), false, "isPortrait(\(a))") }
        expect(PreviewGeometry.isPortrait(angle: 89.9999), true, "isPortrait(89.9999)")
        expect(PreviewGeometry.isPortrait(angle: 270.0001), true, "isPortrait(270.0001)")
        expect(PreviewGeometry.isPortrait(angle: 0.0001), false, "isPortrait(0.0001)")

        // #214 itself: selfie camera, phone upright, a portrait broadcast
        // (pinned for a resume, or live). Preview and capture both at the
        // front camera's mirrored 270°, or one of them at 90°: a tall
        // frame, filled.
        for p in frontQuarter {
            for c in quarter {
                expect(tall(true, preview: p, capture: c), true,
                       "front upright, resume portrait, preview \(p) capture \(c)")
                expect(tall(true, preview: c, capture: p), true,
                       "front upright, resume portrait, preview \(c) capture \(p)")
                expect(tall(true, pinned: false, preview: p, capture: c), true,
                       "front upright, live portrait, preview \(p) capture \(c)")
            }
        }

        // Pinned: only a resume's shape before Go live (#175); live
        // follows the phone (#228, below). Phone upright, interface
        // upright: preview and capture both a quarter turn, in any mix of
        // 90° and 270°.
        for p in quarter {
            for c in quarter {
                expect(tall(true, preview: p, capture: c), true,
                       "upright, resume portrait, preview \(p) capture \(c)")
                // Resuming a landscape broadcast with the phone upright:
                // wide frame, the portrait picture letterboxed in it as
                // it would start out until the turn has held.
                expect(tall(false, preview: p, capture: c), false,
                       "upright, resume landscape, preview \(p) capture \(c)")
            }
        }

        // Phone and interface both turned landscape: neither angle is a
        // quarter turn, nothing is turned.
        for p in level {
            for c in level {
                expect(tall(false, preview: p, capture: c), false,
                       "landscape, resume landscape, preview \(p) capture \(c)")
                expect(tall(true, preview: p, capture: c), true,
                       "landscape, resume portrait, preview \(p) capture \(c)")
            }
        }

        // Portrait Orientation Lock, phone sideways (#193): the preview
        // still turns a quarter, the capture doesn't. The landscape
        // broadcast's frame is turned with the picture, so it is drawn
        // tall in the portrait interface; a portrait broadcast's frame
        // the other way.
        for p in quarter {
            for c in level {
                expect(tall(false, preview: p, capture: c), true,
                       "locked sideways, resume landscape, preview \(p) capture \(c)")
                expect(tall(true, preview: p, capture: c), false,
                       "locked sideways, resume portrait, preview \(p) capture \(c)")
            }
        }

        // The mirror image of the case above (an interface left in
        // landscape while the device is held upright): the preview level,
        // the capture a quarter turn. Turned the other way.
        for p in level {
            for c in quarter {
                expect(tall(true, preview: p, capture: c), false,
                       "preview level capture turned, resume portrait, preview \(p) capture \(c)")
                expect(tall(false, preview: p, capture: c), true,
                       "preview level capture turned, resume landscape, preview \(p) capture \(c)")
            }
        }

        // Before Go live (not pinned) the frame is the shape the preview
        // shows, whatever the capture engine's own (possibly stale)
        // reading says the broadcast is, and whatever the capture angle:
        // the engine's coordinator isn't mixed with the preview's.
        // E.g. lock off, phone just turned landscape, the preview already
        // at 0° but the engine still reporting portrait: a wide frame,
        // not a tall one around a landscape picture.
        let all = quarter + level
        for engine in [true, false] {
            for c in all + [nil] {
                for p in quarter {
                    expect(tall(engine, pinned: false, preview: p, capture: c), true,
                           "not live, engine portrait \(engine), preview \(p) capture \(String(describing: c))")
                }
                for p in level {
                    expect(tall(engine, pinned: false, preview: p, capture: c), false,
                           "not live, engine portrait \(engine), preview \(p) capture \(String(describing: c))")
                }
            }
        }

        // Live (#228): not pinned, the broadcast follows the phone, so
        // the frame is the preview's shape exactly as before Go live:
        // the live preview fills like the pre-Go-live one. Turned to
        // landscape (lock off): wide at once, while the engine still
        // reports portrait for the second it waits out the turn, and
        // after. Turned back: tall.
        for engine in [true, false] {
            for p in level {
                for c in level {
                    expect(tall(engine, pinned: false, preview: p, capture: c), false,
                           "live, turned landscape, engine portrait \(engine), preview \(p) capture \(c)")
                }
            }
            for p in quarter {
                for c in quarter {
                    expect(tall(engine, pinned: false, preview: p, capture: c), true,
                           "live, turned back portrait, engine portrait \(engine), preview \(p) capture \(c)")
                }
            }
        }
        // Live under Portrait Orientation Lock, phone sideways: the
        // broadcast takes the device as held (landscape), and the preview
        // shows that landscape picture turned a quarter in the portrait
        // interface, so the frame is drawn tall around it: the shape
        // viewers get, as the picture stands on this screen. The same as
        // the pinned landscape case above, now for a live broadcast that
        // turned to landscape (engine false) or is about to (true).
        for engine in [true, false] {
            for p in quarter {
                for c in level {
                    expect(tall(engine, pinned: false, preview: p, capture: c),
                           tall(false, preview: p, capture: c),
                           "live, locked sideways, engine portrait \(engine), preview \(p) capture \(c)")
                }
            }
        }

        // Half a reading is no reading: the broadcast's own shape. Live
        // or not (unpinned), no preview angle is the only missing half
        // that counts.
        for portrait in [true, false] {
            expect(tall(portrait, preview: nil, capture: 0), portrait,
                   "resume, no preview angle, broadcast portrait \(portrait)")
            expect(tall(portrait, preview: 90, capture: nil), portrait,
                   "resume, no capture angle, broadcast portrait \(portrait)")
            expect(tall(portrait, preview: nil, capture: nil), portrait,
                   "resume, no reading, broadcast portrait \(portrait)")
            expect(tall(portrait, pinned: false, preview: nil, capture: 90), portrait,
                   "unpinned, no preview angle, broadcast portrait \(portrait)")
            expect(tall(portrait, pinned: false, preview: nil, capture: nil), portrait,
                   "unpinned, no reading, broadcast portrait \(portrait)")
        }

        print("preview geometry: \(checks - failures)/\(checks) checks passed")
        exit(failures == 0 ? 0 : 1)
    }
}
