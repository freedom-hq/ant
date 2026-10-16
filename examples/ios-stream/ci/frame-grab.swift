// Grab one frame of a movie the way AVFoundation presents it (track
// transform applied) and write it as PNG, printing the track's natural
// size and preferred transform (#194).
//
//     swift frame-grab.swift <movie.mp4> <out.png> [seconds]
//
// Used by the antstream-visual workflow on the iOS app's own portrait
// test-pattern segments: the pattern's white disc must come out round
// in a portrait frame.
import AVFoundation
import CoreGraphics
import Foundation
import ImageIO
import UniformTypeIdentifiers

let args = CommandLine.arguments
guard args.count >= 3 else {
    FileHandle.standardError.write("usage: frame-grab <movie> <out.png> [seconds]\n".data(using: .utf8)!)
    exit(2)
}
let asset = AVURLAsset(url: URL(fileURLWithPath: args[1]))
let at = CMTime(seconds: args.count > 3 ? Double(args[3]) ?? 1 : 1, preferredTimescale: 600)

let done = DispatchSemaphore(value: 0)
var status: Int32 = 0
Task {
    defer { done.signal() }
    do {
        guard let track = try await asset.loadTracks(withMediaType: .video).first else {
            print("no video track"); status = 1; return
        }
        let (size, transform) = try await track.load(.naturalSize, .preferredTransform)
        print("AVFoundation natural size: \(Int(size.width))x\(Int(size.height))")
        print("AVFoundation preferred transform: \(transform)\(transform.isIdentity ? " (identity)" : "")")
        let generator = AVAssetImageGenerator(asset: asset)
        generator.appliesPreferredTrackTransform = true
        generator.requestedTimeToleranceBefore = .positiveInfinity
        generator.requestedTimeToleranceAfter = .positiveInfinity
        let (image, _) = try await generator.image(at: at)
        print("frame as presented: \(image.width)x\(image.height)")
        let out = URL(fileURLWithPath: args[2]) as CFURL
        guard let dest = CGImageDestinationCreateWithURL(out, UTType.png.identifier as CFString, 1, nil) else {
            print("cannot write \(args[2])"); status = 1; return
        }
        CGImageDestinationAddImage(dest, image, nil)
        if !CGImageDestinationFinalize(dest) { print("cannot write \(args[2])"); status = 1 }
    } catch {
        print("frame grab failed: \(error)")
        status = 1
    }
}
done.wait()
exit(status)
