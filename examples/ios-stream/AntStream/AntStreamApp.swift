import SwiftUI
import UIKit

/// AntStream — live video from the device camera onto Swarm.
///
/// Two tabs: **Broadcast** (go live; the capture and publish pipeline
/// lands here in the follow-up tickets) and **Storage** (the storage
/// plan, account balance, and account key). Both talk to a single
/// embedded `ant-node` through the C FFI wrapped by `AntNode`, plus the
/// in-process bee gateway that node serves on localhost.
///
/// Lifecycle: iOS gives the app no execution in the background, so on
/// `.background` we spend the short grace window inside a
/// `beginBackgroundTask` letting `ant_suspend` checkpoint node state.
/// `.active` undoes it with `ant_wake` (plus `ant_resume`, which re-dials
/// the peer set the OS reaped, and a gateway rebind). Ported from
/// `examples/ios-drive/AntDrive/AntDriveApp.swift`; AntStream has no
/// deferred upload queue to finish, so there is no `BGProcessingTask`
/// counterpart here — a live broadcast is foreground-only by nature.
@main
struct AntStreamApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var node = AntNode()
    @Environment(\.scenePhase) private var scenePhase

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(node)
                .task { await node.start() }
        }
        .onChange(of: scenePhase) { _, phase in
            switch phase {
            case .background:
                Self.enterBackground(node: node)
            case .active:
                Task {
                    await node.wake()
                    // A broadcast left running is carried on, and this
                    // return buys it a fresh reap window (#172) to get
                    // its first segment back up (or, if the window ran
                    // out while it was paused, ends it now that the
                    // closing uploads can land).
                    if !node.isOffline { await node.resumePublisher() }
                    await node.refreshAll()
                }
            default:
                break
            }
        }
    }

    /// Spend the backgrounding grace window well: keep the process alive
    /// with a `beginBackgroundTask` while `ant_suspend` drains and
    /// checkpoints node state, then end the task so the system doesn't
    /// kill us for overstaying.
    private static func enterBackground(node: AntNode) {
        let app = UIApplication.shared
        var bgTask: UIBackgroundTaskIdentifier = .invalid
        bgTask = app.beginBackgroundTask(withName: "antstream.suspend") {
            if bgTask != .invalid {
                app.endBackgroundTask(bgTask)
                bgTask = .invalid
            }
        }
        Task {
            await node.suspend()
            await MainActor.run {
                if bgTask != .invalid {
                    app.endBackgroundTask(bgTask)
                    bgTask = .invalid
                }
            }
        }
    }
}


/// Only here for the orientation lock: SwiftUI has no scene-level way to
/// say which orientations a screen may take.
@MainActor
final class AppDelegate: NSObject, UIApplicationDelegate {
    func application(
        _ application: UIApplication,
        supportedInterfaceOrientationsFor window: UIWindow?
    ) -> UIInterfaceOrientationMask {
        OrientationLock.mask
    }
}

/// Which way the interface may face (#193). On a phone the tabs stay in
/// portrait, as before; the live screen lets the interface turn with the
/// device (``LiveView`` calls ``allowRotation(_:)`` while it is shown),
/// so its controls re-lay out for landscape. The Info.plist lists every
/// orientation either may use; this narrows it. An iPad keeps all four
/// everywhere (#192).
@MainActor
enum OrientationLock {
    private static var rotates = false

    static var mask: UIInterfaceOrientationMask {
        if UIDevice.current.userInterfaceIdiom == .pad { return .all }
        return rotates ? .allButUpsideDown : .portrait
    }

    /// Let the interface follow the device (`true`), or bring it back to
    /// portrait (`false`).
    static func allowRotation(_ allow: Bool) {
        guard allow != rotates else { return }
        rotates = allow
        for scene in UIApplication.shared.connectedScenes {
            guard let scene = scene as? UIWindowScene else { continue }
            // Every controller up the presentation chain re-reads the
            // mask: the live screen is a full-screen cover.
            for window in scene.windows {
                var controller = window.rootViewController
                while let current = controller {
                    current.setNeedsUpdateOfSupportedInterfaceOrientations()
                    controller = current.presentedViewController
                }
            }
            if !allow, UIDevice.current.userInterfaceIdiom != .pad {
                scene.requestGeometryUpdate(.iOS(interfaceOrientations: .portrait))
            }
        }
    }

    /// Turn the interface to landscape without turning the device. Only
    /// for `antstream-visual`: a simulator on a CI runner can't be
    /// rotated from the workflow.
    static func requestLandscape() {
        for scene in UIApplication.shared.connectedScenes {
            (scene as? UIWindowScene)?.requestGeometryUpdate(
                .iOS(interfaceOrientations: .landscapeRight)
            )
        }
    }
}
