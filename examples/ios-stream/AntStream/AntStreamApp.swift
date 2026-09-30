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
