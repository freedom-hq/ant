import SwiftUI

/// Two-tab shell: Broadcast (going live) and Storage (plan / account).
/// iOS 26 renders the tab bar as a floating glass pill for free.
struct RootView: View {
    @EnvironmentObject var node: AntNode

    /// Launch-argument hooks for the `antstream-visual` CI workflow.
    ///
    /// The macOS runner has no tap driver, so a screenshot can only ever
    /// capture whatever the app opens *to*. These flags let the workflow
    /// open the app straight onto a Storage surface (the Storage tab, or
    /// the buy-flow sheet) instead of the default Broadcast tab, so a PR
    /// touching those surfaces gets visual evidence of its own UI. Same
    /// channel as `-antstreamShowBench` on the Broadcast side.
    ///
    /// Inert in normal use: nothing passes these arguments outside CI, so
    /// the app boots into the two-tab shell exactly as before.
    static let shotArgs = Set(ProcessInfo.processInfo.arguments)

    var body: some View {
        if Self.shotArgs.contains("-antstream-shot-getstarted") {
            // The buy flow (Get Started) full-screen, so CI can capture
            // the plan picker and — with `-antstream-shot-getstarted-pay`
            // — the payment cost breakdown, including the new one-time
            // settlement-deposit line. Otherwise it sits behind a sheet.
            GetStartedView().preferredColorScheme(.dark)
        } else if Self.shotArgs.contains("-antstream-shot-storage") {
            // The Storage tab full-screen (settlement / plan / account
            // cards). `-antstream-shot-deposit` additionally drives it to
            // the deposit-0 top-up state (see StorageView).
            StorageView().preferredColorScheme(.dark)
        } else {
            tabs
        }
    }

    private var tabs: some View {
        TabView {
            Tab("Broadcast", systemImage: "dot.radiowaves.left.and.right") {
                BroadcastView()
            }
            Tab("Storage", systemImage: "externaldrive.fill") {
                StorageView()
            }
        }
        .tabBarMinimizeBehavior(.onScrollDown)
        .preferredColorScheme(.dark)
    }
}
