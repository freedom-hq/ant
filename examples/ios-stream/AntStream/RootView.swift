import SwiftUI

/// Three-tab shell, as on Android: Broadcast (going live), Watch (play a
/// channel) and Storage (plan / account). iOS 26 renders the tab bar as
/// a floating glass pill for free.
struct RootView: View {
    @EnvironmentObject var node: AntNode

    enum TabID: Hashable { case broadcast, watch, storage }

    @State private var tab: TabID = RootView.shotArgs.contains("-antstream-shot-watch")
        ? .watch : .broadcast
    /// The Watch tab's reference field and what it plays. Owned here so
    /// the Broadcast tab's channel card can hand a channel over.
    @State private var watchReference = RootView.shotArgs.contains("-antstream-shot-watch")
        ? Channel.sampleReference : ""
    @State private var watchPlaying: String? = RootView.shotArgs.contains("-antstream-shot-watch")
        ? Channel.sampleReference : nil

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
    /// the app boots into the tab shell exactly as before.
    static let shotArgs = Set(ProcessInfo.processInfo.arguments)

    var body: some View {
        if Self.shotArgs.contains("-antstream-shot-live")
            || Self.shotArgs.contains("-antstream-shot-live-idle") {
            // The going-live screen (#67 stage 2 / #65) full-screen, so
            // CI can capture a broadcast actually running: `LiveView`
            // sees the same flag and starts one from the generated test
            // pattern, since a runner has neither a camera nor a tap
            // driver. `-antstream-shot-live-idle` stops short of going
            // live, to capture the quality picker.
            LiveView().preferredColorScheme(.dark)
        } else if Self.shotArgs.contains("-antstream-shot-getstarted") {
            // The buy flow (Get Started) full-screen, so CI can capture
            // the plan picker and — with `-antstream-shot-getstarted-pay`
            // — the payment cost breakdown, including the new one-time
            // settlement-deposit line. Otherwise it sits behind a sheet.
            GetStartedView().preferredColorScheme(.dark)
        } else if Self.shotArgs.contains("-antstream-shot-storage") {
            // The Storage tab full-screen (settlement / plan / account
            // cards). `-antstream-shot-deposit` additionally drives it to
            // the deposit-0 top-up state, `-antstream-shot-credit` to the
            // Credit card with the account wallet (see StorageView).
            StorageView().preferredColorScheme(.dark)
        } else {
            tabs
        }
    }

    /// `-antstream-shot-channel`, `-antstream-shot-unfinished` and
    /// `-antstream-shot-watch` land here too: the first two on the
    /// Broadcast tab's channel card and unfinished-broadcast card
    /// (BroadcastView installs the sample state), the last on the Watch
    /// tab already playing a sample channel (see the `@State` seeds
    /// above).
    private var tabs: some View {
        TabView(selection: $tab) {
            Tab("Broadcast", systemImage: "dot.radiowaves.left.and.right", value: TabID.broadcast) {
                BroadcastView(onWatch: { watch($0) })
            }
            Tab("Watch", systemImage: "play.rectangle.fill", value: TabID.watch) {
                WatchView(reference: $watchReference, playing: $watchPlaying)
            }
            Tab("Storage", systemImage: "externaldrive.fill", value: TabID.storage) {
                StorageView()
            }
        }
        .tabBarMinimizeBehavior(.onScrollDown)
        .preferredColorScheme(.dark)
    }

    /// Open a channel on the Watch tab, already playing.
    private func watch(_ reference: String) {
        watchReference = reference
        watchPlaying = WatchView.normalised(reference)
        tab = .watch
    }
}
