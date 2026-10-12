import AVKit
import SwiftUI

/// The Watch tab: plays a channel through the in-process gateway, ported
/// from the Android app's `WatchScreen.kt`.
///
/// The channel reference is a feed manifest, so
/// `/bzz/<ref>/stream.m3u8` re-resolves the feed to the newest playlist
/// on every refresh and `AVPlayer`'s HLS engine follows the live edge on
/// its own. Loopback HTTP is allowed by the app's ATS exceptions
/// (Info.plist).
///
/// Neither failure a viewer runs into is final: a channel that is not
/// live yet (the playlist 404s) and a live playlist that stops changing
/// (a broadcast that ended without closing its playlist, or a feed
/// update that hasn't propagated) both reload the channel after a short
/// pause, with a plain message saying so — which also picks up the next
/// broadcast on the same channel.
struct WatchView: View {
    @EnvironmentObject var node: AntNode
    /// The reference field. Pre-filled by the channel card's Watch
    /// button, which also sets ``playing``.
    @Binding var reference: String
    /// The normalised reference being played, or `nil`.
    @Binding var playing: String?
    @StateObject private var watcher = ChannelWatcher()

    init(reference: Binding<String>, playing: Binding<String?>) {
        _reference = reference
        _playing = playing
    }

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .network)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    referenceCard
                    if playing != nil { playerCard }
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 130)
            }
            .scrollIndicators(.hidden)
            .scrollDismissesKeyboard(.interactively)
        }
        .preferredColorScheme(.dark)
        // The player only runs while the tab is on screen, like the
        // Android screen releasing its player when it goes away.
        .onAppear { resume() }
        .onDisappear { watcher.stop() }
        .onChange(of: playing) { _, _ in resume() }
        .onChange(of: node.gatewayAddress) { _, _ in resume() }
    }

    // MARK: header

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Watch")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text(node.gatewayUp ? "Plays through this device's node" : "Waiting for the gateway…")
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
        }
        .padding(.horizontal, 6)
    }

    // MARK: reference

    private var referenceCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("CHANNEL REFERENCE")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                TextField("Paste a channel reference", text: $reference)
                    .textFieldStyle(.plain)
                    .font(.footnote.monospaced())
                    .foregroundStyle(.white)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .submitLabel(.go)
                    .onSubmit { play() }
                    .padding(12)
                    .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                GlassPillButton(title: "Play", icon: "play.fill",
                                tint: .blue.opacity(canPlay ? 0.4 : 0.12)) {
                    play()
                }
                .disabled(!canPlay)
                .opacity(canPlay ? 1 : 0.5)
            }
        }
    }

    private var canPlay: Bool { Self.normalised(reference).count >= 64 }

    private func play() {
        let ref = Self.normalised(reference)
        guard ref.count >= 64 else { return }
        playing = ref
        // Play again even when it's the same channel: that is how a
        // viewer retries by hand.
        if let url = streamURL(for: ref) { watcher.play(url, restart: true) }
    }

    /// Accepts a bare reference, `bzz://<ref>`, `0x<ref>` or a web player
    /// link (`…/#<ref>`, what Share sends).
    static func normalised(_ input: String) -> String {
        var s = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if let hash = s.lastIndex(of: "#") { s = String(s[s.index(after: hash)...]) }
        for prefix in ["bzz://", "0x"] where s.hasPrefix(prefix) {
            s = String(s.dropFirst(prefix.count))
        }
        while s.hasSuffix("/") { s.removeLast() }
        return s.lowercased()
    }

    private func streamURL(for ref: String) -> URL? {
        node.gatewayURL?.appending(path: "bzz/\(ref)/stream.m3u8")
    }

    private func resume() {
        if let playing, let url = streamURL(for: playing) {
            watcher.play(url)
        } else {
            watcher.stop()
        }
    }

    // MARK: player

    private var playerCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                VideoPlayer(player: watcher.player)
                    .aspectRatio(16 / 9, contentMode: .fit)
                    .clipShape(.rect(cornerRadius: 16))
                if let message = watcher.message {
                    Label(message, systemImage: "arrow.clockwise")
                        .font(.footnote)
                        .foregroundStyle(.orange)
                } else if watcher.buffering {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Buffering…")
                            .font(.footnote)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }
                if let playing, let url = streamURL(for: playing) {
                    Text(url.absoluteString)
                        .font(.caption2.monospaced())
                        .foregroundStyle(.white.opacity(0.5))
                        .lineLimit(2)
                        .truncationMode(.middle)
                        .textSelection(.enabled)
                }
            }
        }
    }
}

/// Owns the `AVPlayer` for the Watch tab and keeps it on the channel:
/// a failed item, an item that never loads, and a live playlist that
/// stops moving each reload the channel after a pause. Polled once a
/// second rather than observed, so the one loop decides everything.
@MainActor
final class ChannelWatcher: ObservableObject {
    let player = AVPlayer()
    /// The plain-language reason the player is retrying, or why it
    /// stopped; `nil` while video flows.
    @Published private(set) var message: String?
    @Published private(set) var buffering = false

    private var url: URL?
    private var watchdog: Task<Void, Never>?
    private var retryTask: Task<Void, Never>?

    /// Pause before reloading a channel that failed, as on Android.
    private static let retryDelay: UInt64 = 8_000_000_000
    /// How long a live playlist may sit at the same position, waiting
    /// for a segment that doesn't come, before the channel is reloaded.
    private static let stuckSeconds = 20
    /// How long an item may take to load at all (a cold feed lookup
    /// through the network) before it is reloaded.
    private static let loadSeconds = 60

    /// Play `url`. A no-op when it is already playing, unless `restart`.
    func play(_ url: URL, restart: Bool = false) {
        guard restart || url != self.url || player.currentItem == nil else { return }
        self.url = url
        message = nil
        load()
    }

    func stop() {
        url = nil
        watchdog?.cancel()
        retryTask?.cancel()
        player.replaceCurrentItem(with: nil)
        message = nil
        buffering = false
    }

    private func load() {
        guard let url else { return }
        retryTask?.cancel()
        let item = AVPlayerItem(url: url)
        player.replaceCurrentItem(with: item)
        player.play()
        buffering = true
        watch(item)
    }

    private func retry(_ text: String) {
        message = text
        buffering = false
        watchdog?.cancel()
        retryTask?.cancel()
        retryTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: Self.retryDelay)
            guard !Task.isCancelled, let self, self.url != nil else { return }
            self.load()
        }
    }

    private func watch(_ item: AVPlayerItem) {
        watchdog?.cancel()
        watchdog = Task { @MainActor [weak self] in
            var lastTime = CMTime.invalid
            var stuck = 0
            var loading = 0
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                guard !Task.isCancelled, let self, self.player.currentItem === item else { return }
                switch item.status {
                case .failed:
                    // A 404 on a channel that is not live yet lands here
                    // as well as a real error. Neither is final.
                    let code = (item.error as NSError?)?.code ?? 0
                    self.retry("Can't load the channel yet (error \(code)); retrying…")
                    return
                case .unknown:
                    loading += 1
                    if loading >= Self.loadSeconds {
                        self.retry("The channel is taking long to load; retrying…")
                        return
                    }
                    continue
                case .readyToPlay:
                    break
                @unknown default:
                    break
                }
                // A closed playlist (`#EXT-X-ENDLIST`) played to the end:
                // the broadcast is over, and replaying its last seconds
                // in a loop would help nobody.
                if item.duration.isNumeric,
                   CMTimeCompare(item.currentTime(), item.duration) >= 0 {
                    self.buffering = false
                    self.message = "The broadcast has ended."
                    return
                }
                let status = self.player.timeControlStatus
                let waiting = status == .waitingToPlayAtSpecifiedRate
                if self.buffering != waiting { self.buffering = waiting }
                // Clear a stale retry message once video flows again.
                if status == .playing, self.message != nil { self.message = nil }
                let now = item.currentTime()
                if waiting, lastTime.isValid, CMTimeCompare(now, lastTime) == 0 {
                    stuck += 1
                } else {
                    stuck = 0
                }
                lastTime = now
                if stuck >= Self.stuckSeconds {
                    self.retry("No new video from this channel. The broadcast may have ended "
                               + "without closing; checking again…")
                    return
                }
            }
        }
    }
}
