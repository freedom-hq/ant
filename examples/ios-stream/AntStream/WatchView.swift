import AVKit
import SwiftUI

/// The Watch tab: plays a channel through the in-process gateway, ported
/// from the Android app's `WatchScreen.kt`.
///
/// A channel is published in the Solar Punk stream format (issue #167):
/// every feed update *is* the newest playlist, read with
/// `GET /feeds/{owner}/{topic}`, and it lists segments by bare
/// reference. The channel reference is the feed manifest naming that
/// owner and topic (``FeedAddress``); ``FeedPlaylistLoader`` hands
/// `AVPlayer` each playlist read with the bare references turned into
/// `<gateway>/bytes/<ref>`, the rewrite the Solar Punk viewers do, and
/// `AVPlayer`'s HLS engine follows the live edge on its own. Loopback
/// HTTP is allowed by the app's ATS exceptions (Info.plist).
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
        if let gateway = node.gatewayURL {
            watcher.play(reference: ref, gateway: gateway, restart: true)
        }
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

    private func resume() {
        if let playing, let gateway = node.gatewayURL {
            watcher.play(reference: playing, gateway: gateway)
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
                if playing != nil, let url = watcher.feedURL {
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

    /// Where the playlist is read from once the channel is resolved
    /// (`<gateway>/feeds/<owner>/<topic>`), shown under the player.
    @Published private(set) var feedURL: URL?

    private struct Source: Equatable {
        let reference: String
        let gateway: URL
    }

    private var source: Source?
    /// The feed a channel reference names, once read. A reference never
    /// changes what it names, so a retry does not read it again.
    private var resolved: (reference: String, address: FeedAddress)?
    /// Held here: `AVAssetResourceLoader` keeps its delegate weakly.
    private var loader: FeedPlaylistLoader?
    private var loadTask: Task<Void, Never>?
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

    /// Play the channel `reference` names, through `gateway`. A no-op
    /// when it is already playing or loading, unless `restart`.
    func play(reference: String, gateway: URL, restart: Bool = false) {
        let next = Source(reference: reference, gateway: gateway)
        guard restart || next != source || (player.currentItem == nil && loadTask == nil)
        else { return }
        source = next
        message = nil
        load()
    }

    func stop() {
        source = nil
        loadTask?.cancel()
        loadTask = nil
        watchdog?.cancel()
        retryTask?.cancel()
        player.replaceCurrentItem(with: nil)
        loader = nil
        feedURL = nil
        message = nil
        buffering = false
    }

    private func load() {
        guard let source else { return }
        retryTask?.cancel()
        watchdog?.cancel()
        loadTask?.cancel()
        buffering = true
        loadTask = Task { @MainActor [weak self] in
            let address: FeedAddress
            do {
                if let known = self?.resolved, known.reference == source.reference {
                    address = known.address
                } else {
                    address = try await FeedAddress.resolve(
                        reference: source.reference, gateway: source.gateway)
                }
            } catch {
                guard let self, !Task.isCancelled, self.source == source else { return }
                self.loadTask = nil
                self.retry("Can't find this channel yet (\(error.localizedDescription)); retrying…")
                return
            }
            guard let self, !Task.isCancelled, self.source == source else { return }
            self.loadTask = nil
            self.resolved = (source.reference, address)
            let loader = FeedPlaylistLoader(gateway: source.gateway, address: address)
            self.loader = loader
            self.feedURL = loader.feedURL
            let asset = AVURLAsset(url: loader.playlistURL)
            asset.resourceLoader.setDelegate(loader, queue: loader.queue)
            let item = AVPlayerItem(asset: asset)
            self.player.replaceCurrentItem(with: item)
            self.player.play()
            self.watch(item)
        }
    }

    private func retry(_ text: String) {
        message = text
        buffering = false
        watchdog?.cancel()
        retryTask?.cancel()
        retryTask = Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: Self.retryDelay)
            guard !Task.isCancelled, let self, self.source != nil else { return }
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

/// A channel's feed: the account that writes it and its 32-byte topic,
/// both lowercase hex — everything `GET /feeds/{owner}/{topic}` needs.
struct FeedAddress: Equatable {
    let owner: String
    let topic: String

    enum ResolveError: LocalizedError {
        case unreadable(Int)
        case notAChannel

        var errorDescription: String? {
            switch self {
            case .unreadable(let code): "the reference can't be read (\(code))"
            case .notAChannel: "the reference is not a channel"
            }
        }
    }

    /// The feed a channel reference names. The reference is the feed
    /// manifest `POST /feeds` made: one mantaray node whose `/` fork
    /// carries the feed as JSON metadata, `"swarm-feed-owner":"<40 hex>"`
    /// and `"swarm-feed-topic":"<64 hex>"`, in the clear (bee and ant
    /// write feed manifests unobfuscated). The node is read raw with
    /// `GET /bytes`, since its `/bzz` path can't reach a v2 update's
    /// content.
    static func resolve(reference: String, gateway: URL) async throws -> FeedAddress {
        let url = gateway.appending(path: "bytes/\(reference)")
        let request = URLRequest(url: url, timeoutInterval: 60)
        let (data, response) = try await URLSession.shared.data(for: request)
        let code = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard code == 200 else { throw ResolveError.unreadable(code) }
        guard let address = parse(manifest: data) else { throw ResolveError.notAChannel }
        return address
    }

    /// The owner and topic in a feed manifest node's bytes, or `nil`.
    static func parse(manifest data: Data) -> FeedAddress? {
        guard let owner = hex(after: "swarm-feed-owner", length: 40, in: data),
              let topic = hex(after: "swarm-feed-topic", length: 64, in: data)
        else { return nil }
        return FeedAddress(owner: owner, topic: topic)
    }

    private static func hex(after key: String, length: Int, in data: Data) -> String? {
        guard let key = data.range(of: Data("\"\(key)\":\"".utf8)) else { return nil }
        let value = data[key.upperBound...].prefix(length)
        guard value.count == length,
              let text = String(data: value, encoding: .ascii),
              text.allSatisfy(\.isHexDigit)
        else { return nil }
        return text.lowercased()
    }
}

/// Serves a channel's live playlist to `AVPlayer`: every request for
/// ``playlistURL`` (the first load and each live reload) reads
/// `GET <gateway>/feeds/<owner>/<topic>`, whose body is the newest
/// playlist, and answers with its bare segment and map references
/// turned into `<gateway>/bytes/<ref>` — the rewrite Solar Punk's
/// viewers do, which leaves the publisher out of the choice of gateway.
/// Segments themselves are plain `http://` URLs, fetched by `AVPlayer`
/// directly.
///
/// A custom URL scheme is what routes the playlist through a
/// resource-loader delegate at all; `AVPlayer` never asks one for
/// `http` URLs.
final class FeedPlaylistLoader: NSObject, AVAssetResourceLoaderDelegate {
    static let scheme = "antstream-feed"

    let gateway: URL
    let address: FeedAddress
    let queue = DispatchQueue(label: "antstream.feed-playlist")

    init(gateway: URL, address: FeedAddress) {
        self.gateway = gateway
        self.address = address
    }

    /// What `AVPlayer` is given. The `.m3u8` suffix is what tells it the
    /// resource is an HLS playlist.
    var playlistURL: URL {
        URL(string: "\(Self.scheme)://feed/\(address.owner)/\(address.topic).m3u8")!
    }

    var feedURL: URL {
        gateway.appending(path: "feeds/\(address.owner)/\(address.topic)")
    }

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        shouldWaitForLoadingOfRequestedResource loadingRequest: AVAssetResourceLoadingRequest
    ) -> Bool {
        guard loadingRequest.request.url?.scheme == Self.scheme else { return false }
        // Feed reads are mutable: never answer a live reload from cache.
        let request = URLRequest(
            url: feedURL, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 60)
        let gateway = self.gateway
        URLSession.shared.dataTask(with: request) { data, response, error in
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            guard error == nil, code == 200, let data,
                  let playlist = String(data: data, encoding: .utf8)
            else {
                // A channel that has not gone live yet 404s here; the
                // item fails and the watcher retries it.
                loadingRequest.finishLoading(with: error ?? NSError(
                    domain: NSURLErrorDomain,
                    code: code == 404 ? NSURLErrorFileDoesNotExist : NSURLErrorBadServerResponse))
                return
            }
            let body = Data(Self.rewrite(playlist, gateway: gateway).utf8)
            if let info = loadingRequest.contentInformationRequest {
                info.contentType = "public.m3u-playlist"
                info.contentLength = Int64(body.count)
                info.isByteRangeAccessSupported = false
            }
            loadingRequest.dataRequest?.respond(with: body)
            loadingRequest.finishLoading()
        }.resume()
        return true
    }

    /// `playlist` with each bare reference — a segment line, or the URI
    /// of an `#EXT-X-MAP` — made absolute on `gateway`'s `/bytes/`.
    /// Everything else, absolute URLs included, passes through.
    static func rewrite(_ playlist: String, gateway: URL) -> String {
        let map = "#EXT-X-MAP:URI=\""
        return playlist.split(separator: "\n", omittingEmptySubsequences: false)
            .map { line -> String in
                if let ref = reference(line) {
                    return bytesURL(ref, gateway)
                }
                if line.hasPrefix(map), line.hasSuffix("\""),
                   let ref = reference(line.dropFirst(map.count).dropLast()) {
                    return map + bytesURL(ref, gateway) + "\""
                }
                return String(line)
            }
            .joined(separator: "\n")
    }

    /// A bare Swarm reference: 64 hex (128 for an encrypted one).
    private static func reference(_ text: Substring) -> String? {
        let value = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard value.count == 64 || value.count == 128, value.allSatisfy(\.isHexDigit)
        else { return nil }
        return value.lowercased()
    }

    private static func bytesURL(_ ref: String, _ gateway: URL) -> String {
        gateway.appending(path: "bytes/\(ref)").absoluteString
    }
}
