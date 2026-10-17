import AVKit
import SwiftUI

/// The Watch tab: plays a channel through the in-process gateway, ported
/// from the Android app's `WatchScreen.kt`.
///
/// A channel is an owner and a topic (``ChannelAddress``): typed as
/// `0x<owner>/<topic>`, Solar Punk's `swarm://<owner>/<topic>`, or a web
/// player link; the field starts on this account's own channel. Channels
/// are in the publisher's Solar Punk format (issue #167) only, where every
/// feed update *is* the newest playlist (`GET /feeds/{owner}/{topic}`)
/// listing bare references, turned into `<gateway>/bytes/<ref>` the way
/// the Solar Punk viewers do (``FeedPlaylistLoader``). `AVPlayer`'s HLS
/// engine follows the live edge on its own, and plays a recording (the
/// feed's final update once a broadcast ends) from the start. Loopback
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
    /// The channel field. Pre-filled by the channel card's Watch button,
    /// which also sets ``playing``; this account's channel otherwise.
    @Binding var reference: String
    /// The channel being played (``ChannelAddress/key``), or `nil`.
    @Binding var playing: String?
    @StateObject private var watcher = ChannelWatcher()
    /// The channel's catalog entry (#220): its title and description.
    @StateObject private var catalog = CatalogWatcher()

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
        .onAppear {
            if reference.isEmpty, let own = node.channel?.address { reference = own.display }
            // antstream-visual: `-antstream-shot-watch-info` (with
            // `-antstream-shot-watch`) shows a catalog entry's title and
            // description over the player (#220). The sample channel has
            // no catalog, so a sample entry stands in.
            if RootView.shotArgs.contains("-antstream-shot-watch-info") {
                catalog.installSample()
            }
            resume()
        }
        .onChange(of: node.channel) { _, channel in
            if reference.isEmpty, let own = channel?.address { reference = own.display }
        }
        .onDisappear {
            watcher.stop()
            catalog.stop()
        }
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
                Text("CHANNEL")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                TextField("0x<owner>/<topic>, swarm:// or a player link", text: $reference)
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
                if !reference.isEmpty, !canPlay {
                    Text("Enter the broadcaster's address and topic: 0x<40 hex>/<topic>, swarm://<owner>/<topic>, or a player link.")
                        .font(.footnote)
                        .foregroundStyle(.orange)
                }
            }
        }
    }

    private var canPlay: Bool { ChannelAddress.parse(reference) != nil }

    private func play() {
        guard let channel = ChannelAddress.parse(reference) else { return }
        playing = channel.key
        // Play again even when it's the same channel: that is how a
        // viewer retries by hand.
        if let gateway = node.gatewayURL {
            watcher.play(channel: channel, gateway: gateway, restart: true)
            catalog.follow(channel: channel, gateway: gateway)
        }
    }

    private func resume() {
        if let playing, let channel = ChannelAddress.parse(playing), let gateway = node.gatewayURL {
            watcher.play(channel: channel, gateway: gateway)
            catalog.follow(channel: channel, gateway: gateway)
        } else {
            watcher.stop()
            catalog.stop()
        }
    }

    // MARK: player

    private var playerCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                if let entry = catalog.entry {
                    catalogInfo(entry)
                }
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

extension WatchView {
    /// What the channel's owner says about it in their catalog (#220):
    /// the title, the description, and an announced start.
    fileprivate func catalogInfo(_ entry: CatalogEntry) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            if !entry.title.isEmpty {
                Text(entry.title)
                    .font(.headline)
                    .foregroundStyle(.white)
                    .fixedSize(horizontal: false, vertical: true)
            }
            if !entry.description.isEmpty {
                Text(entry.description)
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                    .fixedSize(horizontal: false, vertical: true)
            }
            if let start = entry.upcomingStart {
                Label("Starts \(start.formatted(date: .abbreviated, time: .shortened))",
                      systemImage: "calendar")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.yellow.opacity(0.9))
            }
        }
    }
}

/// Reads the catalog of the channel the Watch tab plays (#220): the
/// owner's catalog feed (`GET <gateway>/feeds/<owner>/<keccak256
/// "antstream-catalog">`, whose body is the JSON array), for the channel's
/// entry. Beside playback, never in front of it: its own task, read when
/// the channel changes and then once a minute. A result for a channel no
/// longer shown is dropped; a missing or unreadable catalog shows nothing.
@MainActor
final class CatalogWatcher: ObservableObject {
    @Published private(set) var entry: CatalogEntry?

    private struct Source: Equatable {
        let channel: ChannelAddress
        let gateway: URL
    }

    private var source: Source?
    private var task: Task<Void, Never>?
    /// Set by ``installSample()``: the sample stays put for the shot.
    private var sampleActive = false

    /// How often the catalog is read again: it changes with the
    /// broadcaster's edits and at most once a minute while live.
    private static let refreshSeconds: UInt64 = 60

    /// Follow `channel`'s catalog entry. A no-op when it already does.
    func follow(channel: ChannelAddress, gateway: URL) {
        if sampleActive { return }
        let next = Source(channel: channel, gateway: gateway)
        guard next != source else { return }
        if source?.channel != channel { entry = nil }
        source = next
        task?.cancel()
        task = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                await self.read(next)
                try? await Task.sleep(nanoseconds: Self.refreshSeconds * 1_000_000_000)
            }
        }
    }

    func stop() {
        if sampleActive { return }
        task?.cancel()
        task = nil
        source = nil
        entry = nil
    }

    /// One read of `source`'s catalog. A network failure keeps what is
    /// shown; a catalog that isn't there or has no entry for the channel
    /// clears it.
    private func read(_ source: Source) async {
        guard case .text = source.channel.topic,
              let topic = FeedAddress.topicHex(CatalogEntry.topicText)
        else {
            entry = nil
            return
        }
        let url = source.gateway.appending(path: "feeds/\(source.channel.owner)/\(topic)")
        let request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 30)
        let result = try? await URLSession.shared.data(for: request)
        // The channel changed (or the tab stopped) while the read was out.
        guard !Task.isCancelled, !sampleActive, self.source == source else { return }
        guard let (data, response) = result else { return }
        let code = (response as? HTTPURLResponse)?.statusCode ?? 0
        let found = code == 200 ? CatalogEntry.find(in: data, channel: source.channel) : nil
        if found != entry { entry = found }
    }

    /// A catalog entry for the Watch tab's screenshot
    /// (`-antstream-shot-watch-info`): the sample channel has no catalog.
    /// Latched, so no read or channel change replaces it before the shot.
    /// Sample only.
    func installSample() {
        sampleActive = true
        task?.cancel()
        task = nil
        let calendar = Calendar.current
        let tomorrow = calendar.date(byAdding: .day, value: 1, to: Date()) ?? Date()
        entry = CatalogEntry(
            title: "Morning walk",
            description: "Along the river to the old bridge, with the swans if they're up.",
            state: "scheduled",
            thumbnail: nil,
            scheduledStart: calendar.date(bySettingHour: 8, minute: 0, second: 0, of: tomorrow)
        )
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

    /// Where the playlist is read from (`<gateway>/feeds/<owner>/<topic>`),
    /// shown under the player.
    @Published private(set) var feedURL: URL?

    private struct Source: Equatable {
        let channel: ChannelAddress
        let gateway: URL
    }

    private var source: Source?
    /// Held here: `AVAssetResourceLoader` keeps its delegate weakly.
    private var loader: FeedPlaylistLoader?
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

    /// Play `channel` through `gateway`. A no-op when it is already
    /// playing or loading, unless `restart`.
    func play(channel: ChannelAddress, gateway: URL, restart: Bool = false) {
        let next = Source(channel: channel, gateway: gateway)
        guard restart || next != source || player.currentItem == nil
        else { return }
        source = next
        message = nil
        load()
    }

    func stop() {
        source = nil
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
        buffering = true
        guard let address = FeedAddress(source.channel) else {
            retry("Can't work out this channel's feed topic; retrying…")
            return
        }
        let loader = FeedPlaylistLoader(gateway: source.gateway, address: address)
        self.loader = loader
        feedURL = loader.feedURL
        let asset = AVURLAsset(url: loader.playlistURL)
        asset.resourceLoader.setDelegate(loader, queue: loader.queue)
        let item = AVPlayerItem(asset: asset)
        player.replaceCurrentItem(with: item)
        player.play()
        watch(item)
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

    /// The feed `channel` names: a text topic hashed by the library
    /// (`ant_topic_from_string`, what the publisher computes from
    /// `topic_text`), a raw one as it is. `nil` only if the library
    /// can't hash it.
    init?(_ channel: ChannelAddress) {
        owner = channel.owner
        switch channel.topic {
        case .raw(let hex):
            topic = hex
        case .text(let text):
            guard let hex = Self.topicHex(text) else { return nil }
            topic = hex
        }
    }

    /// `keccak256(utf8(text))` as hex, bee-js `Topic.fromString`.
    static func topicHex(_ text: String) -> String? {
        var errPtr: UnsafeMutablePointer<CChar>? = nil
        guard let raw = text.withCString({ ant_topic_from_string($0, &errPtr) }) else {
            if let errPtr { ant_free_string(errPtr) }
            return nil
        }
        defer { ant_free_string(raw) }
        let hex = String(cString: raw)
        return hex.count == 64 ? hex : nil
    }
}

/// Serves a solar-punk channel's live playlist to `AVPlayer`. Every
/// request for ``playlistURL`` (the first load and each live reload)
/// reads `GET <gateway>/feeds/<owner>/<topic>`, whose body *is* the
/// newest playlist (#167), listing bare segment and map references,
/// which are turned into `<gateway>/bytes/<ref>`
/// (``SolarPunkPlaylist/rewrite(_:gateway:)``) — the rewrite Solar
/// Punk's viewers do, which leaves the publisher out of the choice of
/// gateway. A feed whose update is not a playlist (a channel published
/// in the older `"bzz"` format) fails the item; the watcher retries.
///
/// **Joining from a ladder marker** (#174). A broadcast started with
/// `"ladder": true` (this app's are) writes a small marker every ten
/// seconds at an address computed from the clock, naming the feed's
/// newest index. So the very first read of a load is preceded by one
/// marker read (``markerIndex(_:period:topic:)``, the previous period's
/// marker, then the one before it) and, when a marker names the
/// channel, made as `GET <gateway>/feeds/<owner>/<topic>?after=<index>`:
/// one round of slot reads from there instead of a walk from index 0,
/// which a viewer an hour into a broadcast pays in many rounds. The node
/// remembers where the feed stood, so later reloads start there too. No marker, or one that
/// doesn't parse, means the read goes out as before.
///
/// Segments themselves are plain `http://` URLs, fetched by `AVPlayer`
/// directly. A custom URL scheme is what routes the playlist through a
/// resource-loader delegate at all; `AVPlayer` never asks one for
/// `http` URLs. The one relative URI a playlist carries is a lost
/// segment's `#EXT-X-GAP` placeholder (`gap-<sequence>`, #169), which
/// resolves against ``playlistURL`` and so would come here: see
/// ``isGapPlaceholder(_:)``.
///
/// Requests are tracked so a cancelled one (the item replaced on retry
/// or stop) cancels its read and is never answered. All state is touched
/// only on ``queue``, the queue `AVAssetResourceLoader` calls in on.
final class FeedPlaylistLoader: NSObject, AVAssetResourceLoaderDelegate {
    static let scheme = "antstream-feed"

    let gateway: URL
    let address: FeedAddress
    let queue = DispatchQueue(label: "antstream.feed-playlist")

    /// The read in flight for each loading request, by identity.
    private var tasks: [ObjectIdentifier: URLSessionDataTask] = [:]
    /// The first playlist request has been answered or is being: only it
    /// looks for a ladder marker.
    private var joined = false

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
        guard let url = loadingRequest.request.url, url.scheme == Self.scheme else { return false }
        if Self.isGapPlaceholder(url) {
            // RFC 8216bis has a client skip a gap entry rather than load
            // it. Should `AVPlayer` ask anyway, the answer is "no such
            // resource", never the playlist text a feed read returns.
            loadingRequest.finishLoading(with: NSError(
                domain: NSURLErrorDomain, code: NSURLErrorFileDoesNotExist))
            return true
        }
        if joined {
            read(feedURL, for: loadingRequest)
        } else {
            joined = true
            join(loadingRequest)
        }
        return true
    }

    /// The first read: from the newest ladder marker's index when there
    /// is one (see the type's doc). On ``queue``.
    private func join(_ loadingRequest: AVAssetResourceLoadingRequest, older: Bool = false) {
        let now = UInt64(max(0, Date().timeIntervalSince1970) * 1000) / 10_000
        // The current period's marker may not be written yet, so the one
        // before it, then the one before that, as Solar Punk's player does.
        let period = now - (older ? 2 : 1)
        guard now >= 2, let id = Self.markerID(topic: address.topic, period: period) else {
            read(feedURL, for: loadingRequest)
            return
        }
        let url = gateway.appending(path: "soc/\(address.owner)/\(id)")
        let request = URLRequest(
            url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 10)
        let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let self else { return }
            self.queue.async {
                // Cancelled meanwhile: nobody is waiting.
                guard self.tasks.removeValue(forKey: ObjectIdentifier(loadingRequest)) != nil,
                      !loadingRequest.isCancelled, !loadingRequest.isFinished
                else { return }
                let code = (response as? HTTPURLResponse)?.statusCode ?? 0
                let index = error == nil && code == 200
                    ? data.flatMap { Self.markerIndex($0, period: period, topic: self.address.topic) }
                    : nil
                if let index {
                    var feed = self.feedURL
                    feed.append(queryItems: [URLQueryItem(name: "after", value: String(index))])
                    self.read(feed, for: loadingRequest)
                } else if !older, error == nil, code == 404 {
                    // Missing, not a fault: the period before may have one.
                    self.join(loadingRequest, older: true)
                } else {
                    self.read(self.feedURL, for: loadingRequest)
                }
            }
        }
        tasks[ObjectIdentifier(loadingRequest)] = task
        task.resume()
    }

    /// The marker's identifier for the channel with feed topic `topic`
    /// in `period`, from the library (the publisher computes the same).
    static func markerID(topic: String, period: UInt64) -> String? {
        var errPtr: UnsafeMutablePointer<CChar>? = nil
        guard let raw = topic.withCString({ ant_ladder_marker_id($0, period, &errPtr) }) else {
            if let errPtr { ant_free_string(errPtr) }
            return nil
        }
        defer { ant_free_string(raw) }
        return String(cString: raw)
    }

    /// The feed index a marker read for `period` names for `topic`, or
    /// `nil` for anything that is not exactly a version 2 marker of that
    /// period: the checks Solar Punk's `parseLadderMarker` makes, since
    /// the index is jumped to without a second look.
    static func markerIndex(_ data: Data, period: UInt64, topic: String) -> UInt64? {
        guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys) == ["v", "period", "writtenAt", "rungs", "segmentMs"],
              let v = whole(object["v"]), v == 2,
              whole(object["period"]) == period,
              let writtenAt = whole(object["writtenAt"]), writtenAt / 10_000 == period,
              let segmentMs = whole(object["segmentMs"]), segmentMs > 0,
              let rungs = object["rungs"] as? [String: Any], !rungs.isEmpty
        else { return nil }
        return whole(rungs[topic.lowercased()])
    }

    /// A JSON number that is a whole, non-negative, exactly representable
    /// integer (not a Boolean, which `JSONSerialization` also reads as an
    /// `NSNumber`).
    private static func whole(_ value: Any?) -> UInt64? {
        guard let number = value as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID()
        else { return nil }
        let double = number.doubleValue
        guard double >= 0, double <= 9_007_199_254_740_991, double.rounded() == double
        else { return nil }
        return UInt64(double)
    }

    /// `url` is an `#EXT-X-GAP` entry's placeholder, `gap-<sequence>`,
    /// resolved against ``playlistURL``. Names no media: the segment was
    /// dropped or its upload failed.
    static func isGapPlaceholder(_ url: URL) -> Bool {
        let name = url.lastPathComponent
        return name.count > 4 && name.hasPrefix("gap-")
            && name.dropFirst(4).allSatisfy(\.isNumber)
    }

    func resourceLoader(
        _ resourceLoader: AVAssetResourceLoader,
        didCancel loadingRequest: AVAssetResourceLoadingRequest
    ) {
        tasks.removeValue(forKey: ObjectIdentifier(loadingRequest))?.cancel()
    }

    /// Start one read of `url` answering `loadingRequest`. On ``queue``.
    private func read(_ url: URL, for loadingRequest: AVAssetResourceLoadingRequest) {
        // Feed reads are mutable: never answer a live reload from cache.
        let request = URLRequest(
            url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 60)
        let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let self else { return }
            self.queue.async {
                self.finished(url, data: data, response: response, error: error, for: loadingRequest)
            }
        }
        tasks[ObjectIdentifier(loadingRequest)] = task
        task.resume()
    }

    /// One read came back. On ``queue``.
    private func finished(
        _ url: URL, data: Data?, response: URLResponse?, error: Error?,
        for loadingRequest: AVAssetResourceLoadingRequest
    ) {
        // Cancelled meanwhile (its entry is gone): nobody is waiting.
        guard tasks.removeValue(forKey: ObjectIdentifier(loadingRequest)) != nil,
              !loadingRequest.isCancelled, !loadingRequest.isFinished
        else { return }
        let code = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard error == nil, code == 200, let data else {
            // A channel that has not gone live yet 404s here; the item
            // fails and the watcher retries it.
            loadingRequest.finishLoading(with: error ?? NSError(
                domain: NSURLErrorDomain,
                code: code == 404 ? NSURLErrorFileDoesNotExist : NSURLErrorBadServerResponse))
            return
        }
        guard data.starts(with: Data("#EXTM3U".utf8)),
              let playlist = String(data: data, encoding: .utf8)
        else {
            // Not a playlist: a v1 update (a channel in the older "bzz"
            // format, whose content is a manifest), which this tab no
            // longer plays.
            loadingRequest.finishLoading(with: NSError(
                domain: NSURLErrorDomain, code: NSURLErrorCannotParseResponse))
            return
        }
        let body = Data(SolarPunkPlaylist.rewrite(playlist, gateway: gateway).utf8)
        if let info = loadingRequest.contentInformationRequest {
            info.contentType = "public.m3u-playlist"
            info.contentLength = Int64(body.count)
            info.isByteRangeAccessSupported = false
        }
        loadingRequest.dataRequest?.respond(with: body)
        loadingRequest.finishLoading()
    }
}
