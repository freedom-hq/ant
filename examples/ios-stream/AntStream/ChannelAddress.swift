import Foundation

/// A solar-punk channel: the feed's owner (40 lowercase hex, no `0x`) and
/// its topic, as text (hashed the way bee-js `Topic.fromString` does, by
/// the library's `ant_topic_from_string`) or as 32 raw bytes. Same shapes
/// and links as the Android app's `ChannelAddress` and the web player's
/// `parseChannel`.
///
/// Foundation only, no FFI, so `ci/channel-address-tests.swift` compiles
/// and runs it on its own (the app has no test target).
struct ChannelAddress: Equatable, Hashable {
    /// The topic text every AntStream app broadcast goes out on: one
    /// channel per account, the owner tells channels apart.
    static let antstreamTopic = "antstream"

    /// The AntStream web player's feed manifest (`examples/antstream-player`):
    /// always serves its newest build, so the links below never change for
    /// a player update. Keep in step with the Android app's
    /// `ChannelAddress.PLAYER_REF`.
    static let playerReference = "b268072928bd99d637992eb54c2a826b83affa36f61cf495297541ac42857c35"
    static let publicGateway = "https://api.gateway.ethswarm.org"

    enum Topic: Equatable, Hashable {
        /// Text, hashed into the feed topic.
        case text(String)
        /// The topic's 32 bytes, 64 lowercase hex.
        case raw(String)
    }

    let owner: String
    let topic: Topic

    init(owner: String, text: String) {
        self.owner = Self.stripHexPrefix(owner).lowercased()
        self.topic = .text(text)
    }

    init(owner: String, rawTopic: String) {
        self.owner = Self.stripHexPrefix(owner).lowercased()
        self.topic = .raw(Self.stripHexPrefix(rawTopic).lowercased())
    }

    /// The topic as shown: the text, or `0x<64 hex>`.
    var topicLabel: String {
        switch topic {
        case .text(let text): text
        case .raw(let hex): "0x\(hex)"
        }
    }

    /// `0x<owner>/<topic>`, the form a web player link carries after its
    /// `#`: the topic URL-encoded (`encodeURIComponent`), or `0x<64 hex>`.
    var key: String {
        switch topic {
        case .text(let text): "0x\(owner)/\(Self.encodeComponent(text))"
        case .raw(let hex): "0x\(owner)/0x\(hex)"
        }
    }

    /// What the Watch field shows: `0x<owner>/<topic>`, unencoded.
    var display: String { "0x\(owner)/\(topicLabel)" }

    /// Solar Punk's `swarm://<owner>/<topic>`, the URI its players (and
    /// the publisher's master playlist) name a channel by. Text topics
    /// only: their readers hash the text in the URI.
    var swarmURI: String? {
        if case .text(let text) = topic { return "swarm://\(owner)/\(text)" }
        return nil
    }

    /// The web player on this channel in Freedom.
    var playerLink: String { "bzz://\(Self.playerReference)/#\(key)" }

    /// The web player on this channel through a public gateway, for any
    /// browser.
    var gatewayPlayerLink: String { "\(Self.publicGateway)/bzz/\(Self.playerReference)/#\(key)" }

    /// What "Share" sends: the web player links, then the Solar Punk URI.
    /// Same text as the Android app's `ChannelAddress.watchLinks`.
    var watchLinks: String {
        var lines = [
            "Watch my live stream on Swarm:",
            "Freedom browser: \(playerLink)",
            "Any browser: \(gatewayPlayerLink)",
        ]
        if let swarmURI { lines.append("Solar Punk players: \(swarmURI)") }
        lines.append("Channel: \(display)")
        return lines.joined(separator: "\n")
    }

    /// A channel from what a viewer typed or pasted, read the way the web
    /// player's `parseChannel` reads one link or URI (input with no
    /// whitespace): `<owner>/<topic>` (the owner `0x`
    /// + 40 hex, the topic text, URL-encoded or not), Solar Punk's
    /// `swarm://<owner>/<topic>`, or a player link carrying either after
    /// its `#`. A topic of 64 hex digits (`0x` optional) is the raw topic.
    /// An owner alone (`<owner>` or `<owner>/`, not `swarm://<owner>/`) is
    /// that account's AntStream channel (topic ``antstreamTopic``). `nil` for anything else.
    /// Unlike the web player, this does not search free text (the Share
    /// text, a message it was pasted into) for a channel: the whole input
    /// is the one link, so input over more than one line (pasted Share
    /// text) is `nil` here. A topic never has a line break in it.
    static func parse(_ input: String) -> ChannelAddress? {
        var s = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if s.isEmpty || s.unicodeScalars.contains(where: CharacterSet.newlines.contains) { return nil }
        if let hash = s.firstIndex(of: "#") { s = String(s[s.index(after: hash)...]) }
        var owner: Substring
        var rest: Substring
        if s.lowercased().hasPrefix("swarm://") {
            // parseSwarmUri: the topic is the piece up to the next `/`.
            let body = s.dropFirst("swarm://".count)
            let cut = body.firstIndex(where: { "?#".contains($0) }) ?? body.endIndex
            let parts = body[..<cut].split(separator: "/", omittingEmptySubsequences: false)
            // Like the web player's and Android's regex, the topic must be
            // there: a truncated `swarm://<owner>/` is not the owner's
            // AntStream channel.
            guard parts.count >= 2, !parts[1].isEmpty else { return nil }
            owner = parts[0]
            rest = parts[1]
        } else if let slash = s.firstIndex(of: "/") {
            owner = s[..<slash]
            rest = s[s.index(after: slash)...]
        } else {
            owner = Substring(s)
            rest = ""
        }
        if owner.hasPrefix("0x") || owner.hasPrefix("0X") { owner = owner.dropFirst(2) }
        guard owner.count == 40, owner.allSatisfy(\.isASCIIHex) else { return nil }
        while rest.hasSuffix("/") { rest.removeLast() }
        var topic = rest.removingPercentEncoding ?? String(rest)
        if topic.isEmpty { topic = antstreamTopic }
        let bare = topic.hasPrefix("0x") || topic.hasPrefix("0X") ? String(topic.dropFirst(2)) : topic
        if bare.count == 64, bare.allSatisfy(\.isASCIIHex) {
            return ChannelAddress(owner: String(owner), rawTopic: bare)
        }
        return ChannelAddress(owner: String(owner), text: topic)
    }

    /// `encodeURIComponent`: what the web player decodes.
    static func encodeComponent(_ text: String) -> String {
        var allowed = CharacterSet()
        allowed.insert(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()")
        return text.addingPercentEncoding(withAllowedCharacters: allowed) ?? text
    }

    static func stripHexPrefix(_ s: String) -> String {
        s.hasPrefix("0x") || s.hasPrefix("0X") ? String(s.dropFirst(2)) : s
    }
}

/// The feed and format a broadcast writes to, as the publisher config
/// names them.
struct BroadcastFeed: Equatable {
    /// `"solar-punk"`, or `"bzz"` for an unfinished broadcast left by a
    /// build from before the switch (#217), which can only be ended.
    let format: String
    let topic: ChannelAddress.Topic

    /// Every broadcast from this app: the Solar Punk format (the only one
    /// the AntStream viewers read) on the account's `antstream` topic.
    static let current = BroadcastFeed(
        format: "solar-punk", topic: .text(ChannelAddress.antstreamTopic))

    /// The feed a recovery entry was saved for: its `topic_text` when it
    /// has one, else its raw `topic`, and its `format`. The publisher
    /// refuses to resume an entry on any other topic or format.
    static func recovery(topic: String, topicText: String, format: String) -> BroadcastFeed {
        BroadcastFeed(
            format: format,
            topic: topicText.isEmpty ? .raw(topic.lowercased()) : .text(topicText))
    }

    /// The entries the publisher config takes for this feed: `topic_text`
    /// or raw `topic` (one or the other, never both), and `format`. No
    /// `start_index`: the publisher reads the head and continues after it
    /// (#173).
    var configEntries: [String: Any] {
        switch topic {
        case .text(let text): ["format": format, "topic_text": text]
        case .raw(let hex): ["format": format, "topic": hex]
        }
    }

    /// Every broadcast writes ladder time markers (#174), and under
    /// "solar-punk" a master playlist naming the channel by its
    /// `swarm://<owner>/antstream` URI, so a viewer joining hours in reads
    /// one marker and then one round of feed slots instead of walking the
    /// feed from 0. 360 chunks an hour on the plan.
    static let ladder = true

    /// The whole `ant_publisher_start` config for a broadcast on this
    /// feed. `channel` is the channel's name, a label in the playlist notes
    /// and the UI only: renaming it never forks the channel.
    func publisherConfig(
        channel: String,
        gateway: String,
        batchId: String,
        bitrateKbps: UInt32,
        segmentMs: UInt32,
        notes: String,
        recoveryPath: String,
        resume: Bool,
        reapMs: UInt64,
        info: StreamInfo? = nil,
        width: Int = 0,
        height: Int = 0
    ) -> [String: Any] {
        var config: [String: Any] = [
            "recovery_path": recoveryPath,
            "resume": resume,
            "channel": channel,
            "gateway": gateway,
            "batch_id": batchId,
            "bitrate_kbps": bitrateKbps,
            "segment_ms": segmentMs,
            "notes": notes,
            // A broadcast whose network (or the app) is gone for a whole
            // minute ends by itself, as Solar Punk's uploader does
            // (#172); anything shorter is a pause with one seam.
            "reap_ms": reapMs,
            "ladder": Self.ladder,
        ]
        config.merge(configEntries) { _, feed in feed }
        // The stream info (#220): with it, the broadcast keeps the
        // account's catalog entry for the channel.
        if let info { config["info"] = info.infoObject() }
        // The encoded size: the master's RESOLUTION and the catalog
        // entry's rung (#220). Left out unless both are known.
        if width > 0 && height > 0 {
            config["width"] = width
            config["height"] = height
        }
        return config
    }
}

/// The broadcaster's stream info (#220), remembered per account: what
/// the account's catalog entry for the channel says about it. The
/// publisher config's `"info"`, `ant_publisher_set_info`'s argument and
/// part of `ant_stream_catalog_publish`'s (``infoObject()``), plus what
/// the app keeps about the thumbnail and the announcement.
struct StreamInfo: Codable, Equatable {
    /// Empty is the channel's name (the node fills it in).
    var title = ""
    var description = ""
    /// The uploaded thumbnail's reference (64 hex), empty without one or
    /// while the image on the device is not uploaded yet.
    var thumbnail = ""
    /// The broadcaster chose the thumbnail (a photo). A chosen one is
    /// never replaced by a frame captured from the camera.
    var thumbnailPicked = false
    /// The image on the device has not been uploaded yet: ``thumbnail``
    /// is empty until it has.
    var thumbnailPending = false
    /// An upcoming broadcast's start, Unix ms.
    var scheduledStartMs: UInt64?
    /// The start the catalog last announced
    /// (`ant_stream_catalog_publish`), Unix ms.
    var announcedStartMs: UInt64?

    init(
        title: String = "",
        description: String = "",
        thumbnail: String = "",
        thumbnailPicked: Bool = false,
        thumbnailPending: Bool = false,
        scheduledStartMs: UInt64? = nil,
        announcedStartMs: UInt64? = nil
    ) {
        self.title = title
        self.description = description
        self.thumbnail = thumbnail
        self.thumbnailPicked = thumbnailPicked
        self.thumbnailPending = thumbnailPending
        self.scheduledStartMs = scheduledStartMs
        self.announcedStartMs = announcedStartMs
    }

    /// Every field optional, so what an older or newer build saved still
    /// reads.
    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        title = try c.decodeIfPresent(String.self, forKey: .title) ?? ""
        description = try c.decodeIfPresent(String.self, forKey: .description) ?? ""
        thumbnail = try c.decodeIfPresent(String.self, forKey: .thumbnail) ?? ""
        thumbnailPicked = try c.decodeIfPresent(Bool.self, forKey: .thumbnailPicked) ?? false
        thumbnailPending = try c.decodeIfPresent(Bool.self, forKey: .thumbnailPending) ?? false
        scheduledStartMs = try c.decodeIfPresent(UInt64.self, forKey: .scheduledStartMs)
        announcedStartMs = try c.decodeIfPresent(UInt64.self, forKey: .announcedStartMs)
    }

    /// The `"info"` object the library takes: `title`, `description`,
    /// `thumbnail` and `scheduled_start_ms` (`null` without one). A
    /// thumbnail that is not a 64-hex reference is left out, since the
    /// node refuses the whole object for it.
    func infoObject() -> [String: Any] {
        var object: [String: Any] = [
            "title": title.trimmingCharacters(in: .whitespacesAndNewlines),
            "description": description.trimmingCharacters(in: .whitespacesAndNewlines),
            "thumbnail": Self.isReference(thumbnail) ? thumbnail.lowercased() : "",
        ]
        if let scheduledStartMs {
            object["scheduled_start_ms"] = scheduledStartMs
        } else {
            object["scheduled_start_ms"] = NSNull()
        }
        return object
    }

    /// ``infoObject()`` as JSON text, for `ant_publisher_set_info`.
    func infoJSON() -> String? {
        guard let data = try? JSONSerialization.data(withJSONObject: infoObject()) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    /// The info a broadcast goes live with: the broadcast an
    /// announcement promised is the one starting, so the schedule is left
    /// behind.
    var forBroadcast: StreamInfo {
        var info = self
        info.scheduledStartMs = nil
        info.announcedStartMs = nil
        return info
    }

    /// The title shown for it: the broadcaster's, else the channel's name.
    func displayTitle(channel: String) -> String {
        let trimmed = title.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? channel : trimmed
    }

    /// The scheduled start, when the catalog announced exactly this one.
    var announcedStart: Date? {
        guard let announcedStartMs, announcedStartMs == scheduledStartMs else { return nil }
        return Date(timeIntervalSince1970: Double(announcedStartMs) / 1000)
    }

    var scheduledStart: Date? {
        scheduledStartMs.map { Date(timeIntervalSince1970: Double($0) / 1000) }
    }

    /// `ant_stream_catalog_publish`'s config: announce an upcoming
    /// broadcast on the channel `topicText` (``ChannelAddress/antstreamTopic``)
    /// with this info, the rendition the broadcast will go out in as the
    /// entry's rung.
    func catalogPublishConfig(
        gateway: String,
        batchId: String,
        channel: String,
        topicText: String = ChannelAddress.antstreamTopic,
        width: Int,
        height: Int,
        bitrateKbps: Int
    ) -> [String: Any] {
        [
            "gateway": gateway,
            "batch_id": batchId,
            "channel": channel,
            "topic_text": topicText,
            "info": infoObject(),
            "width": width,
            "height": height,
            "bitrate_kbps": bitrateKbps,
        ]
    }

    /// 64 hex digits, `0x` optional: what `info.thumbnail` takes.
    static func isReference(_ text: String) -> Bool {
        let bare = ChannelAddress.stripHexPrefix(text)
        return bare.count == 64 && bare.allSatisfy(\.isASCIIHex)
    }
}

/// A channel's entry in its owner's catalog (#220): what the Watch tab
/// shows about the channel it plays. Each account publishes one catalog
/// feed (owner the account, topic text ``topicText``) whose newest update
/// is a JSON array of Solar Punk catalog entries.
struct CatalogEntry: Equatable {
    /// The catalog feed's topic text, hashed like the channel's.
    static let topicText = "antstream-catalog"

    let title: String
    let description: String
    /// `"scheduled"`, `"live"` or `"vod"` (anything else is kept as is).
    let state: String
    /// The thumbnail's reference (64 lowercase hex), when it has one.
    let thumbnail: String?
    /// `scheduledStartTime`, when it has a readable one.
    let scheduledStart: Date?

    /// The start an upcoming broadcast announces: a `"scheduled"` entry's.
    var upcomingStart: Date? { state == "scheduled" ? scheduledStart : nil }

    /// Caps on what is shown from an entry anyone could have written.
    static let maxTitle = 200
    static let maxDescription = 2000

    /// The entry for `channel` in a catalog feed's body, or `nil`. The
    /// body is untrusted: anything that is not an array, entries that are
    /// not objects, fields of the wrong type are skipped, never trusted.
    /// The same rule as Android's `Catalog.select` and the web player: an
    /// entry is the channel's when its `owner` is the channel's owner
    /// (`0x` and case aside) and its `topic` is the channel's topic text
    /// (for a raw-topic channel, a text `topicHex` hashes to its topic;
    /// without `topicHex` a raw-topic channel has none). Of several, the
    /// newest `timestamp` wins, the later one on a tie. A chosen entry
    /// with neither a title nor a description says nothing to show.
    static func find(
        in data: Data,
        channel: ChannelAddress,
        topicHex: ((String) -> String?)? = nil
    ) -> CatalogEntry? {
        guard let array = try? JSONSerialization.jsonObject(with: data) as? [Any] else { return nil }
        let matches: (String) -> Bool
        switch channel.topic {
        case .text(let topic):
            matches = { $0 == topic }
        case .raw(let hex):
            guard let topicHex else { return nil }
            matches = { topicHex($0)?.lowercased() == hex.lowercased() }
        }
        var best: (at: Double, object: [String: Any])?
        for case let object as [String: Any] in array {
            guard let owner = object["owner"] as? String,
                  ChannelAddress.stripHexPrefix(owner).lowercased() == channel.owner,
                  let topic = object["topic"] as? String, !topic.isEmpty, matches(topic)
            else { continue }
            // `Bool` bridges to `NSNumber` too: only a real number counts.
            let number = object["timestamp"] as? NSNumber
            let value = number.flatMap { CFGetTypeID($0) == CFBooleanGetTypeID() ? nil : $0.doubleValue } ?? 0
            let at = value.isFinite ? value : 0
            if let best, at < best.at { continue }
            best = (at, object)
        }
        guard let object = best?.object else { return nil }
        let title = clean(object["title"], max: maxTitle)
        let description = clean(object["description"], max: maxDescription)
        guard !title.isEmpty || !description.isEmpty else { return nil }
        let thumbnail = (object["thumbnail"] as? String).flatMap { ref -> String? in
            StreamInfo.isReference(ref) ? ChannelAddress.stripHexPrefix(ref).lowercased() : nil
        }
        return CatalogEntry(
            title: title,
            description: description,
            state: clean(object["state"], max: 20),
            thumbnail: thumbnail,
            scheduledStart: (object["scheduledStartTime"] as? String).flatMap(parseDate)
        )
    }

    /// A string field, trimmed and capped; anything else is empty.
    private static func clean(_ value: Any?, max: Int) -> String {
        guard let text = value as? String else { return "" }
        return String(text.trimmingCharacters(in: .whitespacesAndNewlines).prefix(max))
    }

    /// ISO 8601, with or without fractional seconds (JavaScript's
    /// `toISOString` has them).
    static func parseDate(_ text: String) -> Date? {
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = fractional.date(from: text) { return date }
        let plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]
        return plain.date(from: text)
    }
}

/// Solar Punk playlists list their segments and `#EXT-X-MAP` as bare
/// references: a player resolves them on whichever gateway it reads from.
enum SolarPunkPlaylist {
    /// `playlist` with every bare reference (a segment line or the
    /// `#EXT-X-MAP` URI, 64 or 128 hex) made `<gateway>/bytes/<ref>`, the
    /// rewrite Solar Punk's viewers do. Everything else, `#EXT-X-GAP`
    /// placeholders (`gap-<sequence>`) included, passes through.
    static func rewrite(_ playlist: String, gateway: URL) -> String {
        let map = "#EXT-X-MAP:URI=\""
        return playlist.split(separator: "\n", omittingEmptySubsequences: false)
            .map { line -> String in
                if !line.hasPrefix("#"), let url = absolute(line, gateway) {
                    return url
                }
                if line.hasPrefix(map), line.hasSuffix("\""),
                   let url = absolute(line.dropFirst(map.count).dropLast(), gateway) {
                    return map + url + "\""
                }
                return String(line)
            }
            .joined(separator: "\n")
    }

    /// `text` as `<gateway>/bytes/<ref>`, or `nil` when it is not a bare
    /// reference (64 hex, 128 for an encrypted one).
    private static func absolute(_ text: Substring, _ gateway: URL) -> String? {
        let value = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard value.count == 64 || value.count == 128, value.allSatisfy(\.isASCIIHex) else { return nil }
        var base = gateway.absoluteString
        while base.hasSuffix("/") { base.removeLast() }
        return "\(base)/bytes/\(value.lowercased())"
    }
}

extension Character {
    /// `0-9a-fA-F` only. `isHexDigit` also accepts the fullwidth forms
    /// (`０`, `Ａ`), which `lowercased()` keeps non-ASCII and no gateway
    /// reads as hex; Android's `[0-9a-fA-F]` and the web player agree on this.
    var isASCIIHex: Bool { isASCII && isHexDigit }
}
