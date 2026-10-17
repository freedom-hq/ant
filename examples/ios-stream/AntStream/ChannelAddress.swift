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
    /// player's `parseChannel` reads it: `<owner>/<topic>` (the owner `0x`
    /// + 40 hex, the topic text, URL-encoded or not), Solar Punk's
    /// `swarm://<owner>/<topic>`, or a player link carrying either after
    /// its `#`. A topic of 64 hex digits (`0x` optional) is the raw topic.
    /// An owner alone is that account's AntStream channel (topic
    /// ``antstreamTopic``). `nil` for anything else.
    static func parse(_ input: String) -> ChannelAddress? {
        var s = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if s.isEmpty { return nil }
        if let hash = s.firstIndex(of: "#") { s = String(s[s.index(after: hash)...]) }
        var owner: Substring
        var rest: Substring
        if s.lowercased().hasPrefix("swarm://") {
            // parseSwarmUri: the topic is the piece up to the next `/`.
            let body = s.dropFirst("swarm://".count)
            let cut = body.firstIndex(where: { "?#".contains($0) }) ?? body.endIndex
            let parts = body[..<cut].split(separator: "/", omittingEmptySubsequences: false)
            guard parts.count >= 2 else { return nil }
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
        guard owner.count == 40, owner.allSatisfy(\.isHexDigit) else { return nil }
        while rest.hasSuffix("/") { rest.removeLast() }
        var topic = rest.removingPercentEncoding ?? String(rest)
        if topic.isEmpty { topic = antstreamTopic }
        let bare = topic.hasPrefix("0x") || topic.hasPrefix("0X") ? String(topic.dropFirst(2)) : topic
        if bare.count == 64, bare.allSatisfy(\.isHexDigit) {
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
        reapMs: UInt64
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
        return config
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
        guard value.count == 64 || value.count == 128, value.allSatisfy(\.isHexDigit) else { return nil }
        var base = gateway.absoluteString
        while base.hasSuffix("/") { base.removeLast() }
        return "\(base)/bytes/\(value.lowercased())"
    }
}
