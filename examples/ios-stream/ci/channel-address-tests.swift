// Unit tests for the solar-punk channel (#217), `AntStream/ChannelAddress.swift`:
// link parsing, the links a channel is shared by, the playlist rewrite the
// Watch tab feeds `AVPlayer`, the publisher config every broadcast starts
// with, and the stream info and catalog entry of #220 (the catalog read
// from `crates/ant-ffi/testdata/catalog`, so run it from
// `examples/ios-stream`). The app has no test target, so the antstream-visual
// workflow compiles the file on its own with these tests and runs them on
// the macOS runner:
//
//     swiftc -parse-as-library AntStream/ChannelAddress.swift \
//         ci/channel-address-tests.swift -o build/channel-address-tests
//     build/channel-address-tests
//
// The same cases as the Android app's `ChannelAddressTest`/`FeedPlaylistTest`.
import Foundation

@main
struct ChannelAddressTests {
    static var failures = 0
    static var checks = 0

    static func expect<T: Equatable>(_ got: T, _ want: T, _ what: String) {
        checks += 1
        if got != want {
            failures += 1
            print("FAIL: \(what): got \(got), want \(want)")
        }
    }

    static let owner = "d31c1f00000000000000000000000000000ec547"
    static let player = ChannelAddress.playerReference

    static func main() {
        // The account's own channel and its links.
        let own = ChannelAddress(owner: "0x" + owner.uppercased(), text: ChannelAddress.antstreamTopic)
        expect(own.owner, owner, "owner is lowercase, no 0x")
        expect(own.key, "0x\(owner)/antstream", "key")
        expect(own.display, "0x\(owner)/antstream", "display")
        expect(own.swarmURI, "swarm://\(owner)/antstream", "swarm URI")
        expect(own.playerLink, "bzz://\(player)/#0x\(owner)/antstream", "player link")
        expect(own.gatewayPlayerLink,
               "https://api.gateway.ethswarm.org/bzz/\(player)/#0x\(owner)/antstream",
               "gateway player link")
        expect(own.watchLinks.contains(own.playerLink), true, "share has the player link")
        expect(own.watchLinks.contains(own.gatewayPlayerLink), true, "share has the gateway link")
        expect(own.watchLinks.contains("swarm://\(owner)/antstream"), true, "share has the swarm URI")

        // Every input form names the same channel.
        for input in [
            "0x\(owner)/antstream",
            "\(owner)/antstream",
            "  0x\(owner.uppercased())/antstream \n",
            "swarm://\(owner)/antstream",
            "swarm://0x\(owner)/antstream",
            "bzz://\(player)/#0x\(owner)/antstream",
            "https://api.gateway.ethswarm.org/bzz/\(player)/#0x\(owner)/antstream",
            "0x\(owner)",
        ] {
            expect(ChannelAddress.parse(input), own, "parse \(input)")
        }

        // Text topics are URL-decoded; 64 hex is a raw topic.
        let spaced = ChannelAddress.parse("0x\(owner)/my%20show")
        expect(spaced?.topic, .text("my show"), "decoded text topic")
        expect(spaced?.key, "0x\(owner)/my%20show", "re-encoded key")
        let hex = String(repeating: "ab", count: 32)
        let raw = ChannelAddress.parse("0x\(owner)/0x\(hex.uppercased())")
        expect(raw?.topic, .raw(hex), "raw topic")
        expect(raw?.key, "0x\(owner)/0x\(hex)", "raw key")
        expect(raw?.swarmURI, nil, "no swarm URI for a raw topic")
        expect(raw.flatMap { ChannelAddress.parse($0.playerLink) }, raw, "raw round trip")

        for input in ["", "   ", "antstream", "0x1234/antstream", "bzz://\(hex)", "swarm://nope/antstream",
                      "swarm://\(owner)/", "swarm://0x\(owner)/", "swarm://\(owner)", "swarm://\(owner)//antstream"] {
            expect(ChannelAddress.parse(input), nil, "reject \(input)")
        }
        // The Share text, pasted whole: more than one line, not one link.
        for sep in ["\n", "\r\n", "\u{2028}"] {
            let text = own.watchLinks.replacingOccurrences(of: "\n", with: sep)
            expect(ChannelAddress.parse(text), nil, "reject Share text split by \(sep.debugDescription)")
        }
        // Fullwidth hex (`０`-`９`, `ａ`-`ｆ`) passes `isHexDigit` but is no
        // owner or topic a gateway reads; Android's `[0-9a-fA-F]` rejects it too.
        let fullwidth = String(owner.unicodeScalars.map {
            Character(Unicode.Scalar($0.value + 0xFEE0)!)
        })
        expect(ChannelAddress.parse("\(fullwidth)/antstream"), nil, "reject a fullwidth owner")
        let fullwidthTopic = String(repeating: "ａｂ", count: 32)
        expect(ChannelAddress.parse("0x\(owner)/\(fullwidthTopic)")?.topic, .text(fullwidthTopic),
               "a fullwidth 64-char topic is text, not raw")

        // Bare references become `<gateway>/bytes/<ref>`.
        let gw = URL(string: "http://127.0.0.1:1633/")!
        let ref = String(repeating: "ab", count: 32)
        let map = String(repeating: "cd", count: 32)
        let enc = String(repeating: "ef", count: 64)
        let playlist = [
            "#EXTM3U",
            "#EXT-X-MAP:URI=\"\(map)\"",
            "#EXTINF:2.000,",
            ref.uppercased(),
            "#EXT-X-GAP",
            "#EXTINF:2.000,",
            "gap-7",
            "#EXTINF:2.000,",
            enc,
            "https://elsewhere.example/seg.m4s",
            "#EXT-X-ENDLIST",
            "",
        ].joined(separator: "\n")
        let out = SolarPunkPlaylist.rewrite(playlist, gateway: gw).components(separatedBy: "\n")
        expect(out[1], "#EXT-X-MAP:URI=\"http://127.0.0.1:1633/bytes/\(map)\"", "map rewritten")
        expect(out[3], "http://127.0.0.1:1633/bytes/\(ref)", "segment rewritten, lowercase")
        expect(out[6], "gap-7", "gap placeholder kept")
        expect(out[8], "http://127.0.0.1:1633/bytes/\(enc)", "encrypted reference rewritten")
        expect(out[9], "https://elsewhere.example/seg.m4s", "absolute URL kept")
        expect(out.count, 12, "line count kept")

        // A new broadcast: solar-punk on the `antstream` text topic, with
        // ladder markers, and neither a raw topic nor a start index.
        let config = BroadcastFeed.current.publisherConfig(
            channel: "Morning show", gateway: "http://127.0.0.1:1633", batchId: "0xabc",
            bitrateKbps: 900, segmentMs: 2000, notes: "n",
            recoveryPath: "/tmp/r.json", resume: false, reapMs: 60_000)
        expect(config["format"] as? String, "solar-punk", "format")
        expect(config["topic_text"] as? String, "antstream", "topic text")
        expect(config["ladder"] as? Bool, true, "ladder")
        expect(config["channel"] as? String, "Morning show", "channel name is a label")
        expect(config["topic"] == nil, true, "no raw topic")
        expect(config["start_index"] == nil, true, "no start index")
        expect(config["reap_ms"] as? UInt64, 60_000, "reap window")

        // Ending an unfinished broadcast names the entry's own feed: an old
        // "bzz" one on a raw topic, a current one on the text topic.
        let old = BroadcastFeed.recovery(topic: hex.uppercased(), topicText: "", format: "bzz")
        expect(old == .current, false, "an old entry is not the current feed")
        let oldConfig = old.publisherConfig(
            channel: "c", gateway: "g", batchId: "b", bitrateKbps: 1, segmentMs: 1, notes: "",
            recoveryPath: "p", resume: true, reapMs: 0)
        expect(oldConfig["format"] as? String, "bzz", "old entry format")
        expect(oldConfig["topic"] as? String, hex, "old entry raw topic")
        expect(oldConfig["topic_text"] == nil, true, "old entry has no topic text")
        let legacyText = BroadcastFeed.recovery(topic: hex, topicText: "antstream-1234", format: "bzz")
        expect(legacyText.publisherConfig(
            channel: "c", gateway: "g", batchId: "b", bitrateKbps: 1, segmentMs: 1, notes: "",
            recoveryPath: "p", resume: true, reapMs: 0)["topic_text"] as? String,
               "antstream-1234", "old entry text topic")
        expect(BroadcastFeed.recovery(topic: hex, topicText: "antstream", format: "solar-punk"),
               .current, "a current entry is the current feed")

        streamInfoChecks()
        catalogChecks()

        print("channel address: \(checks - failures)/\(checks) checks passed")
        exit(failures == 0 ? 0 : 1)
    }

    /// Stream info (#220): the `"info"` object the library takes, in the
    /// publisher config and the announcement's config, and what the app
    /// keeps across launches.
    static func streamInfoChecks() {
        let ref = String(repeating: "AB", count: 32)
        let info = StreamInfo(
            title: "  Morning walk ", description: "Along the river.", thumbnail: "0x" + ref,
            thumbnailPicked: true, scheduledStartMs: 1_792_224_000_000,
            announcedStartMs: 1_792_224_000_000)
        let object = info.infoObject()
        expect(object["title"] as? String, "Morning walk", "info title trimmed")
        expect(object["description"] as? String, "Along the river.", "info description")
        expect(object["thumbnail"] as? String, "0x" + ref.lowercased(), "info thumbnail")
        expect(object["scheduled_start_ms"] as? UInt64, 1_792_224_000_000, "info scheduled start")
        expect(object.count, 4, "info carries the library's four fields only")
        expect(info.announcedStart != nil, true, "announced start")
        expect(info.forBroadcast.infoObject()["scheduled_start_ms"] is NSNull, true,
               "going live leaves the schedule behind")
        expect(StreamInfo(thumbnail: "not-a-ref").infoObject()["thumbnail"] as? String, "",
               "a thumbnail that is no reference is left out")
        expect(StreamInfo().displayTitle(channel: "Kitchen"), "Kitchen", "empty title is the channel")
        var rescheduled = info
        rescheduled.scheduledStartMs = 1_792_300_000_000
        expect(rescheduled.announcedStart == nil, true, "a changed start is not announced yet")

        // The JSON is what `ant_publisher_set_info` gets.
        let json = info.infoJSON().flatMap { $0.data(using: .utf8) }
            .flatMap { try? JSONSerialization.jsonObject(with: $0) as? [String: Any] }
        expect(json?["title"] as? String, "Morning walk", "info JSON")
        let unscheduled = StreamInfo(title: "t").infoJSON() ?? ""
        expect(unscheduled.contains("\"scheduled_start_ms\":null"), true, "no schedule is null")

        // The publisher config carries it, the announcement's config too.
        let config = BroadcastFeed.current.publisherConfig(
            channel: "c", gateway: "g", batchId: "b", bitrateKbps: 1, segmentMs: 1, notes: "",
            recoveryPath: "p", resume: false, reapMs: 0, info: info.forBroadcast)
        expect((config["info"] as? [String: Any])?["title"] as? String, "Morning walk", "config info")
        let without = BroadcastFeed.current.publisherConfig(
            channel: "c", gateway: "g", batchId: "b", bitrateKbps: 1, segmentMs: 1, notes: "",
            recoveryPath: "p", resume: false, reapMs: 0)
        expect(without["info"] == nil, true, "no info without one")
        let announce = info.catalogPublishConfig(
            gateway: "http://127.0.0.1:1633", batchId: "0xabc", channel: "Kitchen",
            width: 360, height: 640, bitrateKbps: 664)
        expect(announce["topic_text"] as? String, "antstream", "announcement topic text")
        expect(announce["channel"] as? String, "Kitchen", "announcement channel")
        expect(((announce["info"] as? [String: Any])?["scheduled_start_ms"]) as? UInt64,
               1_792_224_000_000, "announcement start")

        // Saved and read back, and a saved record missing fields still reads.
        let saved = (try? JSONEncoder().encode(info)).flatMap { try? JSONDecoder().decode(StreamInfo.self, from: $0) }
        expect(saved, info, "stream info round trip")
        let sparse = try? JSONDecoder().decode(StreamInfo.self, from: Data("{\"title\":\"x\"}".utf8))
        expect(sparse, StreamInfo(title: "x"), "missing fields default")
    }

    /// The catalog entry the Watch tab shows (#220), read from the
    /// library's fixtures and from catalogs nobody should trust.
    static func catalogChecks() {
        let fixtureOwner = "d5572300e441b77b72bdd318bbfa7b97a1f03096"
        let channel = ChannelAddress(owner: "0x" + fixtureOwner.uppercased(), text: "antstream")
        let dir = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
            .appendingPathComponent("../../crates/ant-ffi/testdata/catalog")
        for name in ["live", "vod", "scheduled"] {
            guard let data = try? Data(contentsOf: dir.appendingPathComponent("\(name).json")) else {
                expect("missing", "present", "fixture \(name).json")
                continue
            }
            let entry = CatalogEntry.find(in: data, channel: channel)
            expect(entry?.title, "Morning walk", "\(name) title")
            expect(entry?.description, "Along the river.", "\(name) description")
            expect(entry?.state, name, "\(name) state")
            expect(entry?.thumbnail, String(repeating: "ab", count: 32), "\(name) thumbnail")
            expect(entry?.scheduledStart, Date(timeIntervalSince1970: 1_792_224_000), "\(name) start")
            expect(entry?.upcomingStart != nil, name == "scheduled", "\(name) upcoming")
            let other = ChannelAddress(owner: owner, text: "antstream")
            expect(CatalogEntry.find(in: data, channel: other), nil, "\(name) other owner")
            let otherTopic = ChannelAddress(owner: fixtureOwner, text: "elsewhere")
            expect(CatalogEntry.find(in: data, channel: otherTopic), nil, "\(name) other topic")
        }

        // Garbage never crashes and never shows.
        for body in ["", "null", "{}", "\"x\"", "[1, \"a\", null]", "[{\"owner\": 5}]",
                     "[{\"owner\": \"0x\(fixtureOwner)\", \"topic\": \"antstream\"}]",
                     "[{\"owner\": \"\(fixtureOwner)\", \"topic\": \"antstream\", \"title\": 7}]",
                     "#EXTM3U\n#EXT-X-VERSION:7\n"] {
            expect(CatalogEntry.find(in: Data(body.utf8), channel: channel), nil, "garbage \(body)")
        }
        let odd = """
            [{"owner": "0x\(fixtureOwner.uppercased())", "topic": "antstream", "title": "  Hi  ",
              "description": ["no"], "thumbnail": "zz", "scheduledStartTime": 12, "state": "live"}]
            """
        let entry = CatalogEntry.find(in: Data(odd.utf8), channel: channel)
        expect(entry?.title, "Hi", "odd title trimmed")
        expect(entry?.description, "", "odd description ignored")
        expect(entry?.thumbnail, nil, "odd thumbnail ignored")
        expect(entry?.scheduledStart, nil, "odd start ignored")
        let long = "[{\"owner\": \"\(fixtureOwner)\", \"topic\": \"antstream\", \"title\": \"\(String(repeating: "x", count: 5000))\"}]"
        expect(CatalogEntry.find(in: Data(long.utf8), channel: channel)?.title.count,
               CatalogEntry.maxTitle, "a long title is capped")
        expect(CatalogEntry.parseDate("2026-10-17T08:00:00Z"), Date(timeIntervalSince1970: 1_792_224_000),
               "a start without fractional seconds")
        // Several entries for the channel: the newest `timestamp` wins, the
        // later one on a tie, whatever the order (Android and the web
        // player choose the same), and an empty one never hides the rest.
        let several = """
            [{"owner": "\(fixtureOwner)", "topic": "antstream", "title": "New", "timestamp": 300},
             {"owner": "\(fixtureOwner)", "topic": "antstream", "title": "Old", "timestamp": 100},
             {"owner": "\(fixtureOwner)", "topic": "antstream", "title": "Undated"},
             {"owner": "\(fixtureOwner)", "topic": "elsewhere", "title": "Other", "timestamp": 900}]
            """
        expect(CatalogEntry.find(in: Data(several.utf8), channel: channel)?.title, "New", "newest entry wins")
        let tie = """
            [{"owner": "\(fixtureOwner)", "topic": "antstream", "title": "First", "timestamp": 5},
             {"owner": "\(fixtureOwner)", "topic": "antstream", "title": "Second", "timestamp": 5}]
            """
        expect(CatalogEntry.find(in: Data(tie.utf8), channel: channel)?.title, "Second", "a tie goes to the later")
        let emptyFirst = """
            [{"owner": "\(fixtureOwner)", "topic": "antstream", "timestamp": 1},
             {"owner": "\(fixtureOwner)", "topic": "antstream", "title": "Later", "timestamp": 2}]
            """
        expect(CatalogEntry.find(in: Data(emptyFirst.utf8), channel: channel)?.title, "Later",
               "an empty entry before the channel's real one")
        // A raw-topic channel finds the entry whose text hashes to it.
        let raw = ChannelAddress(owner: fixtureOwner, rawTopic: String(repeating: "c", count: 64))
        let hash: (String) -> String? = { $0 == "antstream" ? String(repeating: "C", count: 64) : nil }
        expect(CatalogEntry.find(in: Data(several.utf8), channel: raw, topicHex: hash)?.title, "New",
               "raw topic matched by hash")
        expect(CatalogEntry.find(in: Data(several.utf8), channel: raw), nil, "raw topic without a hasher")
    }
}
