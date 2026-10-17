// Unit tests for the solar-punk channel (#217), `AntStream/ChannelAddress.swift`:
// link parsing, the links a channel is shared by, the playlist rewrite the
// Watch tab feeds `AVPlayer`, and the publisher config every broadcast
// starts with. The app has no test target, so the antstream-visual
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

        print("channel address: \(checks - failures)/\(checks) checks passed")
        exit(failures == 0 ? 0 : 1)
    }
}
