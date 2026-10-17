package at.vibing.ant.stream

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ChannelAddressTest {
    private val owner = "d31c1f00000000000000000000000000000ec547"
    // keccak256("antstream"), as the web player's keccak.js computes it.
    private val antstream = "f526f44fb746b8c958e2686aafec618f61351d834bd7d9c11b4a6726b6276646"

    @Test
    fun keccakMatchesKnownVectors() {
        assertEquals(
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
            Keccak.hex(Keccak.keccak256(ByteArray(0))),
        )
        assertEquals(antstream, Keccak.topicFromString("antstream"))
        // Longer than one 136-byte block.
        assertEquals(
            "3c3800defb6a25a70a2737e0716eeb5d270559ad3cad8f6abddac58802d7158e",
            Keccak.topicFromString("x".repeat(200)),
        )
        assertEquals(
            "5578f24b50a796dfab6bc55142a70ef25afa15044b9f7113a3659da842868b50",
            Keccak.feedIdentifier(antstream, 5),
        )
        val group = Keccak.ladderGroup(antstream)
        assertEquals("282edb118e6bfa59d32ecb2e6ac0f9a9262c9bbc7d02987c6414b1e73448c019", group)
        assertEquals(
            "b9a50cc4dc1b67882e10374bcc6a6ad2db7bda598dd4b9c33ba2c8f259e52a2e",
            Keccak.ladderMarkerIdentifier(group, 178_000_000),
        )
    }

    @Test
    fun ownChannelLinks() {
        val c = ChannelAddress.ofText("0x" + owner.uppercase(), ChannelAddress.TOPIC)
        assertEquals(owner, c.owner)
        assertEquals(antstream, c.topicHex)
        assertEquals("0x$owner/antstream", c.key)
        assertEquals("swarm://$owner/antstream", c.swarmUri)
        assertEquals("bzz://${ChannelAddress.PLAYER_REF}/#0x$owner/antstream", c.playerLink)
        assertEquals(
            "https://api.gateway.ethswarm.org/bzz/${ChannelAddress.PLAYER_REF}/#0x$owner/antstream",
            c.gatewayPlayerLink,
        )
        val share = c.watchLinks()
        assertTrue(share.contains(c.playerLink))
        assertTrue(share.contains(c.gatewayPlayerLink))
        assertTrue(share.contains("swarm://$owner/antstream"))
    }

    @Test
    fun parsesEveryInputForm() {
        val own = ChannelAddress.ofText(owner, "antstream")
        for (input in listOf(
            "0x$owner/antstream",
            "$owner/antstream",
            "  0x${owner.uppercase()}/antstream ",
            "swarm://$owner/antstream",
            "swarm://0x$owner/antstream",
            "bzz://${ChannelAddress.PLAYER_REF}/#0x$owner/antstream",
            "https://api.gateway.ethswarm.org/bzz/${ChannelAddress.PLAYER_REF}/#0x$owner/antstream",
            // An owner alone is that account's AntStream channel.
            "0x$owner",
            // One link in quotes, as the web player reads it.
            "\"0x$owner/antstream\"",
            "\u201C0x$owner/antstream\u201D",
            "'bzz://${ChannelAddress.PLAYER_REF}/#0x$owner/antstream'",
            "\"0x$owner\"",
        )) {
            assertEquals(input, own, ChannelAddress.parse(input))
        }
    }

    @Test
    fun textAndRawTopics() {
        val spaced = ChannelAddress.parse("0x$owner/my%20show")!!
        assertEquals("my show", spaced.topic)
        assertEquals(Keccak.topicFromString("my show"), spaced.topicHex)
        assertEquals("0x$owner/my%20show", spaced.key)
        assertEquals("ü/?", ChannelAddress.decodeComponent(ChannelAddress.encodeComponent("ü/?")))

        val raw = ChannelAddress.parse("0x$owner/0x${antstream.uppercase()}")!!
        assertTrue(raw.rawTopic)
        assertEquals(antstream, raw.topicHex)
        assertEquals("0x$owner/0x$antstream", raw.key)
        assertNull(raw.swarmUri)
        assertEquals(raw, ChannelAddress.parse(raw.playerLink))
    }

    @Test
    fun rejectsNonChannels() {
        for (input in listOf("", "   ", "antstream", "0x1234/antstream", "bzz://$antstream", "swarm://nope/antstream",
            "swarm://$owner/", "swarm://0x$owner/", "swarm://$owner", "swarm://$owner//antstream",
            "\"swarm://$owner/\"")) {
            assertNull(input, ChannelAddress.parse(input))
        }
        // The Share text, pasted whole: more than one line, not one link.
        val own = ChannelAddress.ofText(owner, "antstream")
        for (sep in listOf("\n", "\r\n", "\u2028")) {
            val text = own.watchLinks().replace("\n", sep)
            assertNull(text, ChannelAddress.parse(text))
        }
    }

    @Test
    fun trimsWhatIosTrims() {
        // Swift's `whitespacesAndNewlines` around the one link: NEL, LS, PS,
        // no-break space and tab go, as on iOS.
        val want = ChannelAddress.ofText(owner, "antstream")
        for (around in listOf("\u0085", "\u2028", "\u2029", "\u00A0", "\t", " \r\n")) {
            val input = "${around}0x$owner/antstream$around"
            assertEquals(input, want, ChannelAddress.parse(input))
        }
        // Not whitespace to Swift (though Kotlin's `trim()` drops it): kept,
        // so the topic is not `antstream`, as on iOS.
        assertEquals("antstream\u001F", ChannelAddress.parse("0x$owner/antstream\u001F")?.topic)
    }

    @Test
    fun publisherConfigIsSolarPunkOnTheAntstreamTopic() {
        val config = AntNode.publisherConfig("Morning show", "http://127.0.0.1:1633", "0xabc", 900, 2000, "n")
        assertEquals("solar-punk", config.getString("format"))
        assertEquals("antstream", config.getString("topic_text"))
        assertTrue(config.getBoolean("ladder"))
        assertEquals("Morning show", config.getString("channel"))
        // The publisher reads the head and continues the feed itself.
        assertFalse(config.has("topic"))
        assertFalse(config.has("start_index"))
    }
}
