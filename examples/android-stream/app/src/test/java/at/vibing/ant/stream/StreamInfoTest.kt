package at.vibing.ant.stream

import java.io.File
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Locale
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class StreamInfoTest {
    private val owner = "d5572300e441b77b72bdd318bbfa7b97a1f03096"
    private val ref = "ab".repeat(32)
    private val vienna = ZoneId.of("Europe/Vienna")

    /** The catalog fixtures the Rust side writes (crates/ant-ffi/testdata/catalog). */
    private fun fixture(name: String): String {
        // Unit tests run in the app module's directory.
        val dir = generateSequence(File("").absoluteFile) { it.parentFile }
            .map { File(it, "crates/ant-ffi/testdata/catalog") }
            .first { it.isDirectory }
        return File(dir, "$name.json").readText()
    }

    @Test
    fun infoJsonLeavesOutWhatIsNotSet() {
        assertEquals(0, StreamInfo().toJson().length())
        assertEquals(0, StreamInfo(title = "  ", description = "", thumbnail = "nothex").toJson().length())
        val o = StreamInfo(" Morning walk ", "Along the river.", ref.uppercase(), 1792224000000).toJson()
        assertEquals("Morning walk", o.getString("title"))
        assertEquals("Along the river.", o.getString("description"))
        assertEquals(ref, o.getString("thumbnail"))
        assertEquals(1792224000000, o.getLong("scheduled_start_ms"))
    }

    @Test
    fun publishedInfoDefaultsTheTitleAndHoldsBackAPendingPhoto() {
        val saved = SavedStreamInfo(StreamInfo(title = "", thumbnail = ref))
        assertEquals("Kitchen", saved.published("Kitchen").title)
        assertEquals(ref, saved.published("Kitchen").thumbnail)
        assertEquals("Mine", saved.copy(info = saved.info.copy(title = "Mine")).published("Kitchen").title)
        assertNull(saved.copy(thumbnailPending = true).published("Kitchen").thumbnail)
    }

    @Test
    fun publisherConfigCarriesTheInfo() {
        val info = StreamInfo("Morning walk", scheduledStartMs = 5)
        val config = AntNode.publisherConfig("Kitchen", "http://127.0.0.1:1633", "0xabc", 900, 2000, "n", info)
        assertEquals("Morning walk", config.getJSONObject("info").getString("title"))
        assertEquals(5, config.getJSONObject("info").getLong("scheduled_start_ms"))
        assertFalse(AntNode.publisherConfig("Kitchen", "gw", "0xabc", 900, 2000, "n").has("info"))
    }

    @Test
    fun catalogPublishConfigNamesTheChannelAndRung() {
        val c = catalogPublishConfig("http://127.0.0.1:1633", "0xabc", "Kitchen", StreamInfo("T", scheduledStartMs = 7), 480, 640, 900)
        assertEquals("antstream", c.getString("topic_text"))
        assertEquals("Kitchen", c.getString("channel"))
        assertEquals("0xabc", c.getString("batch_id"))
        assertEquals(7, c.getJSONObject("info").getLong("scheduled_start_ms"))
        assertEquals(480, c.getInt("width"))
        assertEquals(640, c.getInt("height"))
        assertEquals(900, c.getInt("bitrate_kbps"))
    }

    @Test
    fun savedInfoRoundTripsAndSurvivesGarbage() {
        val saved = SavedStreamInfo(
            StreamInfo("Title", "Desc", ref, 1792224000000),
            thumbnailPicked = true,
            thumbnailPending = false,
            announcedStartMs = 1792224000000,
        )
        assertEquals(saved, SavedStreamInfo.fromPrefsJson(saved.toPrefsJson()))
        assertTrue(saved.announced)
        assertFalse(saved.copy(info = saved.info.copy(scheduledStartMs = 1)).announced)
        assertEquals(SavedStreamInfo(), SavedStreamInfo.fromPrefsJson(null))
        assertEquals(SavedStreamInfo(), SavedStreamInfo.fromPrefsJson("{not json"))
        val odd = SavedStreamInfo.fromPrefsJson("""{"title":5,"thumbnail":"zz","scheduled_start_ms":"soon"}""")
        assertNull(odd.info.thumbnail)
        assertNull(odd.info.scheduledStartMs)
    }

    @Test
    fun afterABroadcastTheAnnouncementIsSpent() {
        val saved = SavedStreamInfo(StreamInfo("T", "D", ref, 9), thumbnailPicked = true, announcedStartMs = 9)
        val after = saved.afterBroadcast()
        assertNull(after.info.scheduledStartMs)
        assertNull(after.announcedStartMs)
        assertEquals("T", after.info.title)
        assertEquals(ref, after.info.thumbnail)
        assertTrue(after.thumbnailPicked)
    }

    @Test
    fun infoIsKeptPerAccount() {
        val a = "0x" + owner
        assertEquals(Channel.prefsName(a), Channel.prefsName(a.uppercase().replace("0X", "0x")))
        assertNotEquals(Channel.prefsName(a), Channel.prefsName("0x" + "11".repeat(20)))
        assertEquals(owner, Channel.ownerKey("0X" + owner.uppercase()))
        assertEquals(owner, Channel.ownerKey(owner))
    }

    @Test
    fun thumbnailSizeKeepsTheLongSideAt640() {
        assertEquals(640 to 360, thumbnailSize(1280, 720))
        assertEquals(360 to 640, thumbnailSize(720, 1280))
        assertEquals(640 to 480, thumbnailSize(4032, 3024))
        assertEquals(320 to 200, thumbnailSize(320, 200))
        assertEquals(0 to 0, thumbnailSize(0, 10))
    }

    @Test
    fun catalogTopicIsTheHashedTopicText() {
        assertEquals(Keccak.topicFromString("antstream-catalog"), Catalog.topicHex)
        assertEquals(
            "http://127.0.0.1:1633/feeds/$owner/${Catalog.topicHex}",
            Catalog.url("http://127.0.0.1:1633/", "0x" + owner.uppercase()),
        )
    }

    @Test
    fun readsTheRustFixtures() {
        val live = Catalog.entryFor(fixture("live"), owner, "antstream")!!
        assertEquals("Morning walk", live.title)
        assertEquals("Along the river.", live.description)
        assertEquals("live", live.state)
        assertEquals(ref, live.thumbnail)
        assertNull(live.startsAt(vienna, Locale.UK))
        assertEquals("vod", Catalog.entryFor(fixture("vod"), owner, "antstream")!!.state)
        val scheduled = Catalog.entryFor(fixture("scheduled"), "0x" + owner.uppercase(), "antstream")!!
        assertEquals(1792224000000, scheduled.scheduledStartMs)
        val now = ZonedDateTime.of(2026, 10, 1, 12, 0, 0, 0, vienna).toInstant().toEpochMilli()
        assertEquals("Starts at Sat 17 Oct, 10:00", scheduled.startsAt(vienna, Locale.UK, now))
    }

    @Test
    fun selectsOnlyThisChannelsEntry() {
        val other = "11".repeat(20)
        val body = """[
            {"owner":"$other","topic":"antstream","title":"Other"},
            {"owner":"0x${owner.uppercase()}","topic":"antstream","title":"Mine"},
            {"owner":"$owner","topic":"elsewhere","title":"Wrong topic"}
        ]"""
        assertEquals("Mine", Catalog.entryFor(body, owner, "antstream")?.title)
        assertEquals("Wrong topic", Catalog.entryFor(body, owner, "elsewhere")?.title)
        assertNull(Catalog.entryFor(body, "22".repeat(20), "antstream"))
    }

    @Test
    fun theNewestEntryWinsAsOnIosAndTheWeb() {
        val body = """[
            {"owner":"$owner","topic":"antstream","title":"New","timestamp":300},
            {"owner":"$owner","topic":"antstream","title":"Old","timestamp":100},
            {"owner":"$owner","topic":"antstream","title":"Undated"},
            {"owner":"$owner","topic":"elsewhere","title":"Other","timestamp":900}
        ]"""
        assertEquals("New", Catalog.entryFor(body, owner, "antstream")?.title)
        val tie = """[
            {"owner":"$owner","topic":"antstream","title":"First","timestamp":5},
            {"owner":"$owner","topic":"antstream","title":"Second","timestamp":5}
        ]"""
        assertEquals("Second", Catalog.entryFor(tie, owner, "antstream")?.title)
        // A raw-topic channel finds the entry whose text hashes to it.
        assertEquals("New", Catalog.select(Catalog.parse(body), owner, Keccak.topicFromString("antstream"))?.title)
    }

    @Test
    fun aBroadcastLeavesTheScheduleBehind() {
        val saved = SavedStreamInfo(info = StreamInfo(title = " ", scheduledStartMs = 1792224000000), announcedStartMs = 1792224000000)
        assertEquals(1792224000000, saved.published("Walk").scheduledStartMs)
        val live = saved.forBroadcast("Walk")
        assertNull(live.scheduledStartMs)
        assertEquals("Walk", live.title)
        assertFalse(live.toJson().has("scheduled_start_ms"))
    }

    @Test
    fun aBadCatalogShowsNothing() {
        for (body in listOf("", "null", "{}", "{\"owner\":\"$owner\"}", "[1,2,", "[null, 3, \"x\"]", "garbage", "[{}]")) {
            assertNull(body, Catalog.entryFor(body, owner, "antstream"))
        }
        // Fields of the wrong type are dropped, the entry is kept.
        val odd = Catalog.entryFor(
            """[{"owner":"$owner","topic":"antstream","title":7,"description":["x"],"state":1,""" +
                """"thumbnail":"short","scheduledStartTime":"tomorrow"}]""",
            owner,
            "antstream",
        )!!
        assertEquals("", odd.title)
        assertNull(odd.description)
        assertNull(odd.state)
        assertNull(odd.thumbnail)
        assertNull(odd.scheduledStartMs)
        // A non-string owner or topic is no entry.
        assertNull(Catalog.entryFor("""[{"owner":1,"topic":"antstream"}]""", owner, "antstream"))
    }

    @Test
    fun scheduledWithoutAReadableTimeHasNoStartsAt() {
        val e = Catalog.entryFor("""[{"owner":"$owner","topic":"antstream","state":"scheduled"}]""", owner, "antstream")!!
        assertNull(e.startsAt(vienna, Locale.UK))
    }

    @Test
    fun formatsStartsAt() {
        val at = { y: Int, mo: Int, d: Int, h: Int, mi: Int ->
            ZonedDateTime.of(y, mo, d, h, mi, 0, 0, vienna).toInstant().toEpochMilli()
        }
        val now = at(2026, 10, 17, 9, 0)
        assertEquals("Starts today at 20:30", formatStartsAt(at(2026, 10, 17, 20, 30), vienna, Locale.UK, now))
        assertEquals("Starts tomorrow at 08:05", formatStartsAt(at(2026, 10, 18, 8, 5), vienna, Locale.UK, now))
        assertEquals("Starts at Wed 21 Oct, 19:00", formatStartsAt(at(2026, 10, 21, 19, 0), vienna, Locale.UK, now))
        assertEquals("Starts at Sat 2 Jan 2027, 19:00", formatStartsAt(at(2027, 1, 2, 19, 0), vienna, Locale.UK, now))
        assertEquals(1792224000000, parseIsoMillis("2026-10-17T08:00:00.000Z"))
        assertEquals(1792224000000, parseIsoMillis("2026-10-17T10:00:00+02:00"))
        assertNull(parseIsoMillis("2026-10-17"))
        // The JSON the config builds is valid for org.json readers too.
        assertTrue(JSONObject(StreamInfo("x").toJson().toString()).has("title"))
    }
}
