package at.vibing.ant.stream.ui

import android.app.DatePickerDialog
import android.app.TimePickerDialog
import android.graphics.BitmapFactory
import android.text.format.DateFormat
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.PickVisualMediaRequest
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.testTagsAsResourceId
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import at.vibing.ant.stream.AntNode
import at.vibing.ant.stream.Channel
import at.vibing.ant.stream.LiveBroadcast
import at.vibing.ant.stream.Thumbnail
import at.vibing.ant.stream.formatStartsAt
import java.io.File
import java.util.Calendar
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * The stream info card (issue #220): the title, description, thumbnail
 * and scheduled start the next broadcast goes out with (or the running
 * one shows), remembered for the account, and the button that edits
 * them. [channelName] is the title's default. Shown on the Broadcast tab
 * and the live screen, before going live and while live.
 */
@OptIn(ExperimentalComposeUiApi::class)
@Composable
fun StreamInfoCard(node: AntNode, broadcast: LiveBroadcast, channelName: String) {
    val channel by node.channel.collectAsState()
    val ch = channel ?: return
    val saved by ch.info.collectAsState()
    val state by broadcast.state.collectAsState()
    val infoError by broadcast.infoError.collectAsState()
    val announcing by broadcast.announcing.collectAsState()
    val announceFailure by broadcast.announceFailure.collectAsState()
    val announceError = broadcast.announceErrorFor(announceFailure, ch.owner, saved.info.scheduledStartMs)
    // Saveable: a rotation mid-announcement reopens the dialog, which
    // picks the running write back up from [LiveBroadcast.announcing].
    var editing by rememberSaveable { mutableStateOf(false) }
    val live = state is LiveBroadcast.State.Live
    val thumb = rememberThumbnail(ch.thumbnailFile, saved)

    SectionCard {
        Column(Modifier.semantics { testTagsAsResourceId = true }, verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Stream info", style = MaterialTheme.typography.titleMedium)
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                thumb?.let {
                    Image(
                        it,
                        contentDescription = "Stream thumbnail",
                        contentScale = ContentScale.Crop,
                        modifier = Modifier.width(96.dp).height(54.dp),
                    )
                }
                Column(Modifier.weight(1f)) {
                    Text(
                        saved.info.title.ifBlank { channelName },
                        style = MaterialTheme.typography.bodyLarge,
                        modifier = Modifier.testTag("streamInfoShownTitle"),
                    )
                    if (saved.info.description.isNotBlank()) {
                        Text(saved.info.description, color = Color.Gray, style = MaterialTheme.typography.bodySmall)
                    }
                }
            }
            Text(
                when {
                    saved.thumbnailPending -> "Thumbnail: your photo, uploaded at Go live"
                    saved.thumbnailPicked -> "Thumbnail: your photo"
                    saved.info.thumbnail != null -> "Thumbnail: a camera frame, taken again at the next Go live"
                    else -> "Thumbnail: a camera frame, taken a few seconds after Go live"
                },
                color = Color.Gray,
                style = MaterialTheme.typography.bodySmall,
            )
            saved.info.scheduledStartMs?.let { start ->
                Text(
                    formatStartsAt(start) + if (saved.announced) " · announced" else " · not announced yet",
                    color = if (saved.announced) Green else Amber,
                    modifier = Modifier.testTag("streamInfoStartsAt"),
                )
                when {
                    announcing -> Text("Announcing…", color = Color.Gray, style = MaterialTheme.typography.bodySmall)
                    !saved.announced && announceError != null -> Text(
                        "Not announced: $announceError",
                        color = Amber,
                        style = MaterialTheme.typography.bodySmall,
                        modifier = Modifier.testTag("streamInfoAnnounceError"),
                    )
                }
            }
            infoError?.let { Text(it, color = Amber, style = MaterialTheme.typography.bodySmall) }
            OutlinedButton(
                onClick = { editing = true },
                modifier = Modifier.fillMaxWidth().testTag("streamInfoEdit"),
            ) { Text(if (live) "Edit stream info (live)" else "Edit stream info") }
        }
    }
    if (editing) {
        StreamInfoDialog(node, broadcast, ch, channelName, live, onDismiss = { editing = false })
    }
}

@OptIn(ExperimentalComposeUiApi::class)
@Composable
private fun StreamInfoDialog(
    node: AntNode,
    broadcast: LiveBroadcast,
    ch: Channel,
    channelName: String,
    live: Boolean,
    onDismiss: () -> Unit,
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val saved by ch.info.collectAsState()
    val plan by node.plan.collectAsState()
    val initial = remember { ch.info.value }
    var title by remember { mutableStateOf(initial.info.title.ifBlank { channelName }) }
    var description by remember { mutableStateOf(initial.info.description) }
    var start by remember { mutableStateOf(initial.info.scheduledStartMs) }
    var picked by remember { mutableStateOf<ByteArray?>(null) }
    var removePhoto by remember { mutableStateOf(false) }
    var work by remember { mutableStateOf<String?>(null) }
    // An announcement this dialog (or one before a rotation) started is
    // still out: the fields wait for it too.
    val announcing by broadcast.announcing.collectAsState()
    val busy = work ?: if (announcing) "Announcing…" else null
    // A write this dialog no longer awaits (started before a rotation)
    // still reports how it ended. Only one that ends after the dialog
    // first opened (saveable, so a rotation keeps the mark) and is about
    // what's saved now: an older failure is the card's to show, not news.
    val openedAt = rememberSaveable { broadcast.nextAnnounceSeq }
    val announceFailure by broadcast.announceFailure.collectAsState()
    val announceError = announceFailure?.takeIf { it.seq >= openedAt }
        ?.let { broadcast.announceErrorFor(it, ch.owner, saved.info.scheduledStartMs) }
    var message by remember { mutableStateOf<Pair<String, Boolean>?>(null) }
    val hasPlan = plan?.enabled == true

    val picker = rememberLauncherForActivityResult(ActivityResultContracts.PickVisualMedia()) { uri ->
        if (uri == null) return@rememberLauncherForActivityResult
        scope.launch {
            message = null
            runCatching { withContext(Dispatchers.IO) { Thumbnail.fromUri(context, uri) } }
                .onSuccess {
                    picked = it
                    removePhoto = false
                }
                .onFailure { message = "Couldn't use that photo: ${it.message}" to false }
        }
    }
    val savedThumb = rememberThumbnail(ch.thumbnailFile, saved)
    val pickedThumb = remember(picked) {
        picked?.let { BitmapFactory.decodeByteArray(it, 0, it.size)?.asImageBitmap() }
    }
    val preview = pickedThumb ?: savedThumb.takeIf { !removePhoto && saved.thumbnailPicked }

    fun pickStart() {
        val cal = Calendar.getInstance().apply {
            timeInMillis = start ?: (System.currentTimeMillis() + 3_600_000L)
        }
        DatePickerDialog(
            context,
            { _, y, m, d ->
                TimePickerDialog(
                    context,
                    { _, h, min ->
                        start = Calendar.getInstance().apply {
                            set(y, m, d, h, min, 0)
                            set(Calendar.MILLISECOND, 0)
                        }.timeInMillis
                    },
                    cal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.MINUTE),
                    DateFormat.is24HourFormat(context),
                ).show()
            },
            cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH),
            cal.get(Calendar.DAY_OF_MONTH),
        ).apply { datePicker.minDate = System.currentTimeMillis() - 60_000 }.show()
    }

    suspend fun save(): String? {
        val problem = broadcast.saveInfo(
            channelName = channelName,
            title = title.ifBlank { channelName },
            description = description,
            scheduledStartMs = start,
            pickedPhoto = picked,
            removePhoto = removePhoto,
        )
        picked = null
        removePhoto = false
        return problem
    }

    Dialog(onDismissRequest = { if (busy == null) onDismiss() }, properties = DialogProperties(usePlatformDefaultWidth = false)) {
        Card(Modifier.fillMaxWidth().padding(16.dp)) {
            Column(
                Modifier
                    .semantics { testTagsAsResourceId = true }
                    .verticalScroll(rememberScrollState())
                    .padding(16.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                Text("Stream info", style = MaterialTheme.typography.titleLarge)
                if (live) {
                    Text(
                        "You're live: saving updates the broadcast's listing at once.",
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
                OutlinedTextField(
                    value = title,
                    onValueChange = { title = it },
                    label = { Text("Title") },
                    singleLine = true,
                    isError = title.isBlank(),
                    supportingText = if (title.isBlank()) ({ Text("Required: empty means \"$channelName\"") }) else null,
                    modifier = Modifier.fillMaxWidth().testTag("streamInfoTitle"),
                )
                OutlinedTextField(
                    value = description,
                    onValueChange = { description = it },
                    label = { Text("Description (optional)") },
                    minLines = 2,
                    modifier = Modifier.fillMaxWidth().testTag("streamInfoDescription"),
                )

                Text("Thumbnail", style = MaterialTheme.typography.labelLarge)
                preview?.let {
                    Image(
                        it,
                        contentDescription = "Picked thumbnail",
                        contentScale = ContentScale.Crop,
                        modifier = Modifier.fillMaxWidth().height(140.dp),
                    )
                }
                Text(
                    if (preview != null) {
                        "Your photo is the thumbnail" + if (!hasPlan) "; it goes up at Go live." else "."
                    } else {
                        "A frame from the camera, taken a few seconds after Go live."
                    },
                    color = Color.Gray,
                    style = MaterialTheme.typography.bodySmall,
                )
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedButton(
                        onClick = {
                            picker.launch(PickVisualMediaRequest(ActivityResultContracts.PickVisualMedia.ImageOnly))
                        },
                        enabled = busy == null,
                        modifier = Modifier.testTag("streamInfoPickPhoto"),
                    ) { Text("Pick photo") }
                    if (preview != null) {
                        TextButton(
                            onClick = {
                                picked = null
                                removePhoto = true
                            },
                            enabled = busy == null,
                            modifier = Modifier.testTag("streamInfoRemovePhoto"),
                        ) { Text("Use camera frame") }
                    }
                }

                Text("Scheduled start (optional)", style = MaterialTheme.typography.labelLarge)
                Text(
                    start?.let { formatStartsAt(it) } ?: "Not scheduled",
                    modifier = Modifier.testTag("streamInfoStartValue"),
                )
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedButton(
                        onClick = { pickStart() },
                        enabled = busy == null,
                        modifier = Modifier.testTag("streamInfoSchedule"),
                    ) { Text(if (start == null) "Set start time" else "Change") }
                    if (start != null) {
                        TextButton(onClick = { start = null }, enabled = busy == null) { Text("Clear") }
                    }
                }
                if (!live) {
                    Text(
                        if (hasPlan) {
                            "Announce lists the upcoming broadcast on your channel, so viewers see when it starts."
                        } else {
                            "Announcing needs a storage plan."
                        },
                        color = Color.Gray,
                        style = MaterialTheme.typography.bodySmall,
                    )
                    OutlinedButton(
                        onClick = {
                            scope.launch {
                                work = "Announcing…"
                                message = null
                                val saveProblem = save()
                                // Awaited here, run on the broadcast's own scope: a
                                // rotation cancels this await, not the write.
                                val err = broadcast.announceAsync(title.ifBlank { channelName }).await()
                                message = when {
                                    err != null -> "Not announced: $err" to false
                                    saveProblem != null -> saveProblem to false
                                    else -> "Announced: ${ch.info.value.info.scheduledStartMs?.let { formatStartsAt(it) }}" to true
                                }
                                work = null
                            }
                        },
                        enabled = busy == null && hasPlan && start != null,
                        modifier = Modifier.fillMaxWidth().testTag("streamInfoAnnounce"),
                    ) { Text("Announce") }
                }

                busy?.let { Text(it, color = Color.Gray) }
                (message ?: announceError?.takeIf { !announcing }?.let { "Not announced: $it" to false })?.let { (text, ok) ->
                    Text(text, color = if (ok) Green else Amber, modifier = Modifier.testTag("streamInfoMessage"))
                }
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp, androidx.compose.ui.Alignment.End)) {
                    TextButton(onClick = onDismiss, enabled = busy == null, modifier = Modifier.testTag("streamInfoClose")) {
                        Text("Close")
                    }
                    Button(
                        onClick = {
                            scope.launch {
                                work = "Saving…"
                                message = null
                                val problem = save()
                                work = null
                                if (problem == null) onDismiss() else message = problem to false
                            }
                        },
                        enabled = busy == null,
                        modifier = Modifier.testTag("streamInfoSave"),
                    ) { Text("Save") }
                }
            }
        }
    }
}

/** The thumbnail saved on the phone, decoded off the main thread; null when there is none. */
@Composable
private fun rememberThumbnail(file: File, key: Any): ImageBitmap? {
    val value by produceState<ImageBitmap?>(null, file, key) {
        value = withContext(Dispatchers.IO) {
            if (file.exists()) runCatching { BitmapFactory.decodeFile(file.path)?.asImageBitmap() }.getOrNull() else null
        }
    }
    return value
}
