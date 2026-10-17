import PhotosUI
import SwiftUI
import UIKit

/// The stream info editor (#220): the title, description, thumbnail and
/// scheduled start of the account's channel, as its catalog entry tells
/// players. Opened from the Broadcast tab's stream info card and from the
/// live screen, before going live and while live.
///
/// What it saves is remembered per account for the next broadcast
/// (``AntNode/saveStreamInfo(title:description:scheduledStart:thumbnail:sync:)``).
/// While live, saving rewrites the running broadcast's catalog entry at
/// once (`ant_publisher_set_info`). Before going live, a scheduled start
/// can be announced (`ant_stream_catalog_publish`), so players list the
/// broadcast as upcoming.
struct StreamInfoEditor: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss

    /// A broadcast is running: saving goes to it at once, and there is
    /// nothing to schedule.
    let live: Bool
    /// The channel's name, which an empty title stands for.
    let channelName: String

    @State private var title = ""
    @State private var details = ""
    /// The thumbnail shown: the account's, or a photo just chosen.
    @State private var image: Data?
    /// The shown thumbnail is a photo the broadcaster chose.
    @State private var picked = false
    @State private var thumbnailEdit: AntNode.ThumbnailEdit = .keep
    @State private var pickerItem: PhotosPickerItem?
    @State private var photoProblem: String?
    @State private var scheduled = false
    @State private var start = Date().addingTimeInterval(3600)
    @State private var announcing = false
    @State private var outcome: Outcome?
    @State private var loaded = false

    struct Outcome {
        let text: String
        let failed: Bool
    }

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .broadcast)
                ScrollView {
                    VStack(spacing: 18) {
                        textCard
                        thumbnailCard
                        if !live { scheduleCard }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 8)
                    .padding(.bottom, 40)
                }
                .scrollIndicators(.hidden)
                .scrollDismissesKeyboard(.interactively)
            }
            .navigationTitle("Stream info")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                // Held while an announcement is being written: it carries
                // on whether the sheet is open or not, and Go live is
                // refused until it returns, so the sheet stays up to say
                // how it went.
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                        .disabled(busy)
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") {
                        save()
                        dismiss()
                    }
                    .disabled(!canSave || busy)
                }
            }
        }
        .interactiveDismissDisabled(busy)
        .preferredColorScheme(.dark)
        .onAppear(perform: load)
        .onChange(of: pickerItem) { _, item in
            guard let item else { return }
            Task { await choose(item) }
        }
    }

    // MARK: title and description

    private var textCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                sectionLabel("TITLE")
                TextField(channelName, text: $title)
                    .textFieldStyle(.plain)
                    .font(.headline)
                    .foregroundStyle(.white)
                    .padding(12)
                    .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                if !canSave {
                    Text("A title is needed: players list the channel by it.")
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
                sectionLabel("DESCRIPTION")
                TextField("Optional", text: $details, axis: .vertical)
                    .lineLimit(3...8)
                    .textFieldStyle(.plain)
                    .font(.subheadline)
                    .foregroundStyle(.white)
                    .padding(12)
                    .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                Text(live
                     ? "Players showing your channel get the change within seconds."
                     : "Players list your channel with these. Remembered for your next broadcast.")
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.6))
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }

    // MARK: thumbnail

    private var thumbnailCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                sectionLabel("THUMBNAIL")
                HStack(alignment: .top, spacing: 14) {
                    StreamThumbnailView(jpeg: image, width: 128)
                    Text(thumbnailNote)
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.7))
                        .fixedSize(horizontal: false, vertical: true)
                }
                if let photoProblem {
                    Text(photoProblem)
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
                HStack(spacing: 10) {
                    PhotosPicker(selection: $pickerItem, matching: .images) {
                        Label("Choose photo", systemImage: "photo.on.rectangle")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.white)
                            .padding(.horizontal, 16).padding(.vertical, 10)
                            .glassEffect(.regular.tint(.blue.opacity(0.35)), in: .capsule)
                            .contentShape(.capsule)
                    }
                    .buttonStyle(.plain)
                    if image != nil {
                        GlassPillButton(title: "Remove", icon: "xmark", tint: .white.opacity(0.18)) {
                            image = nil
                            picked = false
                            thumbnailEdit = .remove
                        }
                    }
                }
            }
        }
    }

    private var thumbnailNote: String {
        let camera = live
            ? "A frame from the camera is taken a few seconds into a broadcast"
            : "A frame from the camera is taken a few seconds after you go live"
        if picked {
            var note = "Your photo, kept for every broadcast. Remove it and the camera's frame is used again."
            if case .picked = thumbnailEdit {
                note += node.hasStorage ? " It goes up when you save." : " It goes up when you go live."
            } else if node.streamInfo.thumbnailPending {
                note += " Not uploaded yet: it goes up when you go live."
            }
            return note
        }
        if image != nil {
            return "Taken from the camera on your last broadcast. \(camera), unless you choose a photo."
        }
        return "\(camera), unless you choose a photo."
    }

    private func choose(_ item: PhotosPickerItem) async {
        defer { pickerItem = nil }
        guard let data = try? await item.loadTransferable(type: Data.self),
              let photo = UIImage(data: data),
              let jpeg = StreamThumbnail.jpeg(from: photo)
        else {
            photoProblem = "That photo couldn't be read; try another one."
            return
        }
        photoProblem = nil
        image = jpeg
        picked = true
        thumbnailEdit = .picked(jpeg)
    }

    // MARK: scheduled start

    private var scheduleCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Toggle(isOn: $scheduled) {
                    VStack(alignment: .leading, spacing: 2) {
                        Text("Scheduled start")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.white)
                        Text("Announce an upcoming broadcast")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.6))
                    }
                }
                .tint(.red)
                if scheduled {
                    DatePicker("Starts", selection: $start, in: Date()...,
                               displayedComponents: [.date, .hourAndMinute])
                        .font(.subheadline)
                        .foregroundStyle(.white)
                    if let announced = node.streamInfo.announcedStart {
                        Label(Self.millis(announced) == Self.millis(start)
                              ? "Announced: starts \(announced.formatted(date: .abbreviated, time: .shortened))"
                              : "Announced for \(announced.formatted(date: .abbreviated, time: .shortened)): announce again for the new time",
                              systemImage: "megaphone.fill")
                            .font(.caption)
                            .foregroundStyle(Self.millis(announced) == Self.millis(start)
                                             ? Color.green : Color.yellow)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    Text("Announcing lists the broadcast in players as upcoming, with its start time, before you go live. It is an upload, so it needs a storage plan.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                        .fixedSize(horizontal: false, vertical: true)
                    if busy {
                        HStack(spacing: 8) {
                            ProgressView().controlSize(.small).tint(.white)
                            Text("Announcing…")
                                .font(.subheadline)
                                .foregroundStyle(.white.opacity(0.7))
                        }
                    } else {
                        GlassPillButton(title: "Announce", icon: "megaphone.fill",
                                        tint: .red.opacity(canAnnounce ? 0.5 : 0.15)) {
                            Task { await announce() }
                        }
                        .disabled(!canAnnounce)
                        .opacity(canAnnounce ? 1 : 0.5)
                    }
                    if !node.hasStorage {
                        Text("Connect a storage plan to announce.")
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                    if let outcome {
                        Text(outcome.text)
                            .font(.caption)
                            .foregroundStyle(outcome.failed ? Color.orange : Color.green)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
            }
        }
    }

    private var canAnnounce: Bool {
        scheduled && canSave && node.hasStorage && !node.broadcastRunning
            && !busy && start > Date()
    }

    /// An announcement is being written: this sheet's, or one an earlier
    /// opening of it started.
    private var busy: Bool { announcing || node.announcingBroadcast }

    private func announce() async {
        announcing = true
        outcome = nil
        save(sync: false)
        do {
            try await node.announceBroadcast()
            outcome = Outcome(
                text: "Announced: starts \(start.formatted(date: .abbreviated, time: .shortened)).",
                failed: false)
        } catch {
            outcome = Outcome(text: error.localizedDescription, failed: true)
        }
        announcing = false
    }

    // MARK: load / save

    private var canSave: Bool {
        !title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private func load() {
        guard !loaded else { return }
        loaded = true
        let info = node.streamInfo
        title = info.displayTitle(channel: channelName)
        details = info.description
        image = node.thumbnailJPEG
        picked = info.thumbnailPicked
        if let scheduledStart = info.scheduledStart, scheduledStart > Date() {
            scheduled = true
            start = scheduledStart
        }
    }

    /// Remember the draft. A title that is just the channel's name is
    /// saved empty, so it keeps following the name.
    private func save(sync: Bool = true) {
        let trimmed = title.trimmingCharacters(in: .whitespacesAndNewlines)
        node.saveStreamInfo(
            title: trimmed == channelName ? "" : trimmed,
            description: details,
            scheduledStart: live ? node.streamInfo.scheduledStart : (scheduled ? start : nil),
            thumbnail: thumbnailEdit,
            sync: sync
        )
        // Saved: a second save must not store (and upload) it again.
        thumbnailEdit = .keep
    }

    private func sectionLabel(_ text: String) -> some View {
        Text(text)
            .font(.system(.caption, design: .rounded).weight(.semibold))
            .tracking(2)
            .foregroundStyle(.white.opacity(0.6))
    }

    private static func millis(_ date: Date) -> Int64 {
        Int64((date.timeIntervalSince1970 * 1000).rounded())
    }
}

/// A stream thumbnail in a 16:9 frame, or a placeholder without one.
struct StreamThumbnailView: View {
    let jpeg: Data?
    var width: CGFloat = 96

    var body: some View {
        ZStack {
            Rectangle().fill(.white.opacity(0.08))
            if let jpeg, let image = UIImage(data: jpeg) {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else {
                Image(systemName: "photo")
                    .font(.title3)
                    .foregroundStyle(.white.opacity(0.4))
            }
        }
        .frame(width: width, height: width * 9 / 16)
        .clipShape(.rect(cornerRadius: 10))
    }
}

/// Stream thumbnails (#220): a downscaled JPEG, about 640 px on the long
/// side and a few tens of KB, the size Solar Punk's players show.
enum StreamThumbnail {
    /// `image` scaled down to ``CaptureEngine/thumbnailLongSide`` on its
    /// long side (never up), as a JPEG.
    static func jpeg(from image: UIImage) -> Data? {
        let size = image.size
        guard size.width > 0, size.height > 0 else { return nil }
        let scale = min(1, CaptureEngine.thumbnailLongSide / max(size.width, size.height))
        let target = CGSize(width: (size.width * scale).rounded(), height: (size.height * scale).rounded())
        let format = UIGraphicsImageRendererFormat()
        format.scale = 1
        format.opaque = true
        let resized = UIGraphicsImageRenderer(size: target, format: format).image { _ in
            image.draw(in: CGRect(origin: .zero, size: target))
        }
        return resized.jpegData(compressionQuality: 0.7)
    }

    /// A drawn landscape (sky, sun, hills, a river) for the
    /// `antstream-visual` stream info shots, which have no camera or
    /// photo library to take one from.
    static func sampleJPEG() -> Data? {
        let size = CGSize(width: 640, height: 360)
        let format = UIGraphicsImageRendererFormat()
        format.scale = 1
        format.opaque = true
        let image = UIGraphicsImageRenderer(size: size, format: format).image { context in
            let cg = context.cgContext
            let sky = [UIColor(red: 0.98, green: 0.62, blue: 0.38, alpha: 1).cgColor,
                       UIColor(red: 0.36, green: 0.42, blue: 0.78, alpha: 1).cgColor] as CFArray
            if let gradient = CGGradient(colorsSpace: CGColorSpaceCreateDeviceRGB(), colors: sky,
                                         locations: [0, 1]) {
                cg.drawLinearGradient(gradient, start: CGPoint(x: 0, y: size.height),
                                      end: .zero, options: [])
            }
            UIColor(red: 1, green: 0.9, blue: 0.6, alpha: 1).setFill()
            cg.fillEllipse(in: CGRect(x: 400, y: 90, width: 110, height: 110))
            UIColor(red: 0.18, green: 0.36, blue: 0.27, alpha: 1).setFill()
            let hills = UIBezierPath()
            hills.move(to: CGPoint(x: 0, y: 250))
            hills.addCurve(to: CGPoint(x: 640, y: 230), controlPoint1: CGPoint(x: 200, y: 160),
                           controlPoint2: CGPoint(x: 420, y: 300))
            hills.addLine(to: CGPoint(x: 640, y: 360))
            hills.addLine(to: CGPoint(x: 0, y: 360))
            hills.close()
            hills.fill()
            UIColor(red: 0.55, green: 0.75, blue: 0.9, alpha: 1).setFill()
            let river = UIBezierPath()
            river.move(to: CGPoint(x: 280, y: 360))
            river.addCurve(to: CGPoint(x: 330, y: 262), controlPoint1: CGPoint(x: 360, y: 320),
                           controlPoint2: CGPoint(x: 250, y: 290))
            river.addLine(to: CGPoint(x: 350, y: 262))
            river.addCurve(to: CGPoint(x: 420, y: 360), controlPoint1: CGPoint(x: 290, y: 300),
                           controlPoint2: CGPoint(x: 430, y: 320))
            river.close()
            river.fill()
        }
        return image.jpegData(compressionQuality: 0.7)
    }
}
