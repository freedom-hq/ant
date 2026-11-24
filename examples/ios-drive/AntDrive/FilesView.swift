import CoreTransferable
import ImageIO
import PhotosUI
import SwiftUI
import UniformTypeIdentifiers

/// The Files tab — a Dropbox-style list of everything the user has
/// uploaded, with an Add menu (upload photos / upload a file) and
/// per-file actions (copy Swarm hash, pause/resume/cancel, retry).
struct FilesView: View {
    @EnvironmentObject var node: AntNode

    @State private var showImporter = false
    @State private var showPhotoPicker = false
    @State private var showGetStarted = false
    @State private var photoSelection: [PhotosPickerItem] = []
    @State private var banner: String?
    @State private var showAddOptions = false
    /// The file whose full-screen detail page is pushed, if any. A tiny
    /// id wrapper (rather than the `UploadJob` value) so the detail page
    /// re-resolves the live job from `node` and reflects ongoing changes.
    @State private var openJob: JobRef?

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .player)

                ScrollView {
                    VStack(spacing: 18) {
                        header
                        if !node.hasStorage {
                            getStartedCard
                        } else if node.jobs.isEmpty {
                            emptyState
                        } else {
                            ForEach(node.jobs) { job in
                                FileRow(job: job, onPause: pause,
                                        onResume: resume, onCancel: cancel,
                                        onOpen: { openJob = JobRef(id: job.id) })
                            }
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 12)
                    .padding(.bottom, 130)
                }
                .scrollIndicators(.hidden)
            }
            .toolbar(.hidden, for: .navigationBar)
            .navigationDestination(item: $openJob) { ref in
                FileDetailView(jobId: ref.id)
            }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { bannerView }
        .fileImporter(isPresented: $showImporter,
                      allowedContentTypes: [.item],
                      allowsMultipleSelection: true) { handleImport($0) }
        .photosPicker(isPresented: $showPhotoPicker,
                      selection: $photoSelection,
                      maxSelectionCount: 10,
                      matching: .any(of: [.images, .videos]))
        .onChange(of: photoSelection) { _, items in handlePhotos(items) }
        .sheet(isPresented: $showGetStarted) { GetStartedView() }
        .task { await node.refreshAll() }
    }

    // MARK: header

    private var header: some View {
        HStack(spacing: 14) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Files")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text(headerStatus)
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
            addButton
        }
        .padding(.horizontal, 6)
    }

    /// Connectivity line under the "Files" headline. Offline wins over
    /// the peer count — uploads are auto-paused for the duration and
    /// resume on their own once a path is back.
    private var headerStatus: String {
        if node.isOffline { return "Waiting for network…" }
        return node.status.isReady ? "\(node.peerCount) connections" : node.status.label
    }

    private var getStartedCard: some View {
        VStack(spacing: 20) {
            Image(systemName: "sparkles")
                .font(.system(size: 46))
                .foregroundStyle(.white)
                .padding(.top, 60)
            VStack(spacing: 8) {
                Text("Get started")
                    .font(.system(.title, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text("Set up a storage plan to start uploading, downloading, and sharing your files.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 24)
            }
            Button { showGetStarted = true } label: {
                Text("Get started")
                    .font(.headline)
                    .foregroundStyle(.white)
                    .padding(.horizontal, 40).padding(.vertical, 15)
                    .glassEffect(.regular.tint(.blue.opacity(0.6)), in: .capsule)
            }
            .buttonStyle(.plain)
        }
        .frame(maxWidth: .infinity)
    }

    private var emptyState: some View {
        VStack(spacing: 14) {
            Image(systemName: "tray.and.arrow.up")
                .font(.system(size: 52))
                .foregroundStyle(.white.opacity(0.5))
            Text("No files yet")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white)
            Text("Tap + to upload photos or a file.")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.65))
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .padding(.top, 80)
    }

    // MARK: add button + menu

    /// The add button only appears once storage is active — uploads
    /// require a storage plan, so before that the Get Started greeter is
    /// the only call to action. It lives in the header row, top-right,
    /// aligned with the "Files" headline.
    ///
    /// This is a plain glass `Button` (not a `Menu`): a `Menu` whose label
    /// carries a hand-applied `.glassEffect` flickers on iOS 26 because the
    /// system can't morph the custom glass into the menu. The options are
    /// presented via a `confirmationDialog` instead, which has no glass
    /// morph and keeps the button rock-steady.
    @ViewBuilder private var addButton: some View {
        if node.hasStorage {
            Button { showAddOptions = true } label: {
                Image(systemName: "plus")
                    .font(.title2.weight(.semibold))
                    .foregroundStyle(.white)
                    .frame(width: 48, height: 48)
                    .glassEffect(.regular.tint(.blue.opacity(0.5)), in: .circle)
            }
            .buttonStyle(.plain)
            .confirmationDialog("Add to AntDrive", isPresented: $showAddOptions,
                                titleVisibility: .visible) {
                Button("Upload photos") { showPhotoPicker = true }
                Button("Upload a file") { showImporter = true }
                Button("Cancel", role: .cancel) {}
            }
        }
    }

    @ViewBuilder private var bannerView: some View {
        if let banner {
            Text(banner)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.white)
                .padding(.horizontal, 18).padding(.vertical, 12)
                .glassEffect(.regular.tint(.black.opacity(0.4)), in: .capsule)
                .padding(.top, 8)
                .transition(.move(edge: .top).combined(with: .opacity))
        }
    }

    // MARK: actions

    private func pause(_ job: UploadJob) { Task { await node.pauseUpload(job.jobId) } }
    private func resume(_ job: UploadJob) { Task { await node.resumeUpload(job.jobId) } }
    private func cancel(_ job: UploadJob) { Task { await node.cancelUpload(job.jobId) } }

    /// Stage each picked photo/video into the sandbox and kick off an
    /// upload, keeping the original iOS filename. `PhotosPickerItem` hands
    /// us out-of-process data (no photo-library permission prompt); loading
    /// it as a file (rather than raw `Data`) lets us recover the source
    /// file's name (e.g. `IMG_4567.HEIC`) so the row and manifest use the
    /// real name instead of a synthetic `Photo-<stamp>` one.
    private func handlePhotos(_ items: [PhotosPickerItem]) {
        guard !items.isEmpty else { return }
        photoSelection = []
        Task {
            for item in items {
                do {
                    guard let picked = try await item.loadTransferable(type: PickedPhoto.self) else {
                        flash("Couldn't read selected item")
                        continue
                    }
                    let name = picked.url.lastPathComponent
                    let ct = UTType(filenameExtension: picked.url.pathExtension)?.preferredMIMEType
                    try await node.startUpload(path: picked.url, name: name, contentType: ct)
                    flash("Uploading \(name)")
                } catch {
                    flash(error.localizedDescription)
                }
            }
        }
    }

    private func handleImport(_ result: Result<[URL], Error>) {
        guard case .success(let urls) = result else { return }
        Task {
            for url in urls {
                do {
                    let local = try copyIntoSandbox(url)
                    let ct = UTType(filenameExtension: url.pathExtension)?.preferredMIMEType
                    try await node.startUpload(path: local,
                                               name: url.lastPathComponent,
                                               contentType: ct)
                    flash("Uploading \(url.lastPathComponent)")
                } catch {
                    flash(error.localizedDescription)
                }
            }
        }
    }

    /// The Rust uploader re-reads the source on its own schedule,
    /// so copy the security-scoped picked file into our sandbox first to
    /// give it a stable, always-readable path.
    private func copyIntoSandbox(_ url: URL) throws -> URL {
        let fm = FileManager.default
        let dest = try antdriveImportsDir().appendingPathComponent(url.lastPathComponent)
        let scoped = url.startAccessingSecurityScopedResource()
        defer { if scoped { url.stopAccessingSecurityScopedResource() } }
        if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
        try fm.copyItem(at: url, to: dest)
        return dest
    }

    private func flash(_ message: String) {
        withAnimation { banner = message }
        Task {
            try? await Task.sleep(nanoseconds: 2_500_000_000)
            withAnimation { banner = nil }
        }
    }
}

/// A photo/video picked from the library, materialised into the app's
/// sandbox with its original iOS filename. Loading the item as a *file*
/// (rather than raw `Data`) is what gives us the source filename; the
/// importing closure must copy the handed-over file synchronously because
/// the system reclaims it once the closure returns.
private struct PickedPhoto: Transferable {
    let url: URL

    static var transferRepresentation: some TransferRepresentation {
        FileRepresentation(importedContentType: .data) { received in
            let fm = FileManager.default
            let name = received.file.lastPathComponent
            let dest = try antdriveImportsDir().appendingPathComponent(name)
            if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
            try fm.copyItem(at: received.file, to: dest)
            return PickedPhoto(url: dest)
        }
    }
}

/// The sandbox directory the Rust uploader streams imports from. Shared by
/// the file importer and the photo picker so both stage files in one place.
func antdriveImportsDir() throws -> URL {
    let fm = FileManager.default
    let base = try fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                          appropriateFor: nil, create: true)
        .appendingPathComponent("antdrive/imports", isDirectory: true)
    try fm.createDirectory(at: base, withIntermediateDirectories: true)
    return base
}

/// One file row: type icon, name, status line, and a trailing control
/// that changes with state (progress ring while uploading, a menu when
/// done, a retry/cancel pair when paused or failed).
private struct FileRow: View {
    @EnvironmentObject var node: AntNode
    let job: UploadJob
    let onPause: (UploadJob) -> Void
    let onResume: (UploadJob) -> Void
    let onCancel: (UploadJob) -> Void
    let onOpen: () -> Void

    var body: some View {
        GlassCard(radius: 22) {
            HStack(spacing: 14) {
                FileThumbnail(job: job, fallbackIcon: icon)

                VStack(alignment: .leading, spacing: 3) {
                    Text(job.displayName)
                        .font(.body.weight(.semibold))
                        .foregroundStyle(.white)
                        .lineLimit(1)
                    // One detail line: an optional status message (uploading
                    // progress, paused/failed, or a propagation warning)
                    // inline with the file size. Kept to a single line so
                    // every row is the same height (the 40pt thumbnail sets
                    // it); no separate progress bar or badge row.
                    detailText
                        .font(.caption)
                        .lineLimit(1)
                }
                Spacer(minLength: 8)
                trailing
            }
        }
        // The whole card opens the file's detail page. Trailing controls
        // (pause/resume/cancel/retry) are `Button`s, so their taps are
        // consumed first and don't fall through to this gesture.
        // No automatic verification here: a file is only checked when the
        // user runs Check again / Deep verify on its detail page.
        .contentShape(Rectangle())
        .onTapGesture { onOpen() }
    }

    /// The single detail line under the filename: the file size first
    /// (always grey), then an optional status message coloured by state
    /// (orange when something is wrong) — e.g. "1.3 MB · Uploading 45%". On
    /// the happy path it's just the grey size.
    private var detailText: Text {
        let grey = Color.white.opacity(0.6)
        let size = Text(formatBytes(job.sourceSize)).foregroundColor(grey)
        guard let message = statusMessage else { return size }
        return size
            + Text(" · ").foregroundColor(grey)
            + Text(message).foregroundColor(messageColor)
    }

    /// Colour for the inline status message: orange when the upload failed
    /// or a completed file's network read-back came back not-retrievable,
    /// neutral grey otherwise (uploading / paused).
    private var messageColor: Color {
        if job.isFailed { return .orange }
        if job.isDone, let p = node.propagation[job.id], !p.isStoredSafely { return .orange }
        // No manual deep-verify yet — fall back to the daemon's automatic heal
        // verdict: a degraded heal is a genuine not-fully-stored warning.
        if node.propagation[job.id] == nil, job.isDegraded { return .orange }
        return .white.opacity(0.6)
    }

    /// The status word shown inline before the size, or `nil` on the happy
    /// path (a completed, retrievable file shows only its size). Covers the
    /// job's own states (uploading/paused/failed) and, for a completed file,
    /// a failed network read-back ("Not fully available yet" / "Not found on
    /// network"). A retrievable verdict — the expected outcome — stays
    /// silent, as does the transient in-progress probe.
    private var statusMessage: String? {
        // Offline: an in-flight or auto-paused upload is waiting on the
        // network, whatever its own label says (the node suspends it
        // and resumes automatically).
        if node.isOffline, job.isActive || job.isAutoPaused {
            return "Waiting for network…"
        }
        // While uploading / paused / failed, the job's own status line wins.
        if !job.isDone { return job.statusLabel.isEmpty ? nil : job.statusLabel }
        // A user-run check in flight beats the heal-derived labels — the
        // row should say what's happening right now.
        if node.verifying.contains(job.id) { return "Checking…" }
        // Completed: a user-run deep verify (propagation) is the richest
        // verdict and *overrides* the daemon's heal flags, which can lag a
        // successful "Push again" — heal may leave the job `degraded` while
        // the re-verify confirms the file is now fully stored. Match the
        // detail page's precedence so the row clears instead of stubbornly
        // showing "Not fully backed up": warn only when the verify says
        // not-stored-safely; stay silent when it confirms safe.
        if let p = node.propagation[job.id] {
            return p.isStoredSafely ? nil : p.label
        }
        // No deep verify yet — fall back to the job's own heal-derived label
        // ("Securing…" / "Not fully backed up" / "").
        return job.statusLabel.isEmpty ? nil : job.statusLabel
    }

    @ViewBuilder private var trailing: some View {
        if job.isActive {
            Button { onPause(job) } label: {
                Image(systemName: "pause.circle.fill")
                    .font(.title2).foregroundStyle(.white.opacity(0.85))
            }
        } else if job.isPaused {
            HStack(spacing: 10) {
                Button { onResume(job) } label: {
                    Image(systemName: "play.circle.fill").font(.title2).foregroundStyle(.white)
                }
                Button { onCancel(job) } label: {
                    Image(systemName: "xmark.circle.fill").font(.title2).foregroundStyle(.white.opacity(0.6))
                }
            }
        } else if job.isDone {
            if job.isSecuring {
                // "Stop securing": cancelling a completed job cleanly
                // stops its heal pass (the job stays completed; "Push
                // again" still works later).
                Button { onCancel(job) } label: {
                    Image(systemName: "stop.circle")
                        .font(.title2).foregroundStyle(.white.opacity(0.6))
                }
            } else {
                // Copy moved to the detail page; the row just hints it's
                // tappable.
                Image(systemName: "chevron.right")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white.opacity(0.4))
            }
        } else if job.isFailed {
            Button { onResume(job) } label: {
                Image(systemName: "arrow.clockwise.circle.fill")
                    .font(.title2).foregroundStyle(.orange)
            }
        }
    }

    private var icon: String { fileTypeIcon(for: job.contentType) }
}

/// SF Symbol name for a file's content type, shared by the list row and the
/// detail page.
func fileTypeIcon(for contentType: String?) -> String {
    let ct = contentType ?? ""
    if ct.hasPrefix("image") { return "photo" }
    if ct.hasPrefix("video") { return "film" }
    if ct.hasPrefix("audio") { return "music.note" }
    if ct.hasPrefix("text") { return "doc.text" }
    if ct.contains("pdf") { return "doc.richtext" }
    if ct.contains("zip") || ct.contains("compressed") { return "archivebox" }
    return "doc"
}

/// The leading element of a file row: a small rounded thumbnail for images
/// whose staged source file is still on disk, falling back to the type's SF
/// Symbol for everything else (videos, docs, or images we can't read yet).
/// The staged file at `job.sourcePath` lives in the sandbox imports dir and
/// isn't deleted after upload, so thumbnails survive across launches.
private struct FileThumbnail: View {
    let job: UploadJob
    let fallbackIcon: String
    /// Rendered side length in points; the loader downsamples to this at
    /// device scale so a 12 MP HEIC never gets fully decoded into memory.
    /// Defaults to the compact list size; the detail page passes a larger one.
    var side: CGFloat = 40
    var cornerRadius: CGFloat = 9
    var iconFont: Font = .title2

    @State private var image: UIImage?

    var body: some View {
        Group {
            if let image {
                Image(uiImage: image)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
                    .frame(width: side, height: side)
                    .clipShape(RoundedRectangle(cornerRadius: cornerRadius, style: .continuous))
            } else {
                Image(systemName: fallbackIcon)
                    .font(iconFont)
                    .foregroundStyle(.white.opacity(0.9))
                    .frame(width: side, height: side)
            }
        }
        .task(id: "\(job.sourcePath)#\(side)") { await load() }
    }

    private func load() async {
        guard image == nil, (job.contentType ?? "").hasPrefix("image") else { return }
        guard let url = localSourceURL(for: job) else { return }
        let maxPixel = side * 3  // cover @3x Retina
        let loaded = await Task.detached(priority: .utility) {
            ThumbnailLoader.downsample(url: url, maxPixel: maxPixel)
        }.value
        guard let loaded else { return }
        await MainActor.run { self.image = loaded }
    }
}

/// Resolve the staged source file on disk. The stored `source_path` is an
/// absolute path captured at upload time, but iOS can hand the app a new
/// data-container UUID on reinstall/update, leaving that path dangling. The
/// file itself still lives in the *current* imports dir under the same
/// basename, so prefer that and fall back to the literal path. Shared by the
/// thumbnail loader and the detail page's "Push again" re-upload.
func localSourceURL(for job: UploadJob) -> URL? {
    let fm = FileManager.default
    let basename = (job.sourcePath as NSString).lastPathComponent
    if let imports = try? antdriveImportsDir() {
        let candidate = imports.appendingPathComponent(basename)
        if fm.fileExists(atPath: candidate.path) { return candidate }
    }
    return fm.fileExists(atPath: job.sourcePath) ? URL(fileURLWithPath: job.sourcePath) : nil
}

/// Memory-efficient thumbnail decoding via ImageIO: decodes straight to a
/// small bitmap (handling HEIC and EXIF orientation) instead of loading the
/// full-resolution image. Results are cached in-memory by path so scrolling
/// the list doesn't re-decode.
private enum ThumbnailLoader {
    private static let cache = NSCache<NSString, UIImage>()

    static func downsample(url: URL, maxPixel: CGFloat) -> UIImage? {
        let key = url.path as NSString
        if let cached = cache.object(forKey: key) { return cached }
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil) else { return nil }
        let options: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceCreateThumbnailWithTransform: true,
            kCGImageSourceShouldCacheImmediately: true,
            kCGImageSourceThumbnailMaxPixelSize: maxPixel,
        ]
        guard let cg = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary)
        else { return nil }
        let image = UIImage(cgImage: cg)
        cache.setObject(image, forKey: key)
        return image
    }
}

// MARK: - File detail page

/// Lightweight identifiable wrapper so `navigationDestination(item:)` can
/// drive the push from just a job id; the detail page re-resolves the live
/// `UploadJob` from the node on each render.
struct JobRef: Identifiable, Hashable { let id: String }

/// Full-screen detail page for one file: a large preview, all of its
/// metadata, the network-verification verdict (with the underlying error
/// when something is wrong), and recovery actions — copy, re-check, and a
/// "Push again" re-upload when the file can't be verified.
struct FileDetailView: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    let jobId: String

    @State private var banner: String?
    @State private var busy = false

    /// The live job, re-resolved from the node so progress and verdicts
    /// stay current while the page is open.
    private var job: UploadJob? { node.jobs.first { $0.id == jobId } }

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .player)

            if let job {
                ScrollView {
                    VStack(spacing: 18) {
                        previewCard(job)
                        transferCard(job)
                        if job.isDone { verificationCard(job) }
                        detailsCard(job)
                        if let err = job.lastError, !err.isEmpty { errorCard(err) }
                        actions(job)
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 12)
                    .padding(.bottom, 60)
                }
                .scrollIndicators(.hidden)
            } else {
                Text("This file is no longer available.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
            }
        }
        .navigationTitle(job?.displayName ?? "File")
        .navigationBarTitleDisplayMode(.inline)
        .toolbarBackground(.hidden, for: .navigationBar)
        .tint(.white)
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                if let job, job.reference?.isEmpty == false {
                    Menu {
                        Button { copyHash(job) } label: { Label("Copy Swarm hash", systemImage: "number") }
                        Button { copyLink(job) } label: { Label("Copy link", systemImage: "link") }
                    } label: {
                        Image(systemName: "square.and.arrow.up").foregroundStyle(.white)
                    }
                }
            }
        }
        .overlay(alignment: .top) { bannerView }
        .preferredColorScheme(.dark)
        // No automatic verification — the user starts a check explicitly
        // with Check again / Deep verify below.
    }

    // MARK: cards

    private func previewCard(_ job: UploadJob) -> some View {
        GlassCard {
            HStack(spacing: 16) {
                FileThumbnail(job: job, fallbackIcon: fileTypeIcon(for: job.contentType),
                              side: 72, cornerRadius: 16, iconFont: .largeTitle)
                VStack(alignment: .leading, spacing: 7) {
                    Text(job.displayName)
                        .font(.title3.weight(.semibold))
                        .foregroundStyle(.white)
                        .lineLimit(3)
                    Text(formatBytes(job.sourceSize))
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                    statusBadge(job)
                }
                Spacer(minLength: 0)
            }
        }
    }

    private func statusBadge(_ job: UploadJob) -> some View {
        let s = statusDisplay(job)
        return Label(s.text, systemImage: s.icon)
            .font(.caption.weight(.semibold))
            .foregroundStyle(s.color)
            .padding(.horizontal, 10).padding(.vertical, 5)
            .background(s.color.opacity(0.16), in: .capsule)
    }

    /// Live progress for the two long-running phases of getting a file onto
    /// the network: the initial **upload** (pushing every chunk once) and the
    /// post-upload **securing** heal (reading them back and re-storing any
    /// that didn't land deeply). Each draws a determinate bar when the daemon
    /// reports counts, and an indeterminate one during the brief steps that
    /// don't. Renders nothing once the file is uploaded and secured.
    @ViewBuilder
    private func transferCard(_ job: UploadJob) -> some View {
        if job.isActive {
            GlassCard {
                progressBlock(
                    icon: "arrow.up.circle",
                    title: job.fraction.map { "Uploading \(Int($0 * 100))%" } ?? "Uploading…",
                    detail: job.chunksTotal.map { "\(job.chunksPushed) / \($0) parts pushed" },
                    explanation: "Splitting the file into encrypted parts and pushing each one to the network.",
                    fraction: job.fraction,
                    tint: .blue)
            }
        } else if job.isSecuring {
            GlassCard {
                progressBlock(
                    icon: job.isQueuedToSecure ? "clock" : "lock.shield",
                    title: job.securingPhaseLabel,
                    detail: securingDetail(job),
                    explanation: job.isQueuedToSecure
                        ? "Waiting its turn — the app secures a few files at a time so each finishes quickly instead of all crawling at once."
                        : "Confirming every part landed in its true place on the network — and re-storing any that didn't — so anyone can open this file, not just this device.",
                    fraction: job.securingFraction,
                    tint: .blue)
            }
        } else if job.isDegraded,
                  node.propagation[job.id]?.isStoredSafely != true,
                  !node.verifying.contains(job.id) {
            // A user-run verify that confirmed the file retrievable is
            // the richest verdict (same precedence as the row and the
            // status badge): don't contradict it with a warning-toned
            // "Propagating" card. The node still optimises placement in
            // the background; the verification card carries a one-line
            // note instead.
            GlassCard {
                progressBlock(
                    icon: "arrow.triangle.2.circlepath",
                    title: job.propagatingLabel,
                    detail: job.healMissing.flatMap { m in
                        job.chunksTotal.map { "\(m) / \($0) parts unconfirmed" }
                    },
                    explanation: degradedExplanation(job),
                    fraction: nil,
                    tint: .orange)
            }
        }
    }

    /// Explanation under the "Propagating…" card: what's left, that the
    /// node retries on its own, the check history (proof the loop is
    /// alive), and when the next check is due.
    private func degradedExplanation(_ job: UploadJob) -> String {
        var text = "Some parts haven't confirmed their place on the network yet. The app re-checks and re-stores them automatically until the file verifies."
        var facts: [String] = []
        if let history = job.checkHistoryLabel { facts.append(history) }
        if let next = job.nextCheckLabel { facts.append(next) }
        if !facts.isEmpty {
            text += " \(facts.joined(separator: " \u{00B7} "))."
        }
        return text
    }

    /// "12 / 480 parts" for a securing step that carries determinate counts.
    private func securingDetail(_ job: UploadJob) -> String? {
        guard let checked = job.healChecked, let total = job.healTotal else { return nil }
        return "\(checked) / \(total) parts"
    }

    /// Shared header + (in)determinate linear bar used by `transferCard`.
    private func progressBlock(
        icon: String,
        title: String,
        detail: String?,
        explanation: String,
        fraction: Double?,
        tint: Color
    ) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                Image(systemName: icon).foregroundStyle(tint)
                Text(title)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.white)
                Spacer()
                if let detail {
                    Text(detail)
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(.white.opacity(0.6))
                }
            }
            if let fraction {
                ProgressView(value: fraction).progressViewStyle(.linear).tint(tint)
            } else {
                ProgressView().progressViewStyle(.linear).tint(tint)
            }
            Text(explanation)
                .font(.caption).foregroundStyle(.white.opacity(0.6))
        }
    }

    @ViewBuilder
    private func verificationCard(_ job: UploadJob) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                sectionTitle("VERIFICATION")

                if node.verifying.contains(job.id) {
                    let prog = node.verifyProgress[job.id]
                    VStack(alignment: .leading, spacing: 10) {
                        HStack(spacing: 8) {
                            Image(systemName: "antenna.radiowaves.left.and.right")
                                .foregroundStyle(.blue)
                            Text(prog?.label ?? "Checking the network…")
                                .font(.subheadline).foregroundStyle(.white.opacity(0.85))
                            Spacer()
                            if let f = prog?.fraction {
                                Text("\(Int(f * 100))%")
                                    .font(.caption.monospacedDigit())
                                    .foregroundStyle(.white.opacity(0.6))
                            }
                        }
                        // Determinate once the node reports leaf counts
                        // (the "checking" phase); indeterminate during the
                        // brief resolve/enumerate phases that precede it.
                        if let f = prog?.fraction {
                            ProgressView(value: f)
                                .progressViewStyle(.linear)
                                .tint(.blue)
                        } else {
                            ProgressView()
                                .progressViewStyle(.linear)
                                .tint(.blue)
                        }
                        Button(role: .cancel) { node.cancelVerify() } label: {
                            Label("Cancel", systemImage: "xmark")
                                .font(.subheadline.weight(.semibold))
                                .foregroundStyle(.white)
                                .frame(maxWidth: .infinity).padding(.vertical, 10)
                                .glassEffect(.regular.tint(.white.opacity(0.12)), in: .capsule)
                        }
                        .buttonStyle(.plain)
                    }
                } else if node.repushing.contains(job.id) {
                    let prog = node.repushProgress[job.id]
                    VStack(alignment: .leading, spacing: 10) {
                        HStack(spacing: 8) {
                            Image(systemName: "arrow.up.circle")
                                .foregroundStyle(.orange)
                            Text(prog?.label ?? "Re-pushing missing chunks…")
                                .font(.subheadline).foregroundStyle(.white.opacity(0.85))
                            Spacer()
                            if let f = prog?.fraction {
                                Text("\(Int(f * 100))%")
                                    .font(.caption.monospacedDigit())
                                    .foregroundStyle(.white.opacity(0.6))
                            }
                        }
                        // Determinate while chunks are being re-pushed
                        // (counts known); indeterminate during the read-back
                        // rounds that bracket it.
                        if let f = prog?.fraction {
                            ProgressView(value: f)
                                .progressViewStyle(.linear)
                                .tint(.orange)
                        } else {
                            ProgressView()
                                .progressViewStyle(.linear)
                                .tint(.orange)
                        }
                    }
                } else if let p = node.propagation[job.id] {
                    if p.isStoredSafely {
                        Label("Stored safely", systemImage: "checkmark.seal.fill")
                            .font(.headline).foregroundStyle(.green)
                        Text("Every part of this file is on the network and can be opened by anyone.")
                            .font(.subheadline).foregroundStyle(.white.opacity(0.75))
                        if job.isDegraded {
                            Text("Placement is still being optimised in the background.")
                                .font(.caption).foregroundStyle(.white.opacity(0.55))
                        }
                    } else {
                        Label("Not fully stored", systemImage: "exclamationmark.triangle.fill")
                            .font(.headline).foregroundStyle(.orange)
                        Text(verificationExplanation(p))
                            .font(.subheadline).foregroundStyle(.white.opacity(0.75))
                        if let e = p.error, !e.isEmpty {
                            Text(e)
                                .font(.caption.monospaced())
                                .foregroundStyle(.orange.opacity(0.9))
                                .textSelection(.enabled)
                        }
                    }
                    verifyMetrics(p)
                } else {
                    Text("Not checked yet.")
                        .font(.subheadline).foregroundStyle(.white.opacity(0.7))
                }

                HStack(spacing: 10) {
                    secondaryButton("Check again", icon: "arrow.clockwise") {
                        Task { await node.reverify(job) }
                    }
                    secondaryButton("Deep verify", icon: "shield.lefthalf.filled") {
                        Task { await node.reverify(job, probes: AntNode.deepVerifyProbes) }
                    }
                }
                .disabled(node.verifying.contains(job.id) || job.reference?.isEmpty != false)

                Text("Deep verify fetches every chunk from more independent peers, confirming how widely your file is replicated.")
                    .font(.caption).foregroundStyle(.white.opacity(0.55))
            }
        }
    }

    private func verifyMetrics(_ p: PropagationInfo) -> some View {
        VStack(spacing: 6) {
            replicationRow(p)
            detailRow("Chunks checked", "\(p.checkedChunks) / \(p.totalChunks)")
            if p.leafChunks > 0 {
                detailRow("Data leaves sampled", "\(p.sampledLeaves) / \(p.leafChunks)")
            }
            // Only surface an unreachable count when the file is actually
            // broken — that's the number "Push again" acts on. Shallow-but-
            // reachable parts aren't shown: they don't affect whether the
            // file loads, and listing them under "Stored safely" reads as a
            // problem when there isn't one.
            if !p.retrievable {
                let unreachable = p.checkedChunks >= p.retrievableChunks
                    ? p.checkedChunks - p.retrievableChunks : 0
                if unreachable > 0 {
                    detailRow("Unreachable parts", "\(unreachable)")
                }
            }
        }
        .padding(.top, 2)
    }

    /// Replication floor — how many independent peers held the
    /// least-replicated sampled chunk. Secondary, informational detail
    /// only: the "Stored safely" verdict keys off actual retrievability, so
    /// this never raises an alarm. A constrained node often can't measure a
    /// chunk's own closest-peer copies even when the file is fully
    /// retrievable, so we go green only when we positively saw 2+ routes
    /// and otherwise show a neutral, factual line.
    private func replicationRow(_ p: PropagationInfo) -> some View {
        let color: Color = p.sources >= 2 ? .green : .white.opacity(0.85)
        let icon: String = p.sources >= 2 ? "checkmark.circle.fill" : "arrow.triangle.branch"
        let value: String
        if p.sources >= 2 {
            value = "\(p.sources) independent routes"
        } else if p.sources == 1 {
            value = "1 route"
        } else {
            value = p.retrievable ? "Reachable via forwarding" : "Not reachable"
        }
        return HStack(alignment: .firstTextBaseline) {
            Text("Replication").font(.subheadline).foregroundStyle(.white.opacity(0.6))
            Spacer(minLength: 12)
            Label(value, systemImage: icon)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(color)
        }
    }

    private func detailsCard(_ job: UploadJob) -> some View {
        GlassCard {
            VStack(spacing: 6) {
                sectionTitle("DETAILS").frame(maxWidth: .infinity, alignment: .leading)
                detailRow("Type", job.contentType ?? "Unknown")
                detailRow("Size", formatBytes(job.sourceSize))
                detailRow("Status", job.status.capitalized)
                if job.isActive, let total = job.chunksTotal {
                    detailRow("Pushed", "\(job.chunksPushed) / \(total) chunks")
                }
                detailRow("Created", formatUnixDate(job.createdAtUnix))
                detailRow("Updated", formatUnixDate(job.lastUpdateUnix))
                if let ref = job.reference, !ref.isEmpty {
                    Button { copyHash(job) } label: {
                        HStack(alignment: .top) {
                            Text("Swarm hash")
                                .font(.subheadline).foregroundStyle(.white.opacity(0.6))
                            Spacer(minLength: 12)
                            Text(shortHash(ref))
                                .font(.subheadline.monospaced())
                                .foregroundStyle(.white)
                            Image(systemName: "doc.on.doc")
                                .font(.caption).foregroundStyle(.white.opacity(0.6))
                        }
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    private func errorCard(_ message: String) -> some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 8) {
                Label("Error", systemImage: "xmark.octagon.fill")
                    .font(.headline).foregroundStyle(.red)
                Text(message)
                    .font(.caption.monospaced())
                    .foregroundStyle(.white.opacity(0.85))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    @ViewBuilder
    private func actions(_ job: UploadJob) -> some View {
        VStack(spacing: 12) {
            // Recovery — retry a failed upload, or re-push a completed file
            // that the network couldn't verify.
            if job.isFailed {
                prominentButton("Retry upload", icon: "arrow.clockwise", tint: .blue) {
                    busy = true
                    Task { await node.resumeUpload(job.jobId); busy = false; flash("Retrying…") }
                }
            } else if needsRepush(job) {
                if localSourceURL(for: job) != nil {
                    prominentButton("Push again", icon: "arrow.up.circle.fill", tint: .orange) {
                        repush(job)
                    }
                    Text("Re-pushes the parts that didn't store properly, on this same file.")
                        .font(.caption).foregroundStyle(.white.opacity(0.6))
                        .multilineTextAlignment(.center)
                } else {
                    prominentButton("Push again", icon: "arrow.up.circle.fill", tint: .gray) {}
                        .disabled(true)
                    Text("The original file isn't on this device anymore, so it can't be pushed again.")
                        .font(.caption).foregroundStyle(.white.opacity(0.6))
                        .multilineTextAlignment(.center)
                }
            }

            // Stop the background securing pass. The file stays
            // completed; the pass can be re-run later with "Push again".
            if job.isSecuring {
                secondaryButton("Stop securing", icon: "stop.fill") {
                    Task { await node.cancelUpload(job.jobId) }
                }
            }

            // State controls for an in-flight or paused upload.
            if job.isActive {
                secondaryButton("Pause", icon: "pause.fill") {
                    Task { await node.pauseUpload(job.jobId) }
                }
            } else if job.isPaused {
                secondaryButton("Resume", icon: "play.fill") {
                    Task { await node.resumeUpload(job.jobId) }
                }
                secondaryButton("Cancel upload", icon: "xmark") {
                    Task { await node.cancelUpload(job.jobId); dismiss() }
                }
            }
        }
    }

    @ViewBuilder private var bannerView: some View {
        if let banner {
            Text(banner)
                .font(.footnote.weight(.medium))
                .foregroundStyle(.white)
                .padding(.horizontal, 18).padding(.vertical, 12)
                .glassEffect(.regular.tint(.black.opacity(0.4)), in: .capsule)
                .padding(.top, 8)
                .transition(.move(edge: .top).combined(with: .opacity))
        }
    }

    // MARK: building blocks

    private func sectionTitle(_ s: String) -> some View {
        Text(s)
            .font(.system(.caption, design: .rounded).weight(.semibold))
            .tracking(2)
            .foregroundStyle(.white.opacity(0.6))
    }

    private func detailRow(_ label: String, _ value: String) -> some View {
        HStack(alignment: .top) {
            Text(label).font(.subheadline).foregroundStyle(.white.opacity(0.6))
            Spacer(minLength: 12)
            Text(value)
                .font(.subheadline.weight(.medium))
                .foregroundStyle(.white)
                .multilineTextAlignment(.trailing)
                .textSelection(.enabled)
        }
    }

    private func prominentButton(_ title: String, icon: String, tint: Color,
                                 action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(spacing: 8) {
                if busy { ProgressView().tint(.white) } else { Image(systemName: icon) }
                Text(title)
            }
            .font(.headline).foregroundStyle(.white)
            .frame(maxWidth: .infinity).padding(.vertical, 14)
            .glassEffect(.regular.tint(tint.opacity(0.6)), in: .capsule)
        }
        .buttonStyle(.plain)
        .disabled(busy)
    }

    private func secondaryButton(_ title: String, icon: String,
                                 action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Label(title, systemImage: icon)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.white)
                .frame(maxWidth: .infinity).padding(.vertical, 12)
                .glassEffect(.regular.tint(.white.opacity(0.12)), in: .capsule)
        }
        .buttonStyle(.plain)
    }

    // MARK: logic

    /// A completed file with parts that aren't retrievable from the
    /// network — the genuine "broken upload" case "Push again" exists to
    /// repair. Shallow-but-reachable files are NOT offered a re-push:
    /// they're fine for everyone, and re-pushing can't satisfy a verdict
    /// that's limited by this device's own reach, not the file.
    private func needsRepush(_ job: UploadJob) -> Bool {
        guard job.isDone else { return false }
        // A user-run deep verify is the richer verdict; otherwise fall back to
        // the daemon's automatic heal result — a degraded heal (ran to the end
        // but couldn't confirm deep reachability) is exactly what Push again
        // repairs, so offer it without requiring a manual verify first.
        if let p = node.propagation[job.id] { return !p.isStoredSafely }
        return job.isDegraded
    }

    private func statusDisplay(_ job: UploadJob) -> (text: String, color: Color, icon: String) {
        if job.isFailed { return ("Upload failed", .red, "xmark.octagon.fill") }
        if job.isPaused { return ("Paused", .yellow, "pause.circle.fill") }
        if job.isActive {
            return (job.statusLabel.isEmpty ? "Uploading" : job.statusLabel, .blue, "arrow.up.circle.fill")
        }
        if job.isDone {
            if node.verifying.contains(job.id) { return ("Checking…", .blue, "magnifyingglass") }
            // A user-run deep verify is the richest verdict — prefer it.
            if let p = node.propagation[job.id] {
                return p.isStoredSafely
                    ? ("Stored safely", .green, "checkmark.seal.fill")
                    : ("Not fully stored", .orange, "exclamationmark.triangle.fill")
            }
            // Otherwise show the daemon's free, automatic heal verdict so the
            // file reports its real durability without the user tapping verify.
            if job.isSecuring { return ("Securing…", .blue, "arrow.triangle.2.circlepath") }
            if job.isDurable  { return ("Stored safely", .green, "checkmark.seal.fill") }
            // The node keeps re-queuing the securing pass until the file
            // verifies, so degraded reads as in-progress, not terminal.
            if job.isDegraded { return ("Propagating…", .orange, "arrow.triangle.2.circlepath") }
            return ("Uploaded", .white.opacity(0.8), "checkmark.circle")
        }
        return (job.status.capitalized, .gray, "minus.circle")
    }

    /// Only shown for the genuinely-broken state (`!isStoredSafely`): some
    /// parts aren't retrievable from the network at all, so Push again has
    /// real work to do.
    private func verificationExplanation(_ p: PropagationInfo) -> String {
        if p.totalChunks == 0 {
            return "We couldn't find this file on the network. Push it again to store it."
        }
        return "Some parts of this file aren't on the network yet, so others may not be able to open it. Push it again to store the missing parts."
    }

    /// Re-push only the missing chunks of this completed file, on the same
    /// job — no new upload row. The node runs a background self-heal and we
    /// stay on the page to watch it re-verify.
    private func repush(_ job: UploadJob) {
        Task { await node.repushUpload(job) }
        flash("Re-pushing missing chunks…")
    }

    private func shortHash(_ ref: String) -> String {
        let hex = ref.hasPrefix("0x") ? String(ref.dropFirst(2)) : ref
        guard hex.count > 14 else { return hex }
        return "\(hex.prefix(8))…\(hex.suffix(6))"
    }

    private func copyHash(_ job: UploadJob) {
        guard let ref = job.reference, !ref.isEmpty else { return }
        let hex = ref.hasPrefix("0x") ? String(ref.dropFirst(2)) : ref
        UIPasteboard.general.string = hex
        flash("Copied Swarm hash")
    }

    private func copyLink(_ job: UploadJob) {
        guard let ref = job.reference, !ref.isEmpty else { return }
        let hex = ref.hasPrefix("0x") ? String(ref.dropFirst(2)) : ref
        UIPasteboard.general.string = "bzz://\(hex)"
        flash("Copied link")
    }

    private func flash(_ message: String) {
        withAnimation { banner = message }
        Task {
            try? await Task.sleep(nanoseconds: 2_500_000_000)
            withAnimation { banner = nil }
        }
    }
}

