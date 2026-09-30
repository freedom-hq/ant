import SwiftUI

/// The Storage tab — the account side of AntStream: how much room the
/// broadcast has (storage plan), the account that pays for it, and where
/// the account key is kept. Ported from `examples/ios-drive`, with the
/// account-key card rewritten around ``AccountKeystore`` (the key lives in
/// the Keychain / Secure Enclave, not in a file).
///
/// Deliberately free of Swarm jargon: a postage batch is a "storage plan",
/// the node EOA is "your account", the signing key is the "account key".
struct StorageView: View {
    @EnvironmentObject var node: AntNode
    @StateObject private var banner = BannerState()

    @AppStorage("gnosisRpc") private var rpc = ""
    @State private var showConnect = false
    @State private var showExtend = false
    @State private var showKey = false
    @State private var showRestore = false
    @State private var exportedKey: String?
    @State private var busy = false
    /// The iCloud switch's own state. Mirrors ``AntNode/keyProtection``,
    /// but is separate from it because turning the switch *off* only asks
    /// a question — the key is not moved until the alert is confirmed.
    @State private var iCloudOn = false
    @State private var confirmICloudOff = false

    var body: some View {
        ZStack {
            LiquidGlassBackground(palette: .network)

            ScrollView {
                VStack(spacing: 18) {
                    header
                    meterCard
                    if node.plan?.enabled == true {
                        if node.settlement?.enabled == false {
                            settlementWarningCard
                        } else if node.settlementDeposit?.needsTopUp == true {
                            settlementDepositCard
                        }
                    } else {
                        connectCard
                    }
                    accountCard
                    accountKeyCard
                    advancedCard
                }
                .padding(.horizontal, 16)
                .padding(.top, 12)
                .padding(.bottom, 130)
            }
            .scrollIndicators(.hidden)
            .refreshable {
                await node.refreshAll()
                await node.refreshValidity(rpc: rpc)
                await node.refreshSettlementDeposit(rpc: activeRpc)
            }
        }
        .preferredColorScheme(.dark)
        .overlay(alignment: .top) { BannerView(message: banner.message) }
        .sheet(isPresented: $showConnect) { connectSheet }
        .sheet(isPresented: $showExtend) {
            ExtendStorageSheet { message in
                banner.flash(message)
                Task { await node.refreshValidity(rpc: rpc) }
            }
        }
        .sheet(isPresented: $showKey, onDismiss: { exportedKey = nil }) { keySheet }
        .sheet(isPresented: $showRestore) {
            RestoreAccountSheet { message in banner.flash(message) }
        }
        // Switching sync off is destructive beyond this device: an iCloud
        // Keychain deletion propagates to every device in the circle, so
        // the user's other iPhone/iPad loses the key too — and the next
        // launch there finds an empty Keychain. Never on a stray tap.
        .alert("Turn off iCloud recovery?", isPresented: $confirmICloudOff) {
            Button("Back up key first") { iCloudOn = true; revealKey() }
            Button("Turn off", role: .destructive) { applyICloudBackup(false) }
            Button("Cancel", role: .cancel) { iCloudOn = true }
        } message: {
            Text("This removes the account key from iCloud Keychain on all your devices, not just this one. Afterwards only this device has it — anywhere else the account can be recovered only from your backed-up key.")
        }
        .onAppear { syncICloudSwitch() }
        .onChange(of: node.keyProtection) { _, _ in syncICloudSwitch() }
        .task {
            // antstream-visual: a fresh simulator account has no plan or
            // chequebook, so the deposit-0 top-up card can't be reached
            // from real state. Publish representative sample state instead
            // so CI can screenshot that surface — installing it *first*
            // (it latches, so later refreshes leave it in place), then only
            // refreshing the account for the real address the card copies.
            if RootView.shotArgs.contains("-antstream-shot-deposit") {
                node.installDepositTopUpSample()
                await node.refreshAccount()
                return
            }
            await node.refreshAll()
            await node.refreshValidity(rpc: rpc)
            // Unlike the validity read, this one falls back to the public
            // RPC: an unfunded chequebook is the state a user has no way
            // of guessing at, and most installs never type an RPC in.
            await node.refreshSettlementDeposit(rpc: activeRpc)
        }
    }

    // MARK: header

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Storage")
                    .font(.system(.largeTitle, design: .rounded).weight(.bold))
                    .foregroundStyle(.white)
                Text("Your space and account")
                    .font(.footnote)
                    .foregroundStyle(.white.opacity(0.65))
            }
            Spacer()
        }
        .padding(.horizontal, 6)
    }

    // MARK: storage meter

    private var meterCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 14) {
                Text("STORAGE")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))

                if let plan = node.plan, plan.enabled {
                    HStack(alignment: .lastTextBaseline, spacing: 6) {
                        Text(formatBytes(plan.usedBytes))
                            .font(.system(.title, design: .rounded).weight(.bold))
                            .foregroundStyle(.white)
                        Text("of \(formatBytes(plan.totalBytes)) used")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                    meterBar(fraction: plan.usedFraction, low: plan.isLow)
                    Text("\(formatBytes(plan.freeBytes)) free")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                    if let v = node.validity, v.enabled {
                        // How long the plan stays valid before the postage
                        // batch expires (derived from on-chain balance).
                        Label {
                            Text("Valid for \(v.durationLabel) · until \(v.expiryDateLabel)")
                        } icon: {
                            Image(systemName: "clock")
                        }
                        .font(.caption)
                        .foregroundStyle(v.remainingSeconds == 0 ? .orange : .white.opacity(0.6))
                    }
                    GlassPillButton(title: "Extend storage", icon: "clock.arrow.circlepath") {
                        showExtend = true
                    }
                } else {
                    Text("No storage plan")
                        .font(.system(.title2, design: .rounded).weight(.semibold))
                        .foregroundStyle(.white)
                    Text("Connect a plan to start broadcasting.")
                        .font(.subheadline)
                        .foregroundStyle(.white.opacity(0.7))
                    meterBar(fraction: 0, low: false)
                }
            }
        }
    }

    private func meterBar(fraction: Double, low: Bool) -> some View {
        GeometryReader { geo in
            ZStack(alignment: .leading) {
                Capsule().fill(.white.opacity(0.15))
                Capsule()
                    .fill(low ? Color.orange : Color.green)
                    .frame(width: max(6, geo.size.width * fraction))
            }
        }
        .frame(height: 12)
    }

    // MARK: settlement warning

    /// Shown when a plan is connected but outbound settlement is off:
    /// segments will stamp and look published locally but won't reach the
    /// network until the one-time chequebook setup runs, which needs a
    /// little xDAI for gas. Re-running "Find my storage" retries it.
    private var settlementWarningCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Broadcasts won't reach the network yet",
                      systemImage: "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(.orange)
                Text("Network settlement isn't set up. There's a one-time setup that needs a little xDAI in your account for gas. Add a small amount of xDAI, then run setup again.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                HStack(spacing: 12) {
                    GlassPillButton(title: "Add xDAI", icon: "plus") {
                        if let addr = node.account?.ethAddress {
                            UIPasteboard.general.string = addr
                        }
                        banner.flash("Send a little xDAI to your account (copied)")
                    }
                    GlassPillButton(title: "Run setup", icon: "arrow.clockwise") {
                        showConnect = true
                    }
                }
            }
        }
    }

    // MARK: settlement deposit (top-up)

    /// Shown when the chequebook is deployed but holds no (or too little)
    /// xBZZ. Paying peers is what keeps a broadcast moving: an empty
    /// chequebook signs cheques nobody can cash, so publishing runs clean
    /// for a while and then collapses into timeouts and growing lag.
    /// Plans set up before this app funded the chequebook are all in that
    /// state, hence the one-tap top-up.
    private var settlementDepositCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Broadcasts will stall without a deposit",
                      systemImage: "bolt.badge.clock")
                    .font(.headline)
                    .foregroundStyle(.orange)
                Text("Your account pays the network as it publishes, and the account it pays from is empty. A one-time \(node.settlementDeposit?.targetBzz ?? "0.0010") xBZZ deposit keeps segments flowing — it stays yours until it's spent.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.75))
                if node.settlementDeposit?.sufficientFunds == false,
                   let send = node.settlementDeposit?.xdaiToSendDisplay {
                    Text("Add about \(send) xDAI to your account first.")
                        .font(.caption)
                        .foregroundStyle(.white.opacity(0.6))
                }
                HStack(spacing: 12) {
                    if busy {
                        ProgressView().tint(.white)
                    } else {
                        GlassPillButton(title: "Add deposit", icon: "arrow.up.circle") {
                            topUpDeposit()
                        }
                    }
                    if node.settlementDeposit?.sufficientFunds == false {
                        GlassPillButton(title: "Add xDAI", icon: "plus") {
                            if let addr = node.account?.ethAddress {
                                UIPasteboard.general.string = addr
                            }
                            banner.flash("Send a little xDAI to your account (copied)")
                        }
                    }
                }
            }
        }
    }

    // MARK: connect (no plan)

    private var connectCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("Set up storage")
                    .font(.headline)
                    .foregroundStyle(.white)
                Text("Already have a storage plan on this account? Connect it to start broadcasting.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))
                GlassPillButton(title: "Connect storage", icon: "link") { showConnect = true }
            }
        }
    }

    // MARK: account

    private var accountCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Text("ACCOUNT")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                HStack {
                    Image(systemName: "person.crop.circle.fill")
                        .font(.title)
                        .foregroundStyle(.white.opacity(0.85))
                    VStack(alignment: .leading, spacing: 2) {
                        Text(node.account?.shortAddress ?? "—")
                            .font(.body.weight(.semibold).monospaced())
                            .foregroundStyle(.white)
                        Text("Used to pay for storage")
                            .font(.caption)
                            .foregroundStyle(.white.opacity(0.6))
                    }
                    Spacer()
                    if let addr = node.account?.ethAddress {
                        Button {
                            UIPasteboard.general.string = addr
                            banner.flash("Address copied")
                        } label: {
                            Image(systemName: "doc.on.doc").foregroundStyle(.white.opacity(0.85))
                        }
                    }
                }
            }
        }
    }

    /// Where the account key is kept, and the two things a user can do
    /// about it: back the key up by hand, or switch on iCloud Keychain so
    /// a reinstall recovers the account by itself.
    private var accountKeyCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 12) {
                Label("Account key", systemImage: "key.fill")
                    .font(.headline)
                    .foregroundStyle(.white)

                HStack(spacing: 8) {
                    Image(systemName: node.keyProtection == .secureEnclave
                          ? "lock.shield.fill" : "lock.fill")
                        .foregroundStyle(.green)
                    Text(node.keyProtection?.label ?? "Not stored yet")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(.white)
                }
                Text(protectionExplanation)
                    .font(.caption)
                    .foregroundStyle(.white.opacity(0.65))

                Toggle(isOn: $iCloudOn) {
                    Text("Recover with iCloud Keychain")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.white)
                }
                .tint(.green)
                .onChange(of: iCloudOn) { _, on in
                    // Ignore the echo of `syncICloudSwitch()`.
                    guard on != storedSyncsToICloud else { return }
                    if on { applyICloudBackup(true) } else { confirmICloudOff = true }
                }

                Text("Your account key keeps your broadcasts yours. Back it up somewhere safe — anyone with it controls this account.")
                    .font(.subheadline)
                    .foregroundStyle(.white.opacity(0.7))

                HStack(spacing: 12) {
                    GlassPillButton(title: "Back up key", icon: "square.and.arrow.up") {
                        revealKey()
                    }
                    GlassPillButton(title: "Restore", icon: "arrow.clockwise.icloud") {
                        showRestore = true
                    }
                }
            }
        }
    }

    private var protectionExplanation: String {
        switch node.keyProtection {
        case .secureEnclave:
            return "Encrypted to a key that never leaves this device's Secure Enclave. A reinstall on another device needs your backed-up key."
        case .deviceOnly:
            return "Stored in this device's Keychain. A reinstall on another device needs your backed-up key."
        case .iCloudKeychain:
            return "Synced through iCloud Keychain, so a reinstall or a new device recovers this account automatically."
        case nil:
            return "The account key is created on first launch and never written to the app's files."
        }
    }

    private var storedSyncsToICloud: Bool { node.keyProtection?.syncsToICloud == true }

    /// Put the switch back in step with what is actually stored — after a
    /// cancelled or failed change, and whenever the protection moves.
    private func syncICloudSwitch() {
        if iCloudOn != storedSyncsToICloud { iCloudOn = storedSyncsToICloud }
    }

    /// Re-store the key under the other protection — read through the old
    /// one first, so the account survives either way. `false` also deletes
    /// the synced item, which is why it is only reached from the alert.
    private func applyICloudBackup(_ enabled: Bool) {
        do {
            try node.setICloudBackup(enabled)
            banner.flash(enabled
                         ? "Account key will recover from iCloud Keychain"
                         : "Account key removed from iCloud — this device only")
        } catch {
            banner.flash(error.localizedDescription)
        }
        syncICloudSwitch()
    }

    private var advancedCard: some View {
        GlassCard {
            VStack(alignment: .leading, spacing: 10) {
                Text("CONNECTION")
                    .font(.system(.caption, design: .rounded).weight(.semibold))
                    .tracking(2)
                    .foregroundStyle(.white.opacity(0.6))
                row("Status", node.status.label)
                row("Connections", "\(node.peerCount)")
                row("Gateway", node.gatewayUp ? AntNode.gatewayAddress : "not running")
                if let agent = node.account?.agent { row("Version", agent) }
            }
        }
    }

    private func row(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).font(.subheadline).foregroundStyle(.white.opacity(0.65))
            Spacer()
            Text(value).font(.subheadline.weight(.medium).monospaced())
                .foregroundStyle(.white).lineLimit(1).truncationMode(.middle)
        }
    }

    // MARK: sheets

    private var connectSheet: some View {
        NavigationStack {
            Form {
                Section("Gnosis RPC URL") {
                    TextField(AntNode.defaultRpc, text: $rpc)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                }
                Section {
                    Button {
                        Task { await discover() }
                    } label: {
                        if busy { ProgressView() } else { Text("Find my storage automatically") }
                    }
                    .disabled(busy)
                } footer: {
                    Text("Searches the chain for any storage plan this account owns, and finishes the one-time settlement setup. May take a moment.")
                }
            }
            .navigationTitle("Connect storage")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    // Dismissing mid-search doesn't stop the search — the
                    // node keeps scanning the chain, and a Restore tapped
                    // meanwhile has to wait for it before the node can be
                    // torn down. Keep the sheet (and its spinner) up so
                    // that wait is visible rather than mysterious.
                    Button("Cancel") { showConnect = false }.disabled(busy)
                }
            }
        }
        .interactiveDismissDisabled(busy)
        .presentationDetents([.medium])
    }

    private var keySheet: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 16) {
                Label("Keep this secret", systemImage: "exclamationmark.triangle.fill")
                    .font(.headline)
                    .foregroundStyle(.orange)
                Text("This is your account key. Store it in a password manager. Never share it.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text(exportedKey ?? "—")
                    .font(.footnote.monospaced())
                    .textSelection(.enabled)
                    .padding()
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(.quaternary, in: .rect(cornerRadius: 12))
                Button {
                    if let k = exportedKey {
                        // Expiring, device-local copy: a private key must
                        // not linger on the clipboard or ride Universal
                        // Clipboard to the user's other devices.
                        copySecretToPasteboard(k)
                        banner.flash("Account key copied — clipboard clears in a minute")
                    }
                } label: {
                    Label("Copy key", systemImage: "doc.on.doc")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                Spacer()
            }
            .padding()
            .navigationTitle("Account key")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { showKey = false }
                }
            }
        }
        .presentationDetents([.medium])
    }

    // MARK: helpers

    /// The RPC to actually use: the field's placeholder shows the default,
    /// so an empty field means "use the default" — same fallback as every
    /// other flow (Get Started, Extend). Matters most on the
    /// restore-onto-a-new-device path, which reaches this sheet before
    /// Get Started ever filled the field in.
    private var activeRpc: String {
        let r = rpc.trimmingCharacters(in: .whitespaces)
        return r.isEmpty ? AntNode.defaultRpc : r
    }

    /// Put the settlement deposit behind the chequebook. Spends real
    /// funds, so it only ever runs from the card's explicit tap.
    private func topUpDeposit() {
        Task {
            busy = true
            defer { busy = false }
            do {
                try await node.topUpSettlementDeposit(rpc: activeRpc)
                banner.flash("Deposit added — broadcasts are paid for")
            } catch {
                banner.flash(error.localizedDescription)
            }
        }
    }

    private func discover() async {
        busy = true
        defer { busy = false }
        do {
            try await node.discoverStorage(rpc: activeRpc)
            showConnect = false
            if node.plan?.enabled == true {
                banner.flash("Storage connected")
            } else {
                banner.flash("No storage plan found for this account")
            }
        } catch {
            banner.flash(error.localizedDescription)
        }
    }

    private func revealKey() {
        Task {
            do {
                exportedKey = try await node.exportKey()
                showKey = true
            } catch {
                banner.flash(error.localizedDescription)
            }
        }
    }
}

/// "Restore" — the clean recovery path when the Keychain copy is gone
/// (reinstall without iCloud Keychain, or a new device): paste the account
/// key from the backup, store it under the same protection, and restart
/// the node on the restored account. The key is validated by
/// `ant_identity_from_key` *before* anything is written, so a typo can't
/// destroy a working account.
///
/// "Same protection" cuts the other way when the stored key syncs: the
/// restore then *overwrites the synced item*, which replaces the account
/// on every device in the iCloud circle and destroys the current key's
/// only synced copy. That configuration gets the same treatment as the
/// sync-off toggle — an explicit alert, never a bare tap — and the
/// footer stops claiming the change is device-local.
private struct RestoreAccountSheet: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss

    /// Called on success with a banner message for the Storage tab.
    let onDone: (String) -> Void

    @State private var key = ""
    @State private var error: String?
    @State private var busy = false
    /// Whether the *stored* key syncs through iCloud Keychain — read
    /// straight from the keystore (on appear, and again on tap), because
    /// the blast radius of the overwrite depends on what is stored now.
    @State private var storedKeySyncs = false
    @State private var confirmSyncedReplace = false

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("64 hex characters", text: $key, axis: .vertical)
                        .font(.footnote.monospaced())
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .lineLimit(2...4)
                } header: {
                    Text("Account key")
                } footer: {
                    Text(storedKeySyncs
                         ? "Paste the key you backed up. The current account is replaced on every device that uses iCloud Keychain, and its key's synced copy is overwritten — back it up first if you still need it."
                         : "Paste the key you backed up. This device's current account is replaced — back it up first if you still need it.")
                }

                if let error {
                    Section { Text(error).foregroundStyle(.orange).font(.footnote) }
                }

                Section {
                    Button {
                        // Re-read at the moment of truth, not just on
                        // appear: this decision is what stands between a
                        // tap and a circle-wide overwrite.
                        storedKeySyncs = AccountKeystore.currentProtection() == .iCloudKeychain
                        if storedKeySyncs {
                            confirmSyncedReplace = true
                        } else {
                            Task { await restore() }
                        }
                    } label: {
                        if busy { ProgressView() } else { Text("Restore account") }
                    }
                    .disabled(busy || key.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
            }
            .navigationTitle("Restore account")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }.disabled(busy)
                }
            }
        }
        // Mirrors the sync-off toggle's alert: an iCloud Keychain
        // overwrite propagates to the whole circle, so it is confirmed
        // with its real blast radius spelled out, never done on a tap.
        .alert("Replace the account on all your devices?", isPresented: $confirmSyncedReplace) {
            Button("Restore on all devices", role: .destructive) { Task { await restore() } }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("Your current account key syncs through iCloud Keychain, so this replaces the account on all your devices that use it — not just this one — and permanently overwrites the current key's synced copy. Back up the current key first if you might still need this account.")
        }
        .onAppear { storedKeySyncs = AccountKeystore.currentProtection() == .iCloudKeychain }
        .interactiveDismissDisabled(busy)
        .presentationDetents([.medium])
    }

    private func restore() async {
        busy = true
        error = nil
        defer { busy = false }
        do {
            try await node.restoreAccount(fromKey: key)
            dismiss()
            onDone("Account restored")
        } catch {
            self.error = error.localizedDescription
        }
    }
}

/// The "Extend storage" flow: pick how much longer the connected plan
/// should last, see the all-in xDAI price for topping the batch up, pay
/// (or use existing funds), and extend on-chain. Mirrors the Get Started
/// payment flow — same quote shape, same waiting-for-payment poll and
/// auto-execute when funds land.
private struct ExtendStorageSheet: View {
    @EnvironmentObject var node: AntNode
    @Environment(\.dismiss) private var dismiss
    @AppStorage("gnosisRpc") private var rpc = ""

    /// Called on success with a banner message for the Storage tab.
    let onDone: (String) -> Void

    /// Durations the user can add to the plan.
    private static let options: [(id: String, label: String, days: UInt64)] = [
        ("m1", "1 month", 30),
        ("m3", "3 months", 90),
        ("m6", "6 months", 180),
        ("y1", "1 year", 365),
    ]

    private enum Step { case pick, payment, extending }

    @State private var step: Step = .pick
    @State private var selectedDays: UInt64?
    @State private var quote: StorageQuote?
    @State private var quotes: [UInt64: StorageQuote] = [:]
    @State private var loadingQuotes = false
    @State private var error: String?
    /// One-shot guard so funds-detected auto-extension fires only once
    /// per payment session (a failure must not re-trigger a loop).
    @State private var didAutoExtend = false

    var body: some View {
        NavigationStack {
            ZStack {
                LiquidGlassBackground(palette: .network).ignoresSafeArea()
                content
            }
            .navigationTitle("Extend storage")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if step == .payment {
                        Button("Back") {
                            withAnimation { step = .pick; quote = nil; didAutoExtend = false }
                        }
                    } else if step == .pick {
                        Button("Close") { dismiss() }
                    }
                }
            }
        }
        .preferredColorScheme(.dark)
        .interactiveDismissDisabled(step == .extending)
        .task { await loadQuotes() }
    }

    @ViewBuilder private var content: some View {
        switch step {
        case .pick: pickStep
        case .payment: paymentStep
        case .extending: extendingStep
        }
    }

    // MARK: Step 1 — duration picker

    private var pickStep: some View {
        ScrollView {
            VStack(spacing: 18) {
                VStack(spacing: 8) {
                    Image(systemName: "clock.arrow.circlepath")
                        .font(.system(size: 44))
                        .foregroundStyle(.white)
                    Text("Keep your broadcasts longer")
                        .font(.system(.title2, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                    if let v = node.validity, v.enabled {
                        Text("Your storage is valid until \(v.expiryDateLabel). Extending adds time on top.")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                            .multilineTextAlignment(.center)
                    }
                }
                .padding(.top, 12)

                ForEach(Self.options, id: \.id) { option in
                    optionCard(label: option.label, days: option.days)
                }

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(20)
        }
    }

    private func optionCard(label: String, days: UInt64) -> some View {
        Button {
            choose(days)
        } label: {
            GlassCard {
                HStack(spacing: 14) {
                    Text("+\(label)")
                        .font(.title3.weight(.bold))
                        .foregroundStyle(.white)
                    Spacer()
                    if let q = quotes[days] {
                        let price = PriceOracle.roundedUpXdai(q.xdaiToSendDisplay)
                        Text(PriceOracle.usdFromXdai(price) ?? "$\(price)")
                            .font(.headline.weight(.bold))
                            .foregroundStyle(.white)
                    } else if loadingQuotes {
                        ProgressView().tint(.white)
                    } else {
                        Image(systemName: "chevron.right").foregroundStyle(.white.opacity(0.6))
                    }
                }
            }
            .contentShape(.rect(cornerRadius: 28))
        }
        .buttonStyle(.plain)
    }

    // MARK: Step 2 — payment

    @ViewBuilder private var paymentStep: some View {
        if let quote {
            VStack(spacing: 22) {
                Spacer(minLength: 0)

                if quote.sufficientFunds {
                    Label("Payment received", systemImage: "checkmark.circle.fill")
                        .font(.title3.weight(.semibold))
                        .foregroundStyle(.green)
                } else {
                    sendCard(quote)
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small).tint(.white)
                        Text("Waiting for payment…")
                            .font(.subheadline)
                            .foregroundStyle(.white.opacity(0.7))
                    }
                }

                Spacer(minLength: 0)

                Button { extend() } label: {
                    Text("Extend")
                        .font(.headline)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .glassEffect(.regular.tint(.blue.opacity(quote.sufficientFunds ? 0.6 : 0.2)),
                                     in: .capsule)
                        .foregroundStyle(.white.opacity(quote.sufficientFunds ? 1 : 0.5))
                }
                .buttonStyle(.plain)
                .disabled(!quote.sufficientFunds)

                if let error {
                    Text(error)
                        .font(.footnote)
                        .foregroundStyle(.orange)
                        .multilineTextAlignment(.center)
                }
            }
            .padding(24)
            .task { await pollForPayment() }
        }
    }

    private func sendCard(_ quote: StorageQuote) -> some View {
        let send = PriceOracle.roundedUpXdai(quote.xdaiToSendDisplay)
        return GlassCard {
            VStack(spacing: 16) {
                VStack(spacing: 2) {
                    Text("Send")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.white.opacity(0.6))
                    Text("\(send) xDAI")
                        .font(.system(.largeTitle, design: .rounded).weight(.bold))
                        .foregroundStyle(.white)
                }
                if let addr = node.account?.ethAddress {
                    AddressQRCode(address: addr, xdaiAmount: send)
                        .frame(maxWidth: .infinity)
                    Button {
                        UIPasteboard.general.string = addr
                    } label: {
                        HStack {
                            Text(addr)
                                .font(.caption.monospaced())
                                .foregroundStyle(.white)
                                .lineLimit(1).truncationMode(.middle)
                            Spacer()
                            Image(systemName: "doc.on.doc")
                                .foregroundStyle(.white.opacity(0.85))
                        }
                        .padding(12)
                        .background(.white.opacity(0.06), in: .rect(cornerRadius: 12))
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    // MARK: Step 3 — extending

    private var extendingStep: some View {
        VStack(spacing: 18) {
            ProgressView().controlSize(.large).tint(.white)
            Text("Extending your storage…")
                .font(.title3.weight(.semibold))
                .foregroundStyle(.white)
            Text("Confirming your payment on the network. This can take up to a minute — please keep the app open.")
                .font(.subheadline)
                .foregroundStyle(.white.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
    }

    // MARK: actions

    private var activeRpc: String {
        let r = rpc.trimmingCharacters(in: .whitespaces)
        return r.isEmpty ? AntNode.defaultRpc : r
    }

    /// Fetch a quote for every duration so the picker shows each option's
    /// all-in price up front. Best-effort per option.
    private func loadQuotes() async {
        loadingQuotes = true
        defer { loadingQuotes = false }
        let rpcURL = activeRpc
        let stream = node
        await withTaskGroup(of: (UInt64, StorageQuote?).self) { group in
            for option in Self.options {
                group.addTask {
                    let q = try? await stream.quoteTopUp(rpc: rpcURL, days: option.days)
                    return (option.days, q)
                }
            }
            for await (days, q) in group {
                if let q { quotes[days] = q }
            }
        }
        if quotes.isEmpty {
            error = "Couldn't reach the network to price extensions. Try again."
        }
    }

    private func choose(_ days: UInt64) {
        selectedDays = days
        error = nil
        didAutoExtend = false
        if let cached = quotes[days] {
            quote = cached
            withAnimation { step = .payment }
        } else {
            Task {
                do {
                    let q = try await node.quoteTopUp(rpc: activeRpc, days: days)
                    quote = q
                    quotes[days] = q
                    withAnimation { step = .payment }
                } catch {
                    self.error = error.localizedDescription
                }
            }
        }
    }

    /// While the payment step is on screen and unfunded, re-quote every
    /// few seconds; when the incoming transfer lands, extend
    /// automatically — no tap needed.
    private func pollForPayment() async {
        guard let days = selectedDays else { return }
        while !Task.isCancelled {
            if quote?.sufficientFunds == true { autoExtendIfFunded(); return }
            try? await Task.sleep(nanoseconds: 6_000_000_000)
            if Task.isCancelled { return }
            guard let fresh = try? await node.quoteTopUp(rpc: activeRpc, days: days)
            else { continue }
            withAnimation {
                quote = fresh
                quotes[days] = fresh
            }
            if fresh.sufficientFunds { autoExtendIfFunded(); return }
        }
    }

    private func autoExtendIfFunded() {
        guard !didAutoExtend else { return }
        didAutoExtend = true
        extend()
    }

    private func extend() {
        guard let quote else { return }
        error = nil
        withAnimation { step = .extending }
        Task {
            do {
                try await node.topUpStorage(rpc: activeRpc, amountPerChunk: quote.amountPerChunk)
                let until = node.validity.map { " — valid until \($0.expiryDateLabel)" } ?? ""
                dismiss()
                onDone("Storage extended\(until)")
            } catch {
                self.error = error.localizedDescription
                withAnimation { step = .payment }
            }
        }
    }
}
