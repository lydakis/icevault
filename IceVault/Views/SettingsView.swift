import AppKit
import SwiftUI

struct SettingsView: View {
    @EnvironmentObject private var appState: AppState

    @State private var draft = AppState.Settings()
    @State private var accessKey = ""
    @State private var secretKey = ""
    @State private var detectedCredentialSource: CredentialSource?
    @State private var detectedCredentialExpiry: Date?
    @State private var ssoTokenStatus: SSOTokenStatus?
    @State private var scheduleInstalled = false
    @State private var savedAt: Date?
    @State private var saveMessage: String?
    @State private var saveSucceeded = false
    @State private var connectionMessage: String?
    @State private var connectionSucceeded = false
    @State private var isTestingConnection = false
    @State private var isRunningSSOLogin = false
    @State private var ssoLoginMessage: String?
    @State private var ssoLoginSucceeded = false

    private static let commonRegions: [String] = [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ca-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-central-1",
        "eu-north-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "ap-south-1",
        "sa-east-1"
    ]

    private static let cardCornerRadius: CGFloat = 16

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                settingsHeaderCard

                settingsCard(title: "AWS Authentication", systemImage: "lock.shield") {
                    Picker("Authentication Method", selection: $draft.authenticationMethod) {
                        ForEach(AppState.Settings.AuthenticationMethod.allCases) { method in
                            Text(method.displayName).tag(method)
                        }
                    }
                    .pickerStyle(.segmented)

                    if draft.authenticationMethod == .ssoProfile {
                        TextField("SSO Profile Name", text: $draft.ssoProfileName)

                        HStack(spacing: 10) {
                            Button("Login") {
                                loginToSSOProfile()
                            }
                            .buttonStyle(.borderedProminent)
                            .disabled(isRunningSSOLogin || trimmed(draft.ssoProfileName).isEmpty)

                            if isRunningSSOLogin {
                                ProgressView()
                                    .controlSize(.small)
                            }

                            Spacer()
                        }

                        Text(ssoStatusDescription)
                            .font(.caption)
                            .foregroundStyle(ssoStatusColor)

                        if let ssoLoginMessage {
                            Text(ssoLoginMessage)
                                .font(.caption)
                                .foregroundStyle(ssoLoginSucceeded ? .green : .red)
                        }

                        Text("Run `aws configure sso --profile <name>` once before first login.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        TextField("Access Key ID", text: $accessKey)
                            .textContentType(.username)

                        SecureField("Secret Access Key", text: $secretKey)
                            .textContentType(.password)

                        Text("Saved credentials are stored in macOS Keychain.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    if let detectedCredentialSource {
                        Text(detectedCredentialSource.settingsDescription)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("No credentials detected.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    if let detectedCredentialExpiry {
                        Text("Detected credentials expire at \(detectedCredentialExpiry.formatted(date: .abbreviated, time: .shortened)).")
                            .font(.caption)
                            .foregroundStyle(detectedCredentialExpiry > Date() ? Color.secondary : Color.red)
                    }
                }

                settingsCard(title: "Storage Target", systemImage: "shippingbox") {
                    TextField("Bucket Name", text: $draft.bucket)

                    Picker("Region", selection: $draft.awsRegion) {
                        ForEach(regionOptions, id: \.self) { region in
                            Text(region).tag(region)
                        }
                    }
                    .pickerStyle(.menu)
                }

                settingsCard(title: "Source & Filters", systemImage: "folder.badge.gearshape") {
                    HStack(alignment: .top, spacing: 10) {
                        Text(draft.sourcePath.isEmpty ? "No folder selected" : draft.sourcePath)
                            .font(.subheadline)
                            .foregroundStyle(draft.sourcePath.isEmpty ? .secondary : .primary)
                            .lineLimit(2)
                            .textSelection(.enabled)

                        Spacer()

                        Button("Choose Folder…") {
                            chooseSourceFolder()
                        }
                    }

                    Toggle("Include hidden files and folders (dotfiles)", isOn: $draft.includeHiddenFiles)
                    Text(draft.includeHiddenFiles
                        ? "Hidden dotfiles are included in backup scans."
                        : "Hidden dotfiles are ignored by default to avoid backing up local app metadata.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                settingsCard(title: "Transfer Performance", systemImage: "speedometer") {
                    Stepper(
                        value: $draft.maxConcurrentFileUploads,
                        in: AppState.Settings.minimumUploadConcurrency...AppState.Settings.maximumConcurrentFileUploads
                    ) {
                        Text("Concurrent File Uploads: \(draft.maxConcurrentFileUploads)")
                    }

                    Stepper(
                        value: $draft.maxConcurrentMultipartPartUploads,
                        in: AppState.Settings.minimumUploadConcurrency...AppState.Settings.maximumConcurrentMultipartPartUploads
                    ) {
                        Text("Multipart Parts Per File: \(draft.maxConcurrentMultipartPartUploads)")
                    }

                    Toggle("Auto-Tune Scan Buffer", isOn: autoTuneScanBufferBinding)
                        .help("Automatically sizes the scan-to-upload pending file buffer based on upload concurrency.")

                    if draft.maxBufferedPendingPlans != nil {
                        Stepper(
                            value: maxBufferedPendingPlansBinding,
                            in: AppState.Settings.minimumBufferedPendingPlans...AppState.Settings.maximumBufferedPendingPlans,
                            step: AppState.Settings.minimumBufferedPendingPlans
                        ) {
                            Text("Max Buffered Pending Files: \(draft.maxBufferedPendingPlans ?? AppState.Settings.defaultManualMaxBufferedPendingPlans)")
                        }
                        .help("Caps how many pending files can be queued between scanning and uploading to bound memory use.")
                    }

                    Text("Higher values can increase throughput but may saturate network bandwidth or trigger S3 throttling.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                settingsCard(title: "Scheduling", systemImage: "clock.arrow.circlepath") {
                    Toggle("Scheduled Backups", isOn: $draft.scheduledBackupsEnabled)

                    if draft.scheduledBackupsEnabled {
                        Picker("Interval", selection: $draft.scheduleInterval) {
                            ForEach(AppState.Settings.ScheduleInterval.allCases) { interval in
                                Text(interval.displayName).tag(interval)
                            }
                        }
                        .pickerStyle(.menu)

                        if draft.scheduleInterval == .customHours {
                            Stepper(value: $draft.customIntervalHours, in: 1...168) {
                                Text("Every \(draft.customIntervalHours) hour\(draft.customIntervalHours == 1 ? "" : "s")")
                            }
                        }

                        Text("LaunchAgent: ~/Library/LaunchAgents/com.icevault.backup.plist")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }

                    Text(scheduleInstalled ? "LaunchAgent is installed." : "LaunchAgent is not installed.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                settingsCard(title: "Actions", systemImage: "checkmark.seal") {
                    HStack(spacing: 10) {
                        Button("Test Connection") {
                            testConnection()
                        }
                        .buttonStyle(.bordered)
                        .disabled(isTestingConnection)

                        if isTestingConnection {
                            ProgressView()
                                .controlSize(.small)
                        }

                        Spacer()

                        Button("Save") {
                            saveSettings()
                        }
                        .buttonStyle(.borderedProminent)
                    }

                    if let connectionMessage {
                        Text(connectionMessage)
                            .font(.caption)
                            .foregroundStyle(connectionSucceeded ? .green : .red)
                    }

                    if let saveMessage {
                        Text(saveMessage)
                            .font(.caption)
                            .foregroundStyle(saveSucceeded ? .green : .red)
                    }

                    if let savedAt {
                        Text("Last saved \(savedAt.formatted(date: .abbreviated, time: .shortened))")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(
            LinearGradient(
                colors: [Color(nsColor: .windowBackgroundColor), Color.accentColor.opacity(0.08)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
        )
        .frame(minWidth: 640, minHeight: 680)
        .animation(.easeInOut(duration: 0.18), value: draft.authenticationMethod)
        .onAppear {
            loadDraft()
        }
        .onChange(of: draft.authenticationMethod) { _, _ in
            ssoLoginMessage = nil
            applyDetectedCredentials(prefillFields: false)
        }
        .onChange(of: draft.ssoProfileName) { _, _ in
            ssoLoginMessage = nil
            applyDetectedCredentials(prefillFields: false)
        }
        .onChange(of: draft.awsRegion) { _, _ in
            applyDetectedCredentials(prefillFields: false)
        }
    }

    private var settingsHeaderCard: some View {
        HStack(alignment: .top) {
            HStack(spacing: 12) {
                Image(systemName: "slider.horizontal.3")
                    .font(.title3.weight(.semibold))
                    .foregroundStyle(Color.accentColor)
                    .frame(width: 36, height: 36)
                    .background(Color.accentColor.opacity(0.14), in: RoundedRectangle(cornerRadius: 10, style: .continuous))

                VStack(alignment: .leading, spacing: 4) {
                    Text("IceVault Settings")
                        .font(.title2.weight(.semibold))
                    Text("Configure backup behavior, credentials, and schedule.")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 4) {
                metadataPill(
                    text: scheduleInstalled ? "Scheduler Installed" : "Scheduler Not Installed",
                    color: scheduleInstalled ? .green : .secondary
                )
                if let savedAt {
                    metadataPill(
                        text: "Saved \(savedAt.formatted(date: .omitted, time: .shortened))",
                        color: .secondary
                    )
                }
            }
        }
        .padding(18)
        .background(cardSurface, in: RoundedRectangle(cornerRadius: Self.cardCornerRadius, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: Self.cardCornerRadius, style: .continuous)
                .stroke(Color(nsColor: .separatorColor).opacity(0.28), lineWidth: 0.8)
        }
    }

    @ViewBuilder
    private func settingsCard<Content: View>(
        title: String,
        systemImage: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack(spacing: 10) {
                Image(systemName: systemImage)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(Color.accentColor)
                    .frame(width: 24)
                Text(title)
                    .font(.headline)
            }
            content()
        }
        .padding(18)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(cardSurface, in: RoundedRectangle(cornerRadius: Self.cardCornerRadius, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: Self.cardCornerRadius, style: .continuous)
                .stroke(Color(nsColor: .separatorColor).opacity(0.24), lineWidth: 0.8)
        }
    }

    private var cardSurface: Color {
        Color(nsColor: .controlBackgroundColor).opacity(0.96)
    }

    @ViewBuilder
    private func metadataPill(text: String, color: Color) -> some View {
        Text(text)
            .font(.caption.weight(.medium))
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .foregroundStyle(color)
            .background(color.opacity(0.12), in: Capsule())
    }

    private var regionOptions: [String] {
        if Self.commonRegions.contains(draft.awsRegion) {
            return Self.commonRegions
        }
        return [draft.awsRegion].filter { !$0.isEmpty } + Self.commonRegions
    }

    private var ssoStatusDescription: String {
        guard draft.authenticationMethod == .ssoProfile else {
            return ""
        }

        let profileName = trimmed(draft.ssoProfileName)
        if profileName.isEmpty {
            return "Enter an SSO profile name from ~/.aws/config."
        }

        switch ssoTokenStatus {
        case .missingProfile, .none:
            return "Profile '\(profileName)' is missing required SSO fields in ~/.aws/config."
        case .missingToken:
            return "No cached SSO login found for '\(profileName)'."
        case .expired(let expiresAt):
            return "SSO token expired at \(expiresAt.formatted(date: .abbreviated, time: .shortened))."
        case .valid(let expiresAt):
            return "SSO token is valid until \(expiresAt.formatted(date: .abbreviated, time: .shortened))."
        }
    }

    private var ssoStatusColor: Color {
        guard draft.authenticationMethod == .ssoProfile else {
            return .secondary
        }

        switch ssoTokenStatus {
        case .valid:
            return .green
        case .expired, .missingProfile, .missingToken, .none:
            return .red
        }
    }

    private var autoTuneScanBufferBinding: Binding<Bool> {
        Binding(
            get: {
                draft.maxBufferedPendingPlans == nil
            },
            set: { isAuto in
                if isAuto {
                    draft.maxBufferedPendingPlans = nil
                } else {
                    draft.maxBufferedPendingPlans = draft.maxBufferedPendingPlans
                        ?? AppState.Settings.defaultManualMaxBufferedPendingPlans
                }
            }
        )
    }

    private var maxBufferedPendingPlansBinding: Binding<Int> {
        Binding(
            get: {
                draft.maxBufferedPendingPlans ?? AppState.Settings.defaultManualMaxBufferedPendingPlans
            },
            set: { newValue in
                draft.maxBufferedPendingPlans = AppState.Settings.clampBufferedPendingPlans(newValue)
            }
        )
    }

    private func loadDraft() {
        draft = appState.settings
        scheduleInstalled = appState.scheduledBackupsInstalled()
        applyDetectedCredentials(prefillFields: true)
    }

    private func saveSettings() {
        do {
            let normalizedAccessKey = trimmed(accessKey)
            let normalizedSecretKey = trimmed(secretKey)

            if draft.authenticationMethod == .staticKeys {
                if normalizedAccessKey.isEmpty && normalizedSecretKey.isEmpty {
                    try appState.deleteStoredCredentials()
                } else {
                    try appState.saveCredentials(
                        accessKey: normalizedAccessKey,
                        secretKey: normalizedSecretKey
                    )
                }
            }

            var normalizedSettings = draft
            normalizedSettings.bucket = trimmed(draft.bucket)
            normalizedSettings.awsRegion = trimmed(draft.awsRegion)
            normalizedSettings.ssoProfileName = trimmed(draft.ssoProfileName)
            normalizedSettings.sourcePath = trimmed(draft.sourcePath)
            normalizedSettings.customIntervalHours = min(max(draft.customIntervalHours, 1), 168)
            normalizedSettings.includeHiddenFiles = draft.includeHiddenFiles
            normalizedSettings.maxConcurrentFileUploads = AppState.Settings.clampFileUploadConcurrency(draft.maxConcurrentFileUploads)
            normalizedSettings.maxConcurrentMultipartPartUploads = AppState.Settings.clampMultipartPartConcurrency(draft.maxConcurrentMultipartPartUploads)
            normalizedSettings.maxBufferedPendingPlans = AppState.Settings.clampBufferedPendingPlans(draft.maxBufferedPendingPlans)
            appState.updateSettings(normalizedSettings)
            _ = try appState.applyScheduledBackups()
            scheduleInstalled = appState.scheduledBackupsInstalled()

            savedAt = Date()
            saveMessage = "Settings saved."
            saveSucceeded = true
            applyDetectedCredentials(prefillFields: false)
        } catch {
            saveMessage = error.localizedDescription
            saveSucceeded = false
        }
    }

    private func testConnection() {
        isTestingConnection = true
        connectionMessage = nil

        let region = trimmed(draft.awsRegion)
        let bucket = trimmed(draft.bucket)
        let normalizedAccessKey = trimmed(accessKey)
        let normalizedSecretKey = trimmed(secretKey)

        Task { @MainActor in
            do {
                let credentials: AWSCredentials
                let resolvedRegion: String

                if draft.authenticationMethod == .staticKeys,
                   (!normalizedAccessKey.isEmpty || !normalizedSecretKey.isEmpty)
                {
                    guard !normalizedAccessKey.isEmpty, !normalizedSecretKey.isEmpty else {
                        throw KeychainServiceError.incompleteCredentials
                    }

                    credentials = AWSCredentials(
                        accessKey: normalizedAccessKey,
                        secretKey: normalizedSecretKey
                    )
                    resolvedRegion = region
                } else {
                    guard let resolvedCredentials = resolveCredentialsForDraft(preferredRegion: region) else {
                        throw GlacierClientError.invalidCredentials
                    }
                    credentials = resolvedCredentials.credentials
                    resolvedRegion = trimmed(resolvedCredentials.region ?? region)
                }

                try await appState.testConnection(
                    accessKey: credentials.accessKey,
                    secretKey: credentials.secretKey,
                    sessionToken: credentials.sessionToken,
                    region: resolvedRegion,
                    bucket: bucket
                )

                connectionMessage = "Connection succeeded. Bucket is reachable."
                connectionSucceeded = true
            } catch {
                connectionMessage = "Connection failed: \(error.localizedDescription)"
                connectionSucceeded = false
            }

            isTestingConnection = false
        }
    }

    private func loginToSSOProfile() {
        let profileName = trimmed(draft.ssoProfileName)
        guard !profileName.isEmpty else {
            ssoLoginMessage = "Enter an SSO profile name first."
            ssoLoginSucceeded = false
            return
        }

        isRunningSSOLogin = true
        ssoLoginMessage = nil

        Task { @MainActor in
            let (exitCode, output) = await runSSOLogin(profileName: profileName)
            isRunningSSOLogin = false

            if exitCode == 0 {
                ssoLoginMessage = "SSO login succeeded for profile '\(profileName)'."
                ssoLoginSucceeded = true
                appState.dismissAuthenticationPrompt()
                applyDetectedCredentials(prefillFields: false)
                return
            }

            let outputSuffix = output.isEmpty ? "" : " \(output)"
            ssoLoginMessage = "SSO login failed for profile '\(profileName)'.\(outputSuffix)"
            ssoLoginSucceeded = false
            applyDetectedCredentials(prefillFields: false)
        }
    }

    private func runSSOLogin(profileName: String) async -> (Int32, String) {
        await Task.detached(priority: .userInitiated) {
            let process = Process()
            let environment = AWSCLI.makeEnvironment(
                base: ProcessInfo.processInfo.environment,
                profileName: profileName
            )
            guard let awsExecutableURL = AWSCLI.executableURL(environment: environment) else {
                return (-1, "AWS CLI not found. Install it with `brew install awscli`.")
            }

            process.executableURL = awsExecutableURL
            process.arguments = ["sso", "login", "--profile", profileName]
            process.environment = environment

            let outputPipe = Pipe()
            process.standardOutput = outputPipe
            process.standardError = outputPipe

            do {
                try process.run()
            } catch {
                return (-1, error.localizedDescription)
            }

            process.waitUntilExit()

            let outputData = outputPipe.fileHandleForReading.readDataToEndOfFile()
            let output = String(data: outputData, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

            let clippedOutput: String
            if output.count > 240 {
                let endIndex = output.index(output.startIndex, offsetBy: 240)
                clippedOutput = String(output[..<endIndex]) + "..."
            } else {
                clippedOutput = output
            }

            return (process.terminationStatus, clippedOutput)
        }.value
    }

    private func resolveCredentialsForDraft(preferredRegion: String?) -> ResolvedCredentials? {
        let keychainCredentials: AWSCredentials?
        do {
            keychainCredentials = try appState.loadStoredCredentials()
        } catch {
            keychainCredentials = nil
        }

        return GlacierClient.resolveCredentials(
            keychainCredentials: keychainCredentials,
            authMethod: draft.authenticationMethod,
            ssoProfileName: draft.ssoProfileName,
            preferredRegion: preferredRegion
        )
    }

    private func chooseSourceFolder() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.canCreateDirectories = false
        panel.allowsMultipleSelection = false
        panel.prompt = "Select"

        if panel.runModal() == .OK, let selectedURL = panel.url {
            draft.sourcePath = selectedURL.path
        }
    }

    private func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func applyDetectedCredentials(prefillFields: Bool) {
        let resolved = resolveCredentialsForDraft(preferredRegion: draft.awsRegion)
        detectedCredentialSource = resolved?.credentialSource
        detectedCredentialExpiry = resolved?.expiration

        if draft.authenticationMethod == .ssoProfile {
            let profileName = trimmed(draft.ssoProfileName)
            if profileName.isEmpty {
                ssoTokenStatus = .missingProfile
            } else {
                ssoTokenStatus = GlacierClient.ssoTokenStatus(profileName: profileName)
            }
        } else {
            ssoTokenStatus = nil
        }

        guard prefillFields else {
            return
        }

        guard let resolved else {
            accessKey = ""
            secretKey = ""
            return
        }

        accessKey = resolved.credentials.accessKey
        secretKey = resolved.credentials.secretKey

        if trimmed(draft.awsRegion).isEmpty, let detectedRegion = resolved.region {
            draft.awsRegion = detectedRegion
        }
    }

}
