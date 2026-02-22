import Combine
import Foundation

@MainActor
final class AppState: ObservableObject {
    struct Settings: Codable, Equatable {
        enum AuthenticationMethod: String, Codable, CaseIterable, Identifiable {
            case staticKeys
            case ssoProfile

            var id: String {
                rawValue
            }

            var displayName: String {
                switch self {
                case .staticKeys:
                    return "Static Keys"
                case .ssoProfile:
                    return "SSO Profile"
                }
            }
        }

        enum ScheduleInterval: String, Codable, CaseIterable, Identifiable {
            case daily
            case weekly
            case customHours

            var id: String {
                rawValue
            }

            var displayName: String {
                switch self {
                case .daily:
                    return "Daily"
                case .weekly:
                    return "Weekly"
                case .customHours:
                    return "Custom (Hours)"
                }
            }
        }

        var awsAccessKey: String = ""
        var awsSecretKey: String = ""
        var awsSessionToken: String = ""
        var awsRegion: String = "us-east-1"
        var authenticationMethod: AuthenticationMethod = .staticKeys
        var ssoProfileName: String = ""
        var bucket: String = ""
        var sourcePath: String = ""
        var scheduledBackupsEnabled: Bool = false
        var scheduleInterval: ScheduleInterval = .daily
        var customIntervalHours: Int = 24

        enum CodingKeys: String, CodingKey {
            case awsRegion
            case authenticationMethod
            case ssoProfileName
            case bucket
            case sourcePath
            case scheduledBackupsEnabled
            case scheduleInterval
            case customIntervalHours
        }

        init(
            awsAccessKey: String = "",
            awsSecretKey: String = "",
            awsSessionToken: String = "",
            awsRegion: String = "us-east-1",
            authenticationMethod: AuthenticationMethod = .staticKeys,
            ssoProfileName: String = "",
            bucket: String = "",
            sourcePath: String = "",
            scheduledBackupsEnabled: Bool = false,
            scheduleInterval: ScheduleInterval = .daily,
            customIntervalHours: Int = 24
        ) {
            self.awsAccessKey = awsAccessKey
            self.awsSecretKey = awsSecretKey
            self.awsSessionToken = awsSessionToken
            self.awsRegion = awsRegion
            self.authenticationMethod = authenticationMethod
            self.ssoProfileName = ssoProfileName
            self.bucket = bucket
            self.sourcePath = sourcePath
            self.scheduledBackupsEnabled = scheduledBackupsEnabled
            self.scheduleInterval = scheduleInterval
            self.customIntervalHours = max(1, customIntervalHours)
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            awsAccessKey = ""
            awsSecretKey = ""
            awsSessionToken = ""
            awsRegion = try container.decodeIfPresent(String.self, forKey: .awsRegion) ?? "us-east-1"
            authenticationMethod = try container.decodeIfPresent(AuthenticationMethod.self, forKey: .authenticationMethod) ?? .staticKeys
            ssoProfileName = try container.decodeIfPresent(String.self, forKey: .ssoProfileName) ?? ""
            bucket = try container.decodeIfPresent(String.self, forKey: .bucket) ?? ""
            sourcePath = try container.decodeIfPresent(String.self, forKey: .sourcePath) ?? ""
            scheduledBackupsEnabled = try container.decodeIfPresent(Bool.self, forKey: .scheduledBackupsEnabled) ?? false
            scheduleInterval = try container.decodeIfPresent(ScheduleInterval.self, forKey: .scheduleInterval) ?? .daily
            customIntervalHours = max(1, try container.decodeIfPresent(Int.self, forKey: .customIntervalHours) ?? 24)
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(awsRegion, forKey: .awsRegion)
            try container.encode(authenticationMethod, forKey: .authenticationMethod)
            try container.encode(ssoProfileName, forKey: .ssoProfileName)
            try container.encode(bucket, forKey: .bucket)
            try container.encode(sourcePath, forKey: .sourcePath)
            try container.encode(scheduledBackupsEnabled, forKey: .scheduledBackupsEnabled)
            try container.encode(scheduleInterval, forKey: .scheduleInterval)
            try container.encode(customIntervalHours, forKey: .customIntervalHours)
        }
    }

    @Published var currentJob: BackupJob? {
        didSet {
            bindCurrentJob()
        }
    }

    @Published var settings: Settings {
        didSet {
            saveSettings()
        }
    }

    @Published var history: [BackupHistoryEntry] {
        didSet {
            saveHistory()
        }
    }

    @Published private(set) var hasAvailableCredentials: Bool
    @Published private(set) var credentialSource: CredentialSource?
    @Published var authenticationPromptMessage: String?
    @Published private(set) var ssoSessionStatus: SSOTokenStatus?
    @Published private(set) var ssoSessionExpiresAt: Date?

    private let userDefaults: UserDefaults
    private let backupEngine: BackupEngine
    private let keychainService: KeychainService
    private let launchAgent: LaunchAgent
    let ssoTokenMonitor: SSOTokenMonitor
    private var currentJobObserver: AnyCancellable?
    private var ssoTokenMonitorObserver: AnyCancellable?
    private var backupTask: Task<Void, Never>?

    private static let settingsKey = "IceVault.settings"
    private static let historyKey = "IceVault.history"
    private static let launchAgentLabel = "com.icevault.backup"

    init(
        userDefaults: UserDefaults = .standard,
        backupEngine: BackupEngine = BackupEngine(),
        keychainService: KeychainService = KeychainService(),
        launchAgent: LaunchAgent = LaunchAgent(),
        ssoTokenMonitor: SSOTokenMonitor? = nil
    ) {
        self.userDefaults = userDefaults
        self.backupEngine = backupEngine
        self.keychainService = keychainService
        self.launchAgent = launchAgent
        self.ssoTokenMonitor = ssoTokenMonitor ?? SSOTokenMonitor(userDefaults: userDefaults)
        self.settings = Self.loadSettings(from: userDefaults, key: Self.settingsKey)
        self.history = Self.loadHistory(from: userDefaults, key: Self.historyKey)
        self.hasAvailableCredentials = false
        self.credentialSource = nil
        self.authenticationPromptMessage = nil
        self.ssoSessionStatus = nil
        self.ssoSessionExpiresAt = nil

        bindSSOTokenMonitor()
        configureSSOTokenMonitor()
        refreshCredentialState()
        saveSettings()
    }

    var statusText: String {
        if let currentJob {
            return currentJob.status.displayName
        }

        if !isConfigured {
            return "Not Configured"
        }

        if let last = history.first {
            return "Last: \(last.status.displayName)"
        }

        return "Idle"
    }

    var menuBarSystemImage: String {
        switch currentJob?.status ?? .idle {
        case .idle:
            return "archivebox"
        case .scanning:
            return "magnifyingglass.circle"
        case .uploading:
            return "icloud.and.arrow.up"
        case .completed:
            return "checkmark.circle"
        case .failed:
            return "exclamationmark.triangle"
        }
    }

    var lastBackupDate: Date? {
        history.first?.displayDate
    }

    var isConfigured: Bool {
        hasAvailableCredentials
            && !Self.trimmed(settings.awsRegion).isEmpty
            && !Self.trimmed(settings.bucket).isEmpty
            && !Self.trimmed(settings.sourcePath).isEmpty
    }

    var usesSSOAuthentication: Bool {
        settings.authenticationMethod == .ssoProfile
    }

    var isSSOSessionExpired: Bool {
        if case .expired = ssoSessionStatus {
            return true
        }
        return false
    }

    var isBackupBlockedBySSOExpiry: Bool {
        usesSSOAuthentication && !ssoTokenMonitor.isSessionValid()
    }

    var ssoSessionStatusText: String? {
        guard usesSSOAuthentication else {
            return nil
        }

        switch ssoSessionStatus {
        case .some(.valid(let expiresAt)):
            return "Session valid (expires \(expiresAt.formatted(date: .abbreviated, time: .omitted)))"
        case .some(.expired):
            return "Session expired ⚠️"
        case .some(.missingToken):
            return "Session missing - refresh login"
        case .some(.missingProfile):
            return "Session unavailable - check SSO profile"
        case .none:
            return "Session status unavailable"
        }
    }

    var backupBlockedReason: String? {
        guard usesSSOAuthentication, isBackupBlockedBySSOExpiry else {
            return nil
        }

        let profileName = Self.trimmed(settings.ssoProfileName)
        let resolvedProfileName = profileName.isEmpty ? "<name>" : profileName

        switch ssoSessionStatus {
        case .some(.missingProfile):
            return "Backup paused: SSO profile '\(resolvedProfileName)' is missing from ~/.aws/config."
        case .some(.missingToken):
            return "Backup paused: run `aws sso login --profile \(resolvedProfileName)`."
        case .some(.expired(let expiresAt)):
            return "Backup paused: SSO session expired at \(expiresAt.formatted(date: .abbreviated, time: .shortened))."
        case .some(.valid), .none:
            return nil
        }
    }

    var canRefreshSSOLogin: Bool {
        usesSSOAuthentication && !Self.trimmed(settings.ssoProfileName).isEmpty
    }

    func updateSettings(_ newSettings: Settings) {
        var sanitized = newSettings
        sanitized.awsAccessKey = ""
        sanitized.awsSecretKey = ""
        sanitized.awsSessionToken = ""
        sanitized.awsRegion = Self.trimmed(sanitized.awsRegion)
        sanitized.ssoProfileName = Self.trimmed(sanitized.ssoProfileName)
        sanitized.bucket = Self.trimmed(sanitized.bucket)
        sanitized.sourcePath = Self.trimmed(sanitized.sourcePath)
        sanitized.customIntervalHours = min(max(sanitized.customIntervalHours, 1), 168)
        settings = sanitized
        configureSSOTokenMonitor()
        refreshCredentialState()
    }

    func loadStoredCredentials() throws -> AWSCredentials? {
        try keychainService.loadCredentials()
    }

    func saveCredentials(accessKey: String, secretKey: String) throws {
        try keychainService.save(accessKey: accessKey, secretKey: secretKey)
        refreshCredentialState()
    }

    func deleteStoredCredentials() throws {
        try keychainService.deleteCredentials()
        refreshCredentialState()
    }

    func clearHistory() {
        history = []
    }

    func resolveCredentials(preferredRegion: String? = nil) -> ResolvedCredentials? {
        let keychainCredentials: AWSCredentials?
        do {
            keychainCredentials = try keychainService.loadCredentials()
        } catch {
            keychainCredentials = nil
        }

        return GlacierClient.resolveCredentials(
            keychainCredentials: keychainCredentials,
            authMethod: settings.authenticationMethod,
            ssoProfileName: settings.ssoProfileName,
            preferredRegion: preferredRegion
        )
    }

    func ssoTokenStatus() -> SSOTokenStatus? {
        guard usesSSOAuthentication else {
            return nil
        }

        ssoTokenMonitor.refreshNow()
        return ssoSessionStatus
    }

    func requestNotificationPermissionIfNeeded() {
        ssoTokenMonitor.requestNotificationPermissionIfNeeded()
    }

    func refreshSSOSessionStatus() {
        guard usesSSOAuthentication else {
            return
        }
        ssoTokenMonitor.refreshNow()
    }

    func refreshSSOLogin() {
        guard canRefreshSSOLogin else {
            return
        }
        authenticationPromptMessage = nil
        ssoTokenMonitor.refreshLogin()
    }

    func dismissAuthenticationPrompt() {
        authenticationPromptMessage = nil
    }

    @discardableResult
    func applyScheduledBackups() throws -> Bool {
        let executablePath = Self.currentExecutablePath()
        guard !executablePath.isEmpty else {
            throw LaunchAgentError.invalidExecutablePath
        }

        if settings.scheduledBackupsEnabled {
            try launchAgent.install(
                label: Self.launchAgentLabel,
                executablePath: executablePath,
                interval: launchAgentInterval(for: settings)
            )
            return true
        }

        try launchAgent.uninstall(label: Self.launchAgentLabel)
        return false
    }

    func scheduledBackupsInstalled() -> Bool {
        launchAgent.isInstalled(label: Self.launchAgentLabel)
    }

    func testConnection(
        accessKey: String,
        secretKey: String,
        sessionToken: String? = nil,
        region: String,
        bucket: String
    ) async throws {
        let client = try GlacierClient(
            accessKey: accessKey,
            secretKey: secretKey,
            sessionToken: sessionToken,
            region: region
        )
        try await client.verifyBucketAccess(bucket: bucket)
    }

    func startBackup() {
        guard currentJob == nil, backupTask == nil else {
            return
        }

        refreshSSOSessionStatus()

        authenticationPromptMessage = nil
        if let ssoPromptMessage = ssoReauthenticationPromptIfNeeded() {
            authenticationPromptMessage = ssoPromptMessage
            return
        }

        let sourceRoot = Self.trimmed(settings.sourcePath)
        let bucket = Self.trimmed(settings.bucket)

        let job = BackupJob(sourceRoot: sourceRoot, bucket: bucket, status: .scanning)
        currentJob = job

        backupTask = Task { [weak self] in
            guard let self else {
                return
            }
            await executeBackup(job: job)
        }
    }

    func cancelBackup() {
        backupTask?.cancel()
    }

    func startManualBackup() async {
        startBackup()
        await backupTask?.value
    }

    private func bindCurrentJob() {
        currentJobObserver = currentJob?.objectWillChange.sink { [weak self] _ in
            self?.objectWillChange.send()
        }
    }

    private func bindSSOTokenMonitor() {
        ssoTokenMonitorObserver = ssoTokenMonitor.$status
            .combineLatest(ssoTokenMonitor.$sessionExpiry)
            .sink { [weak self] status, expiry in
                guard let self else {
                    return
                }

                self.ssoSessionStatus = status
                self.ssoSessionExpiresAt = expiry

                if self.settings.authenticationMethod == .ssoProfile {
                    self.refreshCredentialState()
                }
            }
    }

    private func executeBackup(job: BackupJob) async {
        var runtimeSettings = settings

        if let resolvedCredentials = resolveCredentials(preferredRegion: runtimeSettings.awsRegion) {
            runtimeSettings.awsAccessKey = resolvedCredentials.credentials.accessKey
            runtimeSettings.awsSecretKey = resolvedCredentials.credentials.secretKey
            runtimeSettings.awsSessionToken = Self.trimmed(resolvedCredentials.credentials.sessionToken ?? "")
            if Self.trimmed(runtimeSettings.awsRegion).isEmpty, let detectedRegion = resolvedCredentials.region {
                runtimeSettings.awsRegion = detectedRegion
            }
            hasAvailableCredentials = true
            credentialSource = resolvedCredentials.credentialSource
        } else {
            runtimeSettings.awsAccessKey = ""
            runtimeSettings.awsSecretKey = ""
            runtimeSettings.awsSessionToken = ""
            hasAvailableCredentials = false
            credentialSource = nil
        }

        do {
            try await backupEngine.run(job: job, settings: runtimeSettings)
        } catch {
            if !(error is CancellationError) && job.status != .failed {
                job.markFailed(error.localizedDescription)
            }
        }

        var updatedHistory = history
        updatedHistory.insert(job.historyEntry(), at: 0)
        history = Array(updatedHistory.prefix(100))

        if currentJob?.id == job.id {
            currentJob = nil
        }
        backupTask = nil
    }

    private func ssoReauthenticationPromptIfNeeded() -> String? {
        guard usesSSOAuthentication else {
            return nil
        }

        let profileName = Self.trimmed(settings.ssoProfileName)
        guard !profileName.isEmpty else {
            return "SSO profile name is required. Set a profile and run `aws sso login --profile <name>`."
        }

        let tokenStatus: SSOTokenStatus = ssoSessionStatus ?? GlacierClient.ssoTokenStatus(profileName: profileName)
        switch tokenStatus {
        case .missingProfile:
            return "SSO profile '\(profileName)' was not found in ~/.aws/config. Run `aws configure sso --profile \(profileName)`."
        case .missingToken:
            return "SSO login is required for profile '\(profileName)'. Run `aws sso login --profile \(profileName)`."
        case .expired(let expiresAt):
            let formattedDate = expiresAt.formatted(date: Date.FormatStyle.DateStyle.abbreviated, time: Date.FormatStyle.TimeStyle.shortened)
            return "SSO session for '\(profileName)' expired at \(formattedDate). Run `aws sso login --profile \(profileName)`."
        case .valid:
            return nil
        }
    }

    private func saveSettings() {
        let encoder = JSONEncoder()
        guard let data = try? encoder.encode(settings) else {
            return
        }
        userDefaults.set(data, forKey: Self.settingsKey)
    }

    private func saveHistory() {
        let encoder = JSONEncoder()
        guard let data = try? encoder.encode(history) else {
            return
        }
        userDefaults.set(data, forKey: Self.historyKey)
    }

    private func refreshCredentialState() {
        let resolvedCredentials = resolveCredentials(preferredRegion: settings.awsRegion)
        hasAvailableCredentials = resolvedCredentials != nil
        credentialSource = resolvedCredentials?.credentialSource
    }

    private func configureSSOTokenMonitor() {
        guard usesSSOAuthentication else {
            ssoTokenMonitor.configure(profileName: nil)
            return
        }

        ssoTokenMonitor.configure(profileName: settings.ssoProfileName)
    }

    private static func loadSettings(from userDefaults: UserDefaults, key: String) -> Settings {
        guard
            let data = userDefaults.data(forKey: key),
            let decoded = try? JSONDecoder().decode(Settings.self, from: data)
        else {
            return Settings()
        }

        return decoded
    }

    private static func loadHistory(from userDefaults: UserDefaults, key: String) -> [BackupHistoryEntry] {
        guard
            let data = userDefaults.data(forKey: key),
            let decoded = try? JSONDecoder().decode([BackupHistoryEntry].self, from: data)
        else {
            return []
        }

        return decoded
    }

    private static func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func launchAgentInterval(for settings: Settings) -> LaunchAgent.ScheduleInterval {
        switch settings.scheduleInterval {
        case .daily:
            return .daily()
        case .weekly:
            return .weekly()
        case .customHours:
            return .customHours(min(max(settings.customIntervalHours, 1), 168))
        }
    }

    private static func currentExecutablePath() -> String {
        if
            let executablePath = Bundle.main.executableURL?.path,
            !trimmed(executablePath).isEmpty
        {
            return trimmed(executablePath)
        }
        return trimmed(CommandLine.arguments.first ?? "")
    }
}
