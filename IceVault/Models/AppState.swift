import Combine
import Foundation

@MainActor
final class AppState: ObservableObject {
    struct SourceInventorySummary: Equatable {
        let sourcePath: String
        let totalFiles: Int
        let uploadedFiles: Int
        let pendingFiles: Int
        let totalBytes: Int64
        let uploadedBytes: Int64
        let pendingBytes: Int64
    }

    struct InventoryUploadProgress: Equatable {
        let uploadedFiles: Int
        let totalFiles: Int
        let uploadedBytes: Int64
        let totalBytes: Int64
    }

    struct Settings: Codable, Equatable {
        static let defaultMaxConcurrentFileUploads = 3
        static let defaultMaxConcurrentMultipartPartUploads = 2
        static let minimumUploadConcurrency = 1
        static let maximumConcurrentFileUploads = 16
        static let maximumConcurrentMultipartPartUploads = 16
        static let defaultMaxBufferedPendingPlans: Int? = nil
        static let defaultManualMaxBufferedPendingPlans = 1024
        static let minimumBufferedPendingPlans = 128
        static let maximumBufferedPendingPlans = 32_768

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
        var includeHiddenFiles: Bool = false
        var maxConcurrentFileUploads: Int = Settings.defaultMaxConcurrentFileUploads
        var maxConcurrentMultipartPartUploads: Int = Settings.defaultMaxConcurrentMultipartPartUploads
        var maxBufferedPendingPlans: Int? = Settings.defaultMaxBufferedPendingPlans

        enum CodingKeys: String, CodingKey {
            case awsRegion
            case authenticationMethod
            case ssoProfileName
            case bucket
            case sourcePath
            case scheduledBackupsEnabled
            case scheduleInterval
            case customIntervalHours
            case includeHiddenFiles
            case maxConcurrentFileUploads
            case maxConcurrentMultipartPartUploads
            case maxBufferedPendingPlans
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
            customIntervalHours: Int = 24,
            includeHiddenFiles: Bool = false,
            maxConcurrentFileUploads: Int = Settings.defaultMaxConcurrentFileUploads,
            maxConcurrentMultipartPartUploads: Int = Settings.defaultMaxConcurrentMultipartPartUploads,
            maxBufferedPendingPlans: Int? = Settings.defaultMaxBufferedPendingPlans
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
            self.includeHiddenFiles = includeHiddenFiles
            self.maxConcurrentFileUploads = Self.clampFileUploadConcurrency(maxConcurrentFileUploads)
            self.maxConcurrentMultipartPartUploads = Self.clampMultipartPartConcurrency(maxConcurrentMultipartPartUploads)
            self.maxBufferedPendingPlans = Self.clampBufferedPendingPlans(maxBufferedPendingPlans)
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
            includeHiddenFiles = try container.decodeIfPresent(Bool.self, forKey: .includeHiddenFiles) ?? false
            maxConcurrentFileUploads = Self.clampFileUploadConcurrency(
                try container.decodeIfPresent(Int.self, forKey: .maxConcurrentFileUploads) ?? Self.defaultMaxConcurrentFileUploads
            )
            maxConcurrentMultipartPartUploads = Self.clampMultipartPartConcurrency(
                try container.decodeIfPresent(Int.self, forKey: .maxConcurrentMultipartPartUploads)
                    ?? Self.defaultMaxConcurrentMultipartPartUploads
            )
            maxBufferedPendingPlans = Self.clampBufferedPendingPlans(
                try container.decodeIfPresent(Int.self, forKey: .maxBufferedPendingPlans)
            )
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
            try container.encode(includeHiddenFiles, forKey: .includeHiddenFiles)
            try container.encode(maxConcurrentFileUploads, forKey: .maxConcurrentFileUploads)
            try container.encode(maxConcurrentMultipartPartUploads, forKey: .maxConcurrentMultipartPartUploads)
            try container.encodeIfPresent(maxBufferedPendingPlans, forKey: .maxBufferedPendingPlans)
        }

        static func clampFileUploadConcurrency(_ value: Int) -> Int {
            min(max(value, minimumUploadConcurrency), maximumConcurrentFileUploads)
        }

        static func clampMultipartPartConcurrency(_ value: Int) -> Int {
            min(max(value, minimumUploadConcurrency), maximumConcurrentMultipartPartUploads)
        }

        static func clampBufferedPendingPlans(_ value: Int?) -> Int? {
            guard let value else {
                return nil
            }
            return min(max(value, minimumBufferedPendingPlans), maximumBufferedPendingPlans)
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
    @Published private(set) var sourceInventorySummary: SourceInventorySummary?

    private let userDefaults: UserDefaults
    private let backupEngine: BackupEngine
    private let databaseService: DatabaseService?
    private let keychainService: KeychainService
    private let launchAgent: LaunchAgent
    let ssoTokenMonitor: SSOTokenMonitor
    private var currentJobObserver: AnyCancellable?
    private var ssoTokenMonitorObserver: AnyCancellable?
    private var backupTask: Task<Void, Never>?
    private var activeJobInventoryBaseline: SourceInventorySummary?

    private static let settingsKey = "IceVault.settings"
    private static let historyKey = "IceVault.history"
    private static let launchAgentLabel = "com.icevault.backup"

    init(
        userDefaults: UserDefaults = .standard,
        backupEngine: BackupEngine = BackupEngine(),
        databaseService: DatabaseService? = nil,
        keychainService: KeychainService = KeychainService(),
        launchAgent: LaunchAgent = LaunchAgent(),
        ssoTokenMonitor: SSOTokenMonitor? = nil,
        ssoNotificationsEnabled: Bool = true
    ) {
        self.userDefaults = userDefaults
        self.backupEngine = backupEngine
        self.databaseService = databaseService ?? backupEngine.resolvedDatabaseService
        self.keychainService = keychainService
        self.launchAgent = launchAgent
        self.ssoTokenMonitor = ssoTokenMonitor
            ?? SSOTokenMonitor(
                userDefaults: userDefaults,
                notificationsEnabled: ssoNotificationsEnabled
            )
        self.settings = Self.loadSettings(from: userDefaults, key: Self.settingsKey)
        self.history = Self.loadHistory(from: userDefaults, key: Self.historyKey)
        self.hasAvailableCredentials = false
        self.credentialSource = nil
        self.authenticationPromptMessage = nil
        self.ssoSessionStatus = nil
        self.ssoSessionExpiresAt = nil
        self.sourceInventorySummary = nil

        bindSSOTokenMonitor()
        configureSSOTokenMonitor()
        refreshCredentialState()
        refreshSourceInventorySummary()
        saveSettings()
    }

    var statusText: String {
        if let currentJob {
            return currentJob.status.displayName
        }

        if !hasBackupTargetConfiguration {
            return "Not Configured"
        }

        return "Idle"
    }

    var latestBackupStatus: BackupJob.Status? {
        history.first?.status
    }

    var latestDeferredUploadPendingFiles: Int {
        max(history.first?.deferredUploadPendingFiles ?? 0, 0)
    }

    var hasPendingDeferredUploads: Bool {
        latestBackupStatus == .failed && latestDeferredUploadPendingFiles > 0
    }

    var latestDeferredUploadStatusText: String? {
        guard hasPendingDeferredUploads, let latestEntry = history.first else {
            return nil
        }

        let pendingCount = max(latestEntry.deferredUploadPendingFiles, 0)
        var summary = "\(pendingCount.formatted(.number.grouping(.automatic))) file(s) still pending upload. Run Backup Now to retry."
        let lastError = latestEntry.deferredUploadLastError?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !lastError.isEmpty {
            summary += " Last error: \(lastError)"
        }

        return summary
    }

    var idleStatusText: String {
        guard hasBackupTargetConfiguration else {
            return "Not configured"
        }

        guard isConfigured else {
            if hasKnownSourceInventory {
                return "Authentication required (inventory available)"
            }
            return "Authentication required"
        }

        switch latestBackupStatus {
        case .some(.completed):
            return "All backed up ✓"
        case .some(.failed):
            if hasPendingDeferredUploads {
                return "\(latestDeferredUploadPendingFiles.formatted(.number.grouping(.automatic))) pending uploads"
            }
            return "Last backup failed"
        case .some(.idle), .some(.scanning), .some(.uploading), .none:
            return "Ready to back up"
        }
    }

    var statusBadgeText: String {
        if let currentJob {
            return currentJob.status.displayName.uppercased()
        }

        guard hasBackupTargetConfiguration else {
            return "SETUP"
        }

        guard isConfigured else {
            return hasKnownSourceInventory ? "PAUSED" : "AUTH"
        }

        switch latestBackupStatus {
        case .some(.completed):
            return "HEALTHY"
        case .some(.failed):
            if hasPendingDeferredUploads {
                return "RETRY"
            }
            return "ATTENTION"
        case .some(.idle), .some(.scanning), .some(.uploading), .none:
            return "READY"
        }
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
        history.first(where: { $0.status == .completed })?.displayDate
    }

    var hasBackupTargetConfiguration: Bool {
        !Self.trimmed(settings.awsRegion).isEmpty
            && !Self.trimmed(settings.bucket).isEmpty
            && !Self.trimmed(settings.sourcePath).isEmpty
    }

    var isConfigured: Bool {
        hasAvailableCredentials
            && hasBackupTargetConfiguration
    }

    var hasKnownSourceInventory: Bool {
        guard let sourceInventorySummary else {
            return false
        }
        return sourceInventorySummary.totalFiles > 0
    }

    var runningInventoryUploadProgress: InventoryUploadProgress? {
        guard let currentJob else {
            return nil
        }

        let summaryForActiveJob: SourceInventorySummary? = {
            if let activeJobInventoryBaseline, activeJobInventoryBaseline.sourcePath == currentJob.sourceRoot {
                return activeJobInventoryBaseline
            }
            if let sourceInventorySummary, sourceInventorySummary.sourcePath == currentJob.sourceRoot {
                return sourceInventorySummary
            }
            return nil
        }()

        guard let summaryForActiveJob else {
            return nil
        }

        let baselinePendingFiles = max(summaryForActiveJob.pendingFiles, 0)
        let baselineTotalFiles = max(summaryForActiveJob.totalFiles, baselinePendingFiles)
        let runTotalFiles = max(currentJob.filesTotal, 0)
        let runUploadedFiles = max(currentJob.filesUploaded, 0)
        let discoveredTotalFiles = max(currentJob.discoveredFiles, 0)
        let shouldPreferDiscoveredTotals: Bool = {
            if currentJob.isScanInProgress {
                return false
            }
            if discoveredTotalFiles > 0 {
                return true
            }
            return runTotalFiles == 0 && runUploadedFiles == 0
        }()
        let totalFiles = shouldPreferDiscoveredTotals
            ? discoveredTotalFiles
            : max(baselineTotalFiles, discoveredTotalFiles)
        let pendingFilesDuringRun: Int = {
            let pendingScope = currentJob.isScanInProgress ? max(runTotalFiles, baselinePendingFiles) : runTotalFiles
            return max(pendingScope - runUploadedFiles, 0)
        }()
        let uploadedFiles = min(totalFiles, max(totalFiles - pendingFilesDuringRun, 0))

        let baselinePendingBytes = max(summaryForActiveJob.pendingBytes, 0)
        let baselineTotalBytes = max(summaryForActiveJob.totalBytes, baselinePendingBytes)
        let discoveredTotalBytes = max(currentJob.discoveredBytes, 0)
        let runTotalBytes = max(currentJob.bytesTotal, 0)
        let runUploadedBytes = max(currentJob.bytesUploaded, 0)
        let totalBytes = shouldPreferDiscoveredTotals
            ? discoveredTotalBytes
            : max(baselineTotalBytes, discoveredTotalBytes)
        let pendingBytesDuringRun: Int64 = {
            let pendingScope = currentJob.isScanInProgress ? max(runTotalBytes, baselinePendingBytes) : runTotalBytes
            return max(pendingScope - runUploadedBytes, 0)
        }()
        let uploadedBytes = min(totalBytes, max(totalBytes - pendingBytesDuringRun, 0))

        return InventoryUploadProgress(
            uploadedFiles: uploadedFiles,
            totalFiles: totalFiles,
            uploadedBytes: uploadedBytes,
            totalBytes: totalBytes
        )
    }

    var sourceInventoryStatusText: String? {
        guard let sourceInventorySummary, sourceInventorySummary.totalFiles > 0 else {
            return nil
        }

        let uploadedCountText = sourceInventorySummary.uploadedFiles.formatted(.number.grouping(.automatic))
        let totalCountText = sourceInventorySummary.totalFiles.formatted(.number.grouping(.automatic))
        let pendingCountText = sourceInventorySummary.pendingFiles.formatted(.number.grouping(.automatic))

        if sourceInventorySummary.pendingFiles > 0 {
            return "Known inventory: \(uploadedCountText) / \(totalCountText) uploaded, \(pendingCountText) pending"
        }

        return "Known inventory: \(uploadedCountText) / \(totalCountText) uploaded"
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
        sanitized.maxConcurrentFileUploads = Settings.clampFileUploadConcurrency(sanitized.maxConcurrentFileUploads)
        sanitized.maxConcurrentMultipartPartUploads = Settings.clampMultipartPartConcurrency(sanitized.maxConcurrentMultipartPartUploads)
        sanitized.maxBufferedPendingPlans = Settings.clampBufferedPendingPlans(sanitized.maxBufferedPendingPlans)
        settings = sanitized
        configureSSOTokenMonitor()
        refreshCredentialState()
        refreshSourceInventorySummary()
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
        refreshSourceInventorySummary()
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

        refreshSourceInventorySummary()
        if let sourceInventorySummary, sourceInventorySummary.sourcePath == sourceRoot {
            activeJobInventoryBaseline = sourceInventorySummary
        } else {
            activeJobInventoryBaseline = nil
        }

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
        refreshSourceInventorySummary()
        activeJobInventoryBaseline = nil

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

    private func refreshSourceInventorySummary() {
        guard let databaseService else {
            sourceInventorySummary = nil
            return
        }

        let sourceRoot = Self.trimmed(settings.sourcePath)
        guard !sourceRoot.isEmpty else {
            sourceInventorySummary = nil
            return
        }

        do {
            let pendingFiles = max(try databaseService.pendingFileCount(for: sourceRoot), 0)
            let uploadedFiles = max(try databaseService.uploadedCount(for: sourceRoot), 0)
            let totalBytes = max(try databaseService.totalBytes(for: sourceRoot), 0)
            let uploadedBytes = max(try databaseService.uploadedTotalBytes(for: sourceRoot), 0)
            let pendingBytes = max(try databaseService.pendingTotalBytes(for: sourceRoot), 0)
            let totalFiles = max(pendingFiles + uploadedFiles, 0)

            if totalFiles == 0 {
                sourceInventorySummary = nil
                return
            }

            sourceInventorySummary = SourceInventorySummary(
                sourcePath: sourceRoot,
                totalFiles: totalFiles,
                uploadedFiles: uploadedFiles,
                pendingFiles: pendingFiles,
                totalBytes: totalBytes,
                uploadedBytes: uploadedBytes,
                pendingBytes: pendingBytes
            )
        } catch {
            sourceInventorySummary = nil
        }
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
