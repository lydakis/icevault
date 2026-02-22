import Combine
import Foundation

@MainActor
final class AppState: ObservableObject {
    struct Settings: Codable, Equatable {
        var awsAccessKey: String = ""
        var awsSecretKey: String = ""
        var awsRegion: String = "us-east-1"
        var bucket: String = ""
        var sourcePath: String = ""

        enum CodingKeys: String, CodingKey {
            case awsRegion
            case bucket
            case sourcePath
        }

        init(
            awsAccessKey: String = "",
            awsSecretKey: String = "",
            awsRegion: String = "us-east-1",
            bucket: String = "",
            sourcePath: String = ""
        ) {
            self.awsAccessKey = awsAccessKey
            self.awsSecretKey = awsSecretKey
            self.awsRegion = awsRegion
            self.bucket = bucket
            self.sourcePath = sourcePath
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            awsAccessKey = ""
            awsSecretKey = ""
            awsRegion = try container.decodeIfPresent(String.self, forKey: .awsRegion) ?? "us-east-1"
            bucket = try container.decodeIfPresent(String.self, forKey: .bucket) ?? ""
            sourcePath = try container.decodeIfPresent(String.self, forKey: .sourcePath) ?? ""
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(awsRegion, forKey: .awsRegion)
            try container.encode(bucket, forKey: .bucket)
            try container.encode(sourcePath, forKey: .sourcePath)
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

    @Published private(set) var hasStoredCredentials: Bool

    private let userDefaults: UserDefaults
    private let backupEngine: BackupEngine
    private let keychainService: KeychainService
    private var currentJobObserver: AnyCancellable?
    private var backupTask: Task<Void, Never>?

    private static let settingsKey = "IceVault.settings"
    private static let historyKey = "IceVault.history"

    init(
        userDefaults: UserDefaults = .standard,
        backupEngine: BackupEngine = BackupEngine(),
        keychainService: KeychainService = KeychainService()
    ) {
        self.userDefaults = userDefaults
        self.backupEngine = backupEngine
        self.keychainService = keychainService
        self.settings = Self.loadSettings(from: userDefaults, key: Self.settingsKey)
        self.history = Self.loadHistory(from: userDefaults, key: Self.historyKey)
        self.hasStoredCredentials = false

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
        hasStoredCredentials
            && !Self.trimmed(settings.awsRegion).isEmpty
            && !Self.trimmed(settings.bucket).isEmpty
            && !Self.trimmed(settings.sourcePath).isEmpty
    }

    func updateSettings(_ newSettings: Settings) {
        var sanitized = newSettings
        sanitized.awsAccessKey = ""
        sanitized.awsSecretKey = ""
        sanitized.awsRegion = Self.trimmed(sanitized.awsRegion)
        sanitized.bucket = Self.trimmed(sanitized.bucket)
        sanitized.sourcePath = Self.trimmed(sanitized.sourcePath)
        settings = sanitized
    }

    func loadStoredCredentials() throws -> AWSCredentials? {
        try keychainService.loadCredentials()
    }

    func saveCredentials(accessKey: String, secretKey: String) throws {
        try keychainService.save(accessKey: accessKey, secretKey: secretKey)
        hasStoredCredentials = true
    }

    func deleteStoredCredentials() throws {
        try keychainService.deleteCredentials()
        hasStoredCredentials = false
    }

    func clearHistory() {
        history = []
    }

    func testConnection(accessKey: String, secretKey: String, region: String, bucket: String) async throws {
        let client = try GlacierClient(
            accessKey: accessKey,
            secretKey: secretKey,
            region: region
        )
        try await client.verifyBucketAccess(bucket: bucket)
    }

    func startBackup() {
        guard currentJob == nil, backupTask == nil else {
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

    private func executeBackup(job: BackupJob) async {
        var runtimeSettings = settings

        do {
            if let credentials = try keychainService.loadCredentials() {
                runtimeSettings.awsAccessKey = credentials.accessKey
                runtimeSettings.awsSecretKey = credentials.secretKey
                hasStoredCredentials = true
            } else {
                runtimeSettings.awsAccessKey = ""
                runtimeSettings.awsSecretKey = ""
                hasStoredCredentials = false
            }
        } catch {
            runtimeSettings.awsAccessKey = ""
            runtimeSettings.awsSecretKey = ""
            hasStoredCredentials = false
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
        do {
            hasStoredCredentials = try keychainService.loadCredentials() != nil
        } catch {
            hasStoredCredentials = false
        }
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
}
