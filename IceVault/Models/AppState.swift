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

    private let userDefaults: UserDefaults
    private let backupEngine: BackupEngine
    private var currentJobObserver: AnyCancellable?
    private var backupTask: Task<Void, Never>?

    private static let settingsKey = "IceVault.settings"
    private static let historyKey = "IceVault.history"

    init(
        userDefaults: UserDefaults = .standard,
        backupEngine: BackupEngine = BackupEngine()
    ) {
        self.userDefaults = userDefaults
        self.backupEngine = backupEngine
        self.settings = Self.loadSettings(from: userDefaults, key: Self.settingsKey)
        self.history = Self.loadHistory(from: userDefaults, key: Self.historyKey)
    }

    var statusText: String {
        if let currentJob {
            return currentJob.status.displayName
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

    func updateSettings(_ newSettings: Settings) {
        settings = newSettings
    }

    func startBackup() {
        guard currentJob == nil, backupTask == nil else {
            return
        }

        let sourceRoot = settings.sourcePath.isEmpty ? NSHomeDirectory() : settings.sourcePath
        let bucket = settings.bucket.isEmpty ? "unset-bucket" : settings.bucket

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
        do {
            try await backupEngine.run(job: job, settings: settings)
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
}
