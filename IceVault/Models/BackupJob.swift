import Foundation

@MainActor
final class BackupJob: ObservableObject, Identifiable {
    enum Status: String, Codable, CaseIterable {
        case idle
        case scanning
        case uploading
        case completed
        case failed

        var displayName: String {
            rawValue.capitalized
        }
    }

    let id: UUID
    let sourceRoot: String
    let bucket: String
    let startedAt: Date

    @Published var status: Status
    @Published var filesTotal: Int
    @Published var filesUploaded: Int
    @Published var bytesTotal: Int64
    @Published var bytesUploaded: Int64
    @Published var completedAt: Date?
    @Published var error: String?

    init(
        id: UUID = UUID(),
        sourceRoot: String,
        bucket: String,
        status: Status = .idle,
        filesTotal: Int = 0,
        filesUploaded: Int = 0,
        bytesTotal: Int64 = 0,
        bytesUploaded: Int64 = 0,
        startedAt: Date = Date(),
        completedAt: Date? = nil,
        error: String? = nil
    ) {
        self.id = id
        self.sourceRoot = sourceRoot
        self.bucket = bucket
        self.status = status
        self.filesTotal = filesTotal
        self.filesUploaded = filesUploaded
        self.bytesTotal = bytesTotal
        self.bytesUploaded = bytesUploaded
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.error = error
    }

    var isRunning: Bool {
        status == .scanning || status == .uploading
    }

    var fileProgressFraction: Double {
        guard filesTotal > 0 else {
            return status == .completed ? 1 : 0
        }

        return min(1, Double(filesUploaded) / Double(filesTotal))
    }

    var byteProgressFraction: Double {
        guard bytesTotal > 0 else {
            return status == .completed ? 1 : 0
        }

        return min(1, Double(bytesUploaded) / Double(bytesTotal))
    }

    func setScanTotals(fileCount: Int, byteCount: Int64) {
        status = .scanning
        filesTotal = max(fileCount, 0)
        bytesTotal = max(byteCount, 0)
        filesUploaded = 0
        bytesUploaded = 0
    }

    func markUploaded(fileCount: Int = 1, byteCount: Int64) {
        status = .uploading
        filesUploaded = min(filesTotal, filesUploaded + max(fileCount, 0))
        bytesUploaded = min(bytesTotal, bytesUploaded + max(byteCount, 0))
    }

    func markCompleted() {
        status = .completed
        completedAt = Date()
    }

    func markFailed(_ message: String) {
        status = .failed
        error = message
        completedAt = Date()
    }

    func historyEntry() -> BackupHistoryEntry {
        BackupHistoryEntry(
            id: id,
            startedAt: startedAt,
            completedAt: completedAt,
            fileCount: max(filesTotal, filesUploaded),
            status: status,
            sourceRoot: sourceRoot,
            bucket: bucket,
            error: error
        )
    }
}

struct BackupHistoryEntry: Identifiable, Codable {
    let id: UUID
    let startedAt: Date
    let completedAt: Date?
    let fileCount: Int
    let status: BackupJob.Status
    let sourceRoot: String
    let bucket: String
    let error: String?

    var displayDate: Date {
        completedAt ?? startedAt
    }
}
