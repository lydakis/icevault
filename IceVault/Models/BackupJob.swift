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
    @Published var discoveredFiles: Int
    @Published var discoveredBytes: Int64
    @Published var discoveryEstimatedFiles: Int?
    @Published var discoveryEstimatedBytes: Int64?
    @Published var uploadBytesPerSecond: Double
    @Published var isScanInProgress: Bool
    @Published var completedAt: Date?
    @Published var error: String?
    @Published var deferredUploadFailureCount: Int
    @Published var deferredUploadPendingFiles: Int
    @Published var deferredUploadRetryPassCount: Int
    @Published var deferredUploadLastError: String?
    @Published var isRetryingDeferredUploads: Bool

    init(
        id: UUID = UUID(),
        sourceRoot: String,
        bucket: String,
        status: Status = .idle,
        filesTotal: Int = 0,
        filesUploaded: Int = 0,
        bytesTotal: Int64 = 0,
        bytesUploaded: Int64 = 0,
        discoveredFiles: Int = 0,
        discoveredBytes: Int64 = 0,
        discoveryEstimatedFiles: Int? = nil,
        discoveryEstimatedBytes: Int64? = nil,
        uploadBytesPerSecond: Double = 0,
        startedAt: Date = Date(),
        completedAt: Date? = nil,
        error: String? = nil,
        deferredUploadFailureCount: Int = 0,
        deferredUploadPendingFiles: Int = 0,
        deferredUploadRetryPassCount: Int = 0,
        deferredUploadLastError: String? = nil,
        isRetryingDeferredUploads: Bool = false
    ) {
        self.id = id
        self.sourceRoot = sourceRoot
        self.bucket = bucket
        self.status = status
        self.filesTotal = filesTotal
        self.filesUploaded = filesUploaded
        self.bytesTotal = bytesTotal
        self.bytesUploaded = bytesUploaded
        self.discoveredFiles = discoveredFiles
        self.discoveredBytes = discoveredBytes
        self.discoveryEstimatedFiles = discoveryEstimatedFiles.map { max($0, 0) }
        self.discoveryEstimatedBytes = discoveryEstimatedBytes.map { max($0, 0) }
        self.uploadBytesPerSecond = max(uploadBytesPerSecond, 0)
        self.isScanInProgress = status == .scanning
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.error = error
        self.deferredUploadFailureCount = max(deferredUploadFailureCount, 0)
        self.deferredUploadPendingFiles = max(deferredUploadPendingFiles, 0)
        self.deferredUploadRetryPassCount = max(deferredUploadRetryPassCount, 0)
        self.deferredUploadLastError = deferredUploadLastError
        self.isRetryingDeferredUploads = isRetryingDeferredUploads
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

    /// Primary progress for the menu bar: prefer full-scope inventory progress when available.
    var primaryProgressFraction: Double {
        if let discoveryEstimatedBytes, discoveryEstimatedBytes > 0 {
            let boundedDiscoveredBytes = min(max(discoveredBytes, 0), discoveryEstimatedBytes)
            return min(1, Double(boundedDiscoveredBytes) / Double(discoveryEstimatedBytes))
        }

        if let discoveryEstimatedFiles, discoveryEstimatedFiles > 0 {
            let boundedDiscoveredFiles = min(max(discoveredFiles, 0), discoveryEstimatedFiles)
            return min(1, Double(boundedDiscoveredFiles) / Double(discoveryEstimatedFiles))
        }

        return fileProgressFraction
    }

    var discoveryFilesPerSecond: Double {
        Double(discoveredFiles) / runtimeDuration
    }

    var discoveryBytesPerSecond: Double {
        Double(discoveredBytes) / runtimeDuration
    }

    var shouldShowDiscoveryRate: Bool {
        isScanInProgress
    }

    var hasDiscoveryEstimate: Bool {
        discoveryEstimatedFiles != nil && discoveryEstimatedBytes != nil
    }

    var hasDeferredUploadIssues: Bool {
        deferredUploadFailureCount > 0 || deferredUploadPendingFiles > 0 || isRetryingDeferredUploads
    }

    func setScanTotals(fileCount: Int, byteCount: Int64) {
        status = .scanning
        isScanInProgress = true
        filesTotal = max(fileCount, 0)
        bytesTotal = max(byteCount, 0)
        filesUploaded = 0
        bytesUploaded = 0
    }

    func markScanCompleted() {
        isScanInProgress = false
    }

    func setDiscoveryEstimate(fileCount: Int?, byteCount: Int64?) {
        if let fileCount {
            discoveryEstimatedFiles = max(fileCount, 0)
        } else {
            discoveryEstimatedFiles = nil
        }

        if let byteCount {
            discoveryEstimatedBytes = max(byteCount, 0)
        } else {
            discoveryEstimatedBytes = nil
        }

        if let discoveryEstimatedFiles, discoveredFiles > discoveryEstimatedFiles {
            self.discoveryEstimatedFiles = discoveredFiles
        }

        if let discoveryEstimatedBytes, discoveredBytes > discoveryEstimatedBytes {
            self.discoveryEstimatedBytes = discoveredBytes
        }
    }

    func markDiscovered(fileCount: Int, byteCount: Int64) {
        discoveredFiles = max(discoveredFiles, max(fileCount, 0))
        discoveredBytes = max(discoveredBytes, max(byteCount, 0))

        if let discoveryEstimatedFiles, discoveredFiles > discoveryEstimatedFiles {
            self.discoveryEstimatedFiles = discoveredFiles
        }

        if let discoveryEstimatedBytes, discoveredBytes > discoveryEstimatedBytes {
            self.discoveryEstimatedBytes = discoveredBytes
        }
    }

    func setUploadRate(bytesPerSecond: Double) {
        uploadBytesPerSecond = max(bytesPerSecond, 0)
    }

    func markUploaded(fileCount: Int = 1, byteCount: Int64) {
        status = .uploading
        filesUploaded = min(filesTotal, filesUploaded + max(fileCount, 0))
        bytesUploaded = min(bytesTotal, bytesUploaded + max(byteCount, 0))
    }

    func markCompleted() {
        status = .completed
        isScanInProgress = false
        isRetryingDeferredUploads = false
        completedAt = Date()
    }

    func markFailed(_ message: String) {
        status = .failed
        isScanInProgress = false
        isRetryingDeferredUploads = false
        error = message
        completedAt = Date()
    }

    func resetDeferredUploadTelemetry() {
        deferredUploadFailureCount = 0
        deferredUploadPendingFiles = 0
        deferredUploadRetryPassCount = 0
        deferredUploadLastError = nil
        isRetryingDeferredUploads = false
    }

    func markDeferredUploadFailure(_ message: String?, pendingFiles: Int? = nil) {
        deferredUploadFailureCount += 1
        if let pendingFiles {
            deferredUploadPendingFiles = max(pendingFiles, 0)
        }
        let normalizedMessage = message?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let normalizedMessage, !normalizedMessage.isEmpty {
            deferredUploadLastError = normalizedMessage
        }
    }

    func setDeferredUploadPendingFiles(_ count: Int) {
        deferredUploadPendingFiles = max(count, 0)
    }

    func markDeferredRetryPassStarted() {
        deferredUploadRetryPassCount += 1
        isRetryingDeferredUploads = true
    }

    func markDeferredRetryPassCompleted() {
        isRetryingDeferredUploads = false
    }

    func historyEntry() -> BackupHistoryEntry {
        BackupHistoryEntry(
            id: id,
            startedAt: startedAt,
            completedAt: completedAt,
            filesUploaded: max(filesUploaded, 0),
            bytesUploaded: max(bytesUploaded, 0),
            status: status,
            sourceRoot: sourceRoot,
            bucket: bucket,
            error: error,
            deferredUploadFailureCount: max(deferredUploadFailureCount, 0),
            deferredUploadPendingFiles: max(deferredUploadPendingFiles, 0),
            deferredUploadRetryPassCount: max(deferredUploadRetryPassCount, 0),
            deferredUploadLastError: deferredUploadLastError
        )
    }

    private var runtimeDuration: TimeInterval {
        max((completedAt ?? Date()).timeIntervalSince(startedAt), 0.001)
    }
}

struct BackupHistoryEntry: Identifiable, Codable {
    let id: UUID
    let startedAt: Date
    let completedAt: Date?
    let filesUploaded: Int
    let bytesUploaded: Int64
    let status: BackupJob.Status
    let sourceRoot: String
    let bucket: String
    let error: String?
    let deferredUploadFailureCount: Int
    let deferredUploadPendingFiles: Int
    let deferredUploadRetryPassCount: Int
    let deferredUploadLastError: String?

    enum CodingKeys: String, CodingKey {
        case id
        case startedAt
        case completedAt
        case filesUploaded
        case fileCount
        case bytesUploaded
        case status
        case sourceRoot
        case bucket
        case error
        case deferredUploadFailureCount
        case deferredUploadPendingFiles
        case deferredUploadRetryPassCount
        case deferredUploadLastError
    }

    init(
        id: UUID,
        startedAt: Date,
        completedAt: Date?,
        filesUploaded: Int,
        bytesUploaded: Int64,
        status: BackupJob.Status,
        sourceRoot: String,
        bucket: String,
        error: String?,
        deferredUploadFailureCount: Int = 0,
        deferredUploadPendingFiles: Int = 0,
        deferredUploadRetryPassCount: Int = 0,
        deferredUploadLastError: String? = nil
    ) {
        self.id = id
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.filesUploaded = max(filesUploaded, 0)
        self.bytesUploaded = max(bytesUploaded, 0)
        self.status = status
        self.sourceRoot = sourceRoot
        self.bucket = bucket
        self.error = error
        self.deferredUploadFailureCount = max(deferredUploadFailureCount, 0)
        self.deferredUploadPendingFiles = max(deferredUploadPendingFiles, 0)
        self.deferredUploadRetryPassCount = max(deferredUploadRetryPassCount, 0)
        self.deferredUploadLastError = deferredUploadLastError
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(UUID.self, forKey: .id)
        startedAt = try container.decode(Date.self, forKey: .startedAt)
        completedAt = try container.decodeIfPresent(Date.self, forKey: .completedAt)

        let legacyFileCount = try container.decodeIfPresent(Int.self, forKey: .fileCount) ?? 0
        filesUploaded = max(
            try container.decodeIfPresent(Int.self, forKey: .filesUploaded) ?? legacyFileCount,
            0
        )

        bytesUploaded = max(try container.decodeIfPresent(Int64.self, forKey: .bytesUploaded) ?? 0, 0)
        status = try container.decode(BackupJob.Status.self, forKey: .status)
        sourceRoot = try container.decode(String.self, forKey: .sourceRoot)
        bucket = try container.decode(String.self, forKey: .bucket)
        error = try container.decodeIfPresent(String.self, forKey: .error)
        deferredUploadFailureCount = max(
            try container.decodeIfPresent(Int.self, forKey: .deferredUploadFailureCount) ?? 0,
            0
        )
        deferredUploadPendingFiles = max(
            try container.decodeIfPresent(Int.self, forKey: .deferredUploadPendingFiles) ?? 0,
            0
        )
        deferredUploadRetryPassCount = max(
            try container.decodeIfPresent(Int.self, forKey: .deferredUploadRetryPassCount) ?? 0,
            0
        )
        deferredUploadLastError = try container.decodeIfPresent(String.self, forKey: .deferredUploadLastError)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(startedAt, forKey: .startedAt)
        try container.encodeIfPresent(completedAt, forKey: .completedAt)
        try container.encode(filesUploaded, forKey: .filesUploaded)
        try container.encode(filesUploaded, forKey: .fileCount)
        try container.encode(bytesUploaded, forKey: .bytesUploaded)
        try container.encode(status, forKey: .status)
        try container.encode(sourceRoot, forKey: .sourceRoot)
        try container.encode(bucket, forKey: .bucket)
        try container.encodeIfPresent(error, forKey: .error)
        try container.encode(deferredUploadFailureCount, forKey: .deferredUploadFailureCount)
        try container.encode(deferredUploadPendingFiles, forKey: .deferredUploadPendingFiles)
        try container.encode(deferredUploadRetryPassCount, forKey: .deferredUploadRetryPassCount)
        try container.encodeIfPresent(deferredUploadLastError, forKey: .deferredUploadLastError)
    }

    var fileCount: Int {
        filesUploaded
    }

    var displayDate: Date {
        completedAt ?? startedAt
    }

    var duration: TimeInterval {
        max((completedAt ?? startedAt).timeIntervalSince(startedAt), 0)
    }
}
