import Foundation

enum BackupEngineError: LocalizedError {
    case invalidSettings(String)
    case databaseUnavailable(String)
    case missingFileRecordID(String)
    case sourceFileMissing(String)

    var errorDescription: String? {
        switch self {
        case .invalidSettings(let message):
            return message
        case .databaseUnavailable(let message):
            return "Backup database unavailable: \(message)"
        case .missingFileRecordID(let relativePath):
            return "Missing database identifier for pending file: \(relativePath)"
        case .sourceFileMissing(let path):
            return "Source file disappeared before upload: \(path)"
        }
    }
}

/// Thread-safety invariant: stored properties are immutable after initialization, and
/// per-run mutable state is kept in local actors/value types inside `run`.
final class BackupEngine: @unchecked Sendable {
    typealias GlacierClientFactory = (
        _ accessKey: String,
        _ secretKey: String,
        _ sessionToken: String?,
        _ region: String,
        _ database: DatabaseService?
    ) throws -> GlacierClient

    private static let staleMultipartUploadThreshold: TimeInterval = 24 * 60 * 60
    private static let remoteValidationConcurrency = 4

    private enum DatabaseState {
        case available(DatabaseService)
        case unavailable(String)
    }

    private struct PendingUploadPlan: Sendable {
        let recordID: Int64
        let relativePath: String
        let localPath: String
        let objectKey: String
        let fileSize: Int64
        let sha256: String
    }

    private struct UploadProgressSnapshot: Sendable {
        let filesUploaded: Int
        let bytesUploaded: Int64
    }

    private struct RemoteValidationOutcome: Sendable {
        let recordID: Int64
        let result: RemoteObjectValidationResult
    }

    private actor UploadProgressTracker {
        private let fileSizesByRecordID: [Int64: Int64]
        private let totalBytes: Int64

        private var completedRecordIDs: Set<Int64> = []
        private var inFlightBytesByRecordID: [Int64: Int64] = [:]
        private var inFlightBytesTotal: Int64 = 0
        private var completedBytes: Int64 = 0

        init(plans: [PendingUploadPlan], totalBytes: Int64) {
            self.fileSizesByRecordID = Dictionary(
                uniqueKeysWithValues: plans.map { ($0.recordID, $0.fileSize) }
            )
            self.totalBytes = max(totalBytes, 0)
        }

        func update(recordID: Int64, uploadedBytes: Int64) -> UploadProgressSnapshot {
            guard
                !completedRecordIDs.contains(recordID),
                let fileSize = fileSizesByRecordID[recordID]
            else {
                return snapshot()
            }

            let boundedProgress = min(max(uploadedBytes, 0), max(fileSize, 0))
            let previousProgress = inFlightBytesByRecordID[recordID] ?? 0
            let nextProgress = max(previousProgress, boundedProgress)
            inFlightBytesByRecordID[recordID] = nextProgress
            inFlightBytesTotal += nextProgress - previousProgress
            return snapshot()
        }

        func markCompleted(recordID: Int64) -> UploadProgressSnapshot {
            guard !completedRecordIDs.contains(recordID) else {
                return snapshot()
            }

            completedRecordIDs.insert(recordID)
            let previousInFlightProgress = inFlightBytesByRecordID.removeValue(forKey: recordID) ?? 0
            inFlightBytesTotal -= previousInFlightProgress
            completedBytes += max(fileSizesByRecordID[recordID] ?? 0, 0)
            return snapshot()
        }

        private func snapshot() -> UploadProgressSnapshot {
            let uploadedBytes = min(totalBytes, max(completedBytes + inFlightBytesTotal, 0))
            return UploadProgressSnapshot(
                filesUploaded: completedRecordIDs.count,
                bytesUploaded: uploadedBytes
            )
        }
    }

    private let scanner: FileScanner
    private let fileManager: FileManager
    private let databaseState: DatabaseState
    private let glacierClientFactory: GlacierClientFactory

    init(
        scanner: FileScanner = FileScanner(),
        fileManager: FileManager = .default,
        database: DatabaseService? = nil,
        databaseFactory: () throws -> DatabaseService = { try DatabaseService() },
        glacierClientFactory: @escaping GlacierClientFactory = { accessKey, secretKey, sessionToken, region, database in
            try GlacierClient(
                accessKey: accessKey,
                secretKey: secretKey,
                sessionToken: sessionToken,
                region: region,
                database: database
            )
        }
    ) {
        self.scanner = scanner
        self.fileManager = fileManager
        if let database {
            databaseState = .available(database)
        } else {
            do {
                databaseState = .available(try databaseFactory())
            } catch {
                databaseState = .unavailable(error.localizedDescription)
            }
        }
        self.glacierClientFactory = glacierClientFactory
    }

    func run(job: BackupJob, settings: AppState.Settings) async throws {
        let sourceRoot = await MainActor.run { job.sourceRoot }
        let bucket = await MainActor.run { job.bucket }

        do {
            try validate(settings: settings, sourceRoot: sourceRoot, bucket: bucket)
            let database = try resolveDatabase()
            let glacierClient = try glacierClientFactory(
                settings.awsAccessKey,
                settings.awsSecretKey,
                settings.awsSessionToken.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : settings.awsSessionToken,
                settings.awsRegion,
                database
            )
            await glacierClient.abortStaleMultipartUploads(olderThan: Self.staleMultipartUploadThreshold)

            await MainActor.run {
                job.status = .scanning
                job.error = nil
                job.completedAt = nil
                job.filesUploaded = 0
                job.bytesUploaded = 0
            }

            let scannedFiles = try scanner.scan(sourceRoot: sourceRoot)
            try Task.checkCancellation()

            let pendingFiles = try await pendingFilesAfterSync(
                scannedFiles: scannedFiles,
                sourceRoot: sourceRoot,
                bucket: bucket,
                database: database,
                glacierClient: glacierClient
            )
            let bytesTotal = pendingFiles.reduce(Int64(0)) { partialResult, record in
                partialResult + max(record.fileSize, 0)
            }

            await MainActor.run {
                job.setScanTotals(fileCount: pendingFiles.count, byteCount: bytesTotal)
                job.status = .uploading
            }

            guard !pendingFiles.isEmpty else {
                await MainActor.run {
                    job.markCompleted()
                }
                return
            }

            let sourceRootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true)
            let uploadPlans = try makeUploadPlans(pendingFiles: pendingFiles, sourceRootURL: sourceRootURL)
            try await uploadPendingFiles(
                plans: uploadPlans,
                bucket: bucket,
                settings: settings,
                database: database,
                glacierClient: glacierClient,
                job: job,
                totalBytes: bytesTotal
            )

            await MainActor.run {
                job.markCompleted()
            }
        } catch is CancellationError {
            await MainActor.run {
                job.markFailed("Backup canceled")
            }
            throw CancellationError()
        } catch {
            await MainActor.run {
                job.markFailed(error.localizedDescription)
            }
            throw error
        }
    }

    private func resolveDatabase() throws -> DatabaseService {
        switch databaseState {
        case .available(let database):
            return database
        case .unavailable(let message):
            throw BackupEngineError.databaseUnavailable(message)
        }
    }

    private func pendingFilesAfterSync(
        scannedFiles: [FileRecord],
        sourceRoot: String,
        bucket: String,
        database: DatabaseService,
        glacierClient: GlacierClient
    ) async throws -> [FileRecord] {
        try database.syncScannedFiles(scannedFiles, for: sourceRoot)
        try await validateUploadedFilesOnRemote(
            sourceRoot: sourceRoot,
            bucket: bucket,
            database: database,
            glacierClient: glacierClient
        )
        return try database.pendingFiles(for: sourceRoot)
    }

    private func makeUploadPlans(pendingFiles: [FileRecord], sourceRootURL: URL) throws -> [PendingUploadPlan] {
        var plans: [PendingUploadPlan] = []
        plans.reserveCapacity(pendingFiles.count)

        for record in pendingFiles {
            guard let recordID = record.id else {
                throw BackupEngineError.missingFileRecordID(record.relativePath)
            }

            let localFileURL = sourceRootURL.appendingPathComponent(record.relativePath)
            let objectKey = record.glacierKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? Self.objectKey(from: record.relativePath)
                : record.glacierKey
            plans.append(
                PendingUploadPlan(
                    recordID: recordID,
                    relativePath: record.relativePath,
                    localPath: localFileURL.path,
                    objectKey: objectKey,
                    fileSize: max(record.fileSize, 0),
                    sha256: record.sha256
                )
            )
        }

        return plans
    }

    private func uploadPendingFiles(
        plans: [PendingUploadPlan],
        bucket: String,
        settings: AppState.Settings,
        database: DatabaseService,
        glacierClient: GlacierClient,
        job: BackupJob,
        totalBytes: Int64
    ) async throws {
        guard !plans.isEmpty else {
            return
        }

        let fileConcurrency = max(1, min(settings.maxConcurrentFileUploads, plans.count))
        let multipartPartConcurrency = max(1, settings.maxConcurrentMultipartPartUploads)
        let progressTracker = UploadProgressTracker(plans: plans, totalBytes: totalBytes)

        try await BoundedTaskRunner.run(
            inputs: plans,
            maxConcurrentTasks: fileConcurrency,
            operation: { [self] plan in
                let remoteValidation = try await self.preflightRemoteValidation(
                    for: plan,
                    bucket: bucket,
                    glacierClient: glacierClient
                )

                switch remoteValidation {
                case .valid:
                    return plan
                case .missing, .mismatch, .inaccessible:
                    _ = try await glacierClient.uploadFile(
                        localPath: plan.localPath,
                        bucket: bucket,
                        key: plan.objectKey,
                        storageClass: FileRecord.deepArchiveStorageClass,
                        fileRecordId: plan.recordID,
                        multipartPartConcurrency: multipartPartConcurrency
                    ) { uploadedBytesForCurrentFile in
                        let snapshot = await progressTracker.update(
                            recordID: plan.recordID,
                            uploadedBytes: uploadedBytesForCurrentFile
                        )
                        await MainActor.run {
                            Self.applyMonotonicProgress(
                                filesUploaded: snapshot.filesUploaded,
                                bytesUploaded: snapshot.bytesUploaded,
                                to: job
                            )
                        }
                    }
                    return plan
                }
            },
            onSuccess: { completedPlan in
                try database.markUploaded(id: completedPlan.recordID, glacierKey: completedPlan.objectKey)
                let snapshot = await progressTracker.markCompleted(recordID: completedPlan.recordID)
                await MainActor.run {
                    Self.applyMonotonicProgress(
                        filesUploaded: snapshot.filesUploaded,
                        bytesUploaded: snapshot.bytesUploaded,
                        to: job
                    )
                }
            }
        )
    }

    private func validateUploadedFilesOnRemote(
        sourceRoot: String,
        bucket: String,
        database: DatabaseService,
        glacierClient: GlacierClient
    ) async throws {
        let uploadedFiles = try database.uploadedFiles(for: sourceRoot)
        guard !uploadedFiles.isEmpty else {
            return
        }

        let auditTargets = uploadedFiles.compactMap { record -> PendingUploadPlan? in
            guard let recordID = record.id else {
                return nil
            }
            let localFileURL = URL(fileURLWithPath: sourceRoot, isDirectory: true)
                .appendingPathComponent(record.relativePath)
            let objectKey = record.glacierKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? Self.objectKey(from: record.relativePath)
                : record.glacierKey
            return PendingUploadPlan(
                recordID: recordID,
                relativePath: record.relativePath,
                localPath: localFileURL.path,
                objectKey: objectKey,
                fileSize: max(record.fileSize, 0),
                sha256: record.sha256
            )
        }

        guard !auditTargets.isEmpty else {
            return
        }

        try await BoundedTaskRunner.run(
            inputs: auditTargets,
            maxConcurrentTasks: min(Self.remoteValidationConcurrency, auditTargets.count),
            operation: { [self] target in
                let result = try await self.preflightRemoteValidation(
                    for: target,
                    bucket: bucket,
                    glacierClient: glacierClient
                )
                return RemoteValidationOutcome(recordID: target.recordID, result: result)
            },
            onSuccess: { outcome in
                switch outcome.result {
                case .valid, .inaccessible:
                    break
                case .missing, .mismatch:
                    try database.markPending(id: outcome.recordID)
                }
            }
        )
    }

    private func preflightRemoteValidation(
        for plan: PendingUploadPlan,
        bucket: String,
        glacierClient: GlacierClient
    ) async throws -> RemoteObjectValidationResult {
        do {
            return try await glacierClient.validateRemoteObject(
                bucket: bucket,
                key: plan.objectKey,
                expectedSize: plan.fileSize,
                expectedSHA256: plan.sha256
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as GlacierClientError {
            if case .s3OperationFailed = error {
                return .inaccessible
            }
            throw error
        }
    }

    @MainActor
    static func applyMonotonicProgress(
        filesUploaded: Int,
        bytesUploaded: Int64,
        to job: BackupJob
    ) {
        let boundedFiles = min(max(filesUploaded, 0), job.filesTotal)
        let boundedBytes = min(max(bytesUploaded, 0), job.bytesTotal)
        job.filesUploaded = max(job.filesUploaded, boundedFiles)
        job.bytesUploaded = max(job.bytesUploaded, boundedBytes)
    }

    private func validate(settings: AppState.Settings, sourceRoot: String, bucket: String) throws {
        let accessKey = settings.awsAccessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        if accessKey.isEmpty {
            throw BackupEngineError.invalidSettings("AWS access key is required.")
        }

        let secretKey = settings.awsSecretKey.trimmingCharacters(in: .whitespacesAndNewlines)
        if secretKey.isEmpty {
            throw BackupEngineError.invalidSettings("AWS secret key is required.")
        }

        let region = settings.awsRegion.trimmingCharacters(in: .whitespacesAndNewlines)
        if region.isEmpty {
            throw BackupEngineError.invalidSettings("AWS region is required.")
        }

        let normalizedBucket = bucket.trimmingCharacters(in: .whitespacesAndNewlines)
        if normalizedBucket.isEmpty {
            throw BackupEngineError.invalidSettings("S3 bucket is required.")
        }

        var isDirectory = ObjCBool(false)
        let exists = fileManager.fileExists(atPath: sourceRoot, isDirectory: &isDirectory)
        if !exists || !isDirectory.boolValue {
            throw BackupEngineError.invalidSettings("Source path must be an existing directory: \(sourceRoot)")
        }
    }

    private static func objectKey(from relativePath: String) -> String {
        relativePath
            .replacingOccurrences(of: "\\", with: "/")
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }
}
