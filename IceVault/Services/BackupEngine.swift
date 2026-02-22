import Foundation

enum BackupEngineError: LocalizedError {
    case invalidSettings(String)
    case missingFileRecordID(String)
    case sourceFileMissing(String)

    var errorDescription: String? {
        switch self {
        case .invalidSettings(let message):
            return message
        case .missingFileRecordID(let relativePath):
            return "Missing database identifier for pending file: \(relativePath)"
        case .sourceFileMissing(let path):
            return "Source file disappeared before upload: \(path)"
        }
    }
}

final class BackupEngine: @unchecked Sendable {
    typealias GlacierClientFactory = (_ accessKey: String, _ secretKey: String, _ region: String) throws -> GlacierClient

    private let scanner: FileScanner
    private let fileManager: FileManager
    private let database: DatabaseService?
    private let glacierClientFactory: GlacierClientFactory

    init(
        scanner: FileScanner = FileScanner(),
        fileManager: FileManager = .default,
        database: DatabaseService? = try? DatabaseService(),
        glacierClientFactory: @escaping GlacierClientFactory = { accessKey, secretKey, region in
            try GlacierClient(accessKey: accessKey, secretKey: secretKey, region: region)
        }
    ) {
        self.scanner = scanner
        self.fileManager = fileManager
        self.database = database
        self.glacierClientFactory = glacierClientFactory
    }

    func run(job: BackupJob, settings: AppState.Settings) async throws {
        let sourceRoot = await MainActor.run { job.sourceRoot }
        let bucket = await MainActor.run { job.bucket }

        do {
            try validate(settings: settings, sourceRoot: sourceRoot, bucket: bucket)

            await MainActor.run {
                job.status = .scanning
                job.error = nil
                job.completedAt = nil
                job.filesUploaded = 0
                job.bytesUploaded = 0
            }

            let scannedFiles = try scanner.scan(sourceRoot: sourceRoot)
            try Task.checkCancellation()

            let pendingFiles = try pendingFilesAfterSync(scannedFiles: scannedFiles, sourceRoot: sourceRoot)
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

            let glacierClient = try glacierClientFactory(
                settings.awsAccessKey,
                settings.awsSecretKey,
                settings.awsRegion
            )

            var completedFiles = 0
            var completedBytes: Int64 = 0
            let sourceRootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true)

            for record in pendingFiles {
                try Task.checkCancellation()

                guard let recordID = record.id else {
                    throw BackupEngineError.missingFileRecordID(record.relativePath)
                }

                let localFileURL = sourceRootURL.appendingPathComponent(record.relativePath)
                guard fileManager.fileExists(atPath: localFileURL.path) else {
                    throw BackupEngineError.sourceFileMissing(localFileURL.path)
                }

                let objectKey = Self.objectKey(from: record.relativePath)
                let fileSize = max(record.fileSize, 0)
                let bytesCommittedBeforeFile = completedBytes
                let filesCommittedBeforeFile = completedFiles

                _ = try await glacierClient.uploadFile(
                    localPath: localFileURL.path,
                    bucket: bucket,
                    key: objectKey,
                    storageClass: FileRecord.deepArchiveStorageClass
                ) { uploadedBytesForCurrentFile in
                    let boundedProgress = min(max(uploadedBytesForCurrentFile, 0), fileSize)
                    Task { @MainActor in
                        job.filesUploaded = filesCommittedBeforeFile
                        job.bytesUploaded = min(job.bytesTotal, bytesCommittedBeforeFile + boundedProgress)
                    }
                }

                if let database {
                    try database.markUploaded(id: recordID, glacierKey: objectKey)
                }

                completedFiles += 1
                completedBytes += fileSize
                let filesCommitted = completedFiles
                let bytesCommitted = completedBytes

                await MainActor.run {
                    job.filesUploaded = filesCommitted
                    job.bytesUploaded = min(job.bytesTotal, bytesCommitted)
                }
            }

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

    private func pendingFilesAfterSync(scannedFiles: [FileRecord], sourceRoot: String) throws -> [FileRecord] {
        guard let database else {
            return scannedFiles
        }

        try database.syncScannedFiles(scannedFiles, for: sourceRoot)
        return try database.pendingFiles(for: sourceRoot)
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
