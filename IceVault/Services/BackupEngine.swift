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
    private static let scanSyncBatchSize = 128
    private static let remoteAuditBatchSize = 256
    private static let pendingPlanBufferOverrideEnvironmentKey = "ICEVAULT_MAX_BUFFERED_PENDING_PLANS"

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
        let filesTotal: Int
        let bytesTotal: Int64
        let filesUploaded: Int
        let bytesUploaded: Int64
    }

    private struct RemoteValidationOutcome: Sendable {
        let recordID: Int64
        let result: RemoteObjectValidationResult
    }

    private struct PendingPlanStreamChannel {
        let stream: AsyncThrowingStream<[PendingUploadPlan], Error>
        let continuation: AsyncThrowingStream<[PendingUploadPlan], Error>.Continuation
    }

    /// Records asynchronous upload-loop failures so the scanning producer can fail fast.
    private final class UploadLoopFailureSignal: @unchecked Sendable {
        private let stateLock = NSLock()
        private var error: Error?

        func record(_ error: Error) {
            stateLock.lock()
            if self.error == nil {
                self.error = error
            }
            stateLock.unlock()
        }

        func throwIfFailed() throws {
            stateLock.lock()
            let recordedError = error
            stateLock.unlock()

            if let recordedError {
                throw recordedError
            }
        }
    }

    /// Bounds scan-ahead so pending upload plans cannot grow unbounded in memory.
    private actor PendingPlanBackpressureGate {
        private let maxBufferedPlans: Int
        private var availableSlots: Int
        private var isCanceled = false
        private var nextWaiterID: UInt64 = 0
        private var waiters: [UInt64: CheckedContinuation<Void, Never>] = [:]

        init(maxBufferedPlans: Int) {
            let normalizedMaxBufferedPlans = max(1, maxBufferedPlans)
            self.maxBufferedPlans = normalizedMaxBufferedPlans
            availableSlots = normalizedMaxBufferedPlans
        }

        var maxReservableSlotsPerBatch: Int {
            maxBufferedPlans
        }

        func reserve(
            slots: Int,
            whileWaiting abortCheck: (() throws -> Void)? = nil
        ) async throws {
            let normalizedSlots = max(0, slots)
            guard normalizedSlots > 0 else {
                return
            }

            while true {
                try throwIfCanceled()
                try abortCheck?()

                if availableSlots >= normalizedSlots {
                    availableSlots -= normalizedSlots
                    return
                }

                let waiterID = nextWaiterID
                nextWaiterID += 1

                await withTaskCancellationHandler(
                    operation: {
                        await withCheckedContinuation { continuation in
                            waiters[waiterID] = continuation
                        }
                    },
                    onCancel: {
                        Task {
                            await self.cancelWaiter(id: waiterID)
                        }
                    }
                )
            }
        }

        func release(slots: Int) {
            guard slots > 0 else {
                return
            }

            availableSlots = min(maxBufferedPlans, availableSlots + slots)
            resumeAllWaiters()
        }

        func cancel() {
            guard !isCanceled else {
                return
            }

            isCanceled = true
            resumeAllWaiters()
        }

        private func cancelWaiter(id: UInt64) {
            guard let continuation = waiters.removeValue(forKey: id) else {
                return
            }
            continuation.resume()
        }

        private func resumeAllWaiters() {
            guard !waiters.isEmpty else {
                return
            }

            let continuations = waiters.values
            waiters.removeAll(keepingCapacity: true)
            for continuation in continuations {
                continuation.resume()
            }
        }

        private func throwIfCanceled() throws {
            if isCanceled || Task.isCancelled {
                throw CancellationError()
            }
        }
    }

    private actor UploadProgressTracker {
        private var trackedRecordIDs: Set<Int64> = []
        private var fileSizesByRecordID: [Int64: Int64] = [:]
        private var totalFiles: Int = 0
        private var totalBytes: Int64 = 0
        private var completedRecordIDs: Set<Int64> = []
        private var inFlightBytesByRecordID: [Int64: Int64] = [:]
        private var inFlightBytesTotal: Int64 = 0
        private var completedBytes: Int64 = 0

        func register(plans: [PendingUploadPlan]) -> UploadProgressSnapshot {
            for plan in plans where !trackedRecordIDs.contains(plan.recordID) {
                trackedRecordIDs.insert(plan.recordID)
                fileSizesByRecordID[plan.recordID] = plan.fileSize
                totalFiles += 1
                totalBytes += max(plan.fileSize, 0)
            }
            return snapshot()
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
                filesTotal: totalFiles,
                bytesTotal: totalBytes,
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
                job.filesTotal = 0
                job.filesUploaded = 0
                job.bytesTotal = 0
                job.bytesUploaded = 0
            }

            try Task.checkCancellation()
            let sourceRootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true)
            let progressTracker = UploadProgressTracker()
            let remoteAuditUploadedBefore = Date()

            let scanToken = try database.beginScan(for: sourceRoot)
            let pendingPlanChannel = makePendingPlanStreamChannel()
            let maxBufferedPendingPlans = Self.resolvedMaxBufferedPendingPlans(settings: settings)
            let pendingPlanBackpressureGate = PendingPlanBackpressureGate(
                maxBufferedPlans: maxBufferedPendingPlans
            )
            let uploadLoopFailureSignal = UploadLoopFailureSignal()

            let uploadLoopTask = Task {
                do {
                    try await self.uploadPendingPlansFromStream(
                        pendingPlanStream: pendingPlanChannel.stream,
                        bucket: bucket,
                        settings: settings,
                        database: database,
                        glacierClient: glacierClient,
                        job: job,
                        progressTracker: progressTracker,
                        backpressureGate: pendingPlanBackpressureGate
                    )
                } catch {
                    uploadLoopFailureSignal.record(error)
                    throw error
                }
            }

            do {
                try await withTaskCancellationHandler(
                    operation: {
                        var scannedBatch: [FileRecord] = []
                        scannedBatch.reserveCapacity(Self.scanSyncBatchSize)
                        var didFlushInitialBatch = false
                        let initialFlushBatchSize = max(1, min(Self.scanSyncBatchSize, settings.maxConcurrentFileUploads))

                        try await scanner.scan(sourceRoot: sourceRoot) { record in
                            try Task.checkCancellation()
                            try uploadLoopFailureSignal.throwIfFailed()
                            scannedBatch.append(record)

                            let shouldFlushBatch =
                                scannedBatch.count >= Self.scanSyncBatchSize ||
                                (!didFlushInitialBatch && scannedBatch.count >= initialFlushBatchSize)
                            if shouldFlushBatch {
                                let pendingPlans = try self.makePendingPlansForScannedBatch(
                                    scannedBatch,
                                    sourceRoot: sourceRoot,
                                    scanToken: scanToken,
                                    sourceRootURL: sourceRootURL,
                                    database: database
                                )
                                scannedBatch.removeAll(keepingCapacity: true)
                                didFlushInitialBatch = true
                                try await self.enqueuePendingPlans(
                                    pendingPlans,
                                    into: pendingPlanChannel,
                                    backpressureGate: pendingPlanBackpressureGate,
                                    uploadLoopFailureSignal: uploadLoopFailureSignal
                                )
                            }
                        }
                        try Task.checkCancellation()
                        try uploadLoopFailureSignal.throwIfFailed()

                        if !scannedBatch.isEmpty {
                            let pendingPlans = try makePendingPlansForScannedBatch(
                                scannedBatch,
                                sourceRoot: sourceRoot,
                                scanToken: scanToken,
                                sourceRootURL: sourceRootURL,
                                database: database
                            )
                            try await enqueuePendingPlans(
                                pendingPlans,
                                into: pendingPlanChannel,
                                backpressureGate: pendingPlanBackpressureGate,
                                uploadLoopFailureSignal: uploadLoopFailureSignal
                            )
                        }

                        try database.finishScan(for: sourceRoot, scanToken: scanToken)
                        try await validateUploadedFilesOnRemote(
                            sourceRoot: sourceRoot,
                            bucket: bucket,
                            sourceRootURL: sourceRootURL,
                            uploadedBefore: remoteAuditUploadedBefore,
                            database: database,
                            glacierClient: glacierClient,
                            onPendingPlansBatch: { pendingPlans in
                                try await self.enqueuePendingPlans(
                                    pendingPlans,
                                    into: pendingPlanChannel,
                                    backpressureGate: pendingPlanBackpressureGate,
                                    uploadLoopFailureSignal: uploadLoopFailureSignal
                                )
                            }
                        )
                        pendingPlanChannel.continuation.finish()
                        try await uploadLoopTask.value
                    },
                    onCancel: {
                        Task {
                            await pendingPlanBackpressureGate.cancel()
                        }
                        uploadLoopTask.cancel()
                        pendingPlanChannel.continuation.finish(throwing: CancellationError())
                    }
                )
            } catch {
                await pendingPlanBackpressureGate.cancel()
                uploadLoopTask.cancel()
                pendingPlanChannel.continuation.finish(throwing: error)
                _ = try? await uploadLoopTask.value
                throw error
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

    private func resolveDatabase() throws -> DatabaseService {
        switch databaseState {
        case .available(let database):
            return database
        case .unavailable(let message):
            throw BackupEngineError.databaseUnavailable(message)
        }
    }

    private func makePendingPlansForScannedBatch(
        _ scannedBatch: [FileRecord],
        sourceRoot: String,
        scanToken: String,
        sourceRootURL: URL,
        database: DatabaseService
    ) throws -> [PendingUploadPlan] {
        let pendingFiles = try database.syncScannedFilesBatch(
            scannedBatch,
            for: sourceRoot,
            scanToken: scanToken
        )
        return try makeUploadPlans(pendingFiles: pendingFiles, sourceRootURL: sourceRootURL)
    }

    private func uploadPendingPlansFromStream(
        pendingPlanStream: AsyncThrowingStream<[PendingUploadPlan], Error>,
        bucket: String,
        settings: AppState.Settings,
        database: DatabaseService,
        glacierClient: GlacierClient,
        job: BackupJob,
        progressTracker: UploadProgressTracker,
        backpressureGate: PendingPlanBackpressureGate
    ) async throws {
        for try await plans in pendingPlanStream {
            try Task.checkCancellation()
            do {
                try await uploadPendingFiles(
                    plans: plans,
                    bucket: bucket,
                    settings: settings,
                    database: database,
                    glacierClient: glacierClient,
                    job: job,
                    progressTracker: progressTracker
                )
                await backpressureGate.release(slots: plans.count)
            } catch {
                await backpressureGate.release(slots: plans.count)
                throw error
            }
        }
    }

    private func enqueuePendingPlans(
        _ plans: [PendingUploadPlan],
        into channel: PendingPlanStreamChannel,
        backpressureGate: PendingPlanBackpressureGate,
        uploadLoopFailureSignal: UploadLoopFailureSignal
    ) async throws {
        guard !plans.isEmpty else {
            return
        }

        let chunkSize = max(1, await backpressureGate.maxReservableSlotsPerBatch)
        var nextChunkStart = plans.startIndex
        while nextChunkStart < plans.endIndex {
            try uploadLoopFailureSignal.throwIfFailed()

            let nextChunkEnd = min(nextChunkStart + chunkSize, plans.endIndex)
            let chunk = Array(plans[nextChunkStart..<nextChunkEnd])
            var didReserveSlots = false

            do {
                try await backpressureGate.reserve(
                    slots: chunk.count,
                    whileWaiting: {
                        try uploadLoopFailureSignal.throwIfFailed()
                    }
                )
                didReserveSlots = true
                try uploadLoopFailureSignal.throwIfFailed()

                let yieldResult = channel.continuation.yield(chunk)
                if case .terminated = yieldResult {
                    throw CancellationError()
                }
            } catch {
                if didReserveSlots {
                    await backpressureGate.release(slots: chunk.count)
                }
                try uploadLoopFailureSignal.throwIfFailed()
                throw error
            }

            nextChunkStart = nextChunkEnd
        }
    }

    private static func resolvedMaxBufferedPendingPlans(settings: AppState.Settings) -> Int {
        let defaultBufferSize = max(
            scanSyncBatchSize * 2,
            max(1, settings.maxConcurrentFileUploads) * scanSyncBatchSize
        )

        if let configuredBufferedPendingPlans = settings.maxBufferedPendingPlans {
            return max(scanSyncBatchSize, configuredBufferedPendingPlans)
        }

        guard
            let rawOverrideValue = ProcessInfo.processInfo.environment[pendingPlanBufferOverrideEnvironmentKey],
            let parsedOverrideValue = Int(rawOverrideValue)
        else {
            return defaultBufferSize
        }

        // Buffer size must be able to hold at least one full scan batch to avoid enqueue deadlocks.
        return max(scanSyncBatchSize, parsedOverrideValue)
    }

    private func makePendingPlanStreamChannel() -> PendingPlanStreamChannel {
        var continuation: AsyncThrowingStream<[PendingUploadPlan], Error>.Continuation?
        let stream = AsyncThrowingStream<[PendingUploadPlan], Error> { streamContinuation in
            continuation = streamContinuation
        }

        guard let continuation else {
            fatalError("Failed to initialize pending upload plan stream.")
        }

        return PendingPlanStreamChannel(
            stream: stream,
            continuation: continuation
        )
    }

    private func makeUploadPlans(pendingFiles: [FileRecord], sourceRootURL: URL) throws -> [PendingUploadPlan] {
        let orderedPendingFiles = pendingFiles.sorted { lhs, rhs in
            lhs.relativePath.localizedStandardCompare(rhs.relativePath) == .orderedAscending
        }

        var plans: [PendingUploadPlan] = []
        plans.reserveCapacity(orderedPendingFiles.count)

        for record in orderedPendingFiles {
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
        progressTracker: UploadProgressTracker
    ) async throws {
        guard !plans.isEmpty else {
            return
        }

        let fileConcurrency = max(1, min(settings.maxConcurrentFileUploads, plans.count))
        let multipartPartConcurrency = max(1, settings.maxConcurrentMultipartPartUploads)

        let registeredSnapshot = await progressTracker.register(plans: plans)
        await MainActor.run {
            Self.applyProgressSnapshot(registeredSnapshot, to: job)
        }

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
                            Self.applyProgressSnapshot(snapshot, to: job)
                        }
                    }
                    return plan
                }
            },
            onSuccess: { completedPlan in
                // Persist each completion immediately so interrupted runs remain resume-safe.
                try database.markUploaded(id: completedPlan.recordID, glacierKey: completedPlan.objectKey)

                let snapshot = await progressTracker.markCompleted(recordID: completedPlan.recordID)
                await MainActor.run {
                    Self.applyProgressSnapshot(snapshot, to: job)
                }
            }
        )
    }

    private func validateUploadedFilesOnRemote(
        sourceRoot: String,
        bucket: String,
        sourceRootURL: URL,
        uploadedBefore: Date,
        database: DatabaseService,
        glacierClient: GlacierClient,
        onPendingPlansBatch: (([PendingUploadPlan]) async throws -> Void)? = nil
    ) async throws {
        var lastRelativePath: String?

        while true {
            let uploadedFiles = try database.uploadedFiles(
                for: sourceRoot,
                afterRelativePath: lastRelativePath,
                limit: Self.remoteAuditBatchSize,
                uploadedBefore: uploadedBefore
            )

            guard !uploadedFiles.isEmpty else {
                return
            }

            lastRelativePath = uploadedFiles.last?.relativePath

            let auditTargets = uploadedFiles.compactMap { record -> PendingUploadPlan? in
                guard let recordID = record.id else {
                    return nil
                }
                let localFileURL = sourceRootURL.appendingPathComponent(record.relativePath)
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
                continue
            }

            let auditTargetsByRecordID = Dictionary(uniqueKeysWithValues: auditTargets.map { ($0.recordID, $0) })
            var pendingPlansFromBatch: [PendingUploadPlan] = []

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
                        if let pendingPlan = auditTargetsByRecordID[outcome.recordID] {
                            pendingPlansFromBatch.append(pendingPlan)
                        }
                    }
                }
            )

            if !pendingPlansFromBatch.isEmpty {
                let orderedPendingPlans = pendingPlansFromBatch.sorted { lhs, rhs in
                    lhs.relativePath.localizedStandardCompare(rhs.relativePath) == .orderedAscending
                }
                try await onPendingPlansBatch?(orderedPendingPlans)
            }
        }
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

    @MainActor
    private static func applyProgressSnapshot(
        _ snapshot: UploadProgressSnapshot,
        to job: BackupJob
    ) {
        let nextFilesTotal = max(job.filesTotal, max(snapshot.filesTotal, 0))
        let nextBytesTotal = max(job.bytesTotal, max(snapshot.bytesTotal, 0))
        job.filesTotal = nextFilesTotal
        job.bytesTotal = nextBytesTotal

        applyMonotonicProgress(
            filesUploaded: snapshot.filesUploaded,
            bytesUploaded: snapshot.bytesUploaded,
            to: job
        )

        if nextFilesTotal > 0 || snapshot.filesUploaded > 0 {
            job.status = .uploading
        }
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
