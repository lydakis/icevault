import Foundation
import protocol ClientRuntime.HTTPError

enum BackupEngineError: LocalizedError {
    case invalidSettings(String)
    case databaseUnavailable(String)
    case missingFileRecordID(String)
    case uploadsDeferred(pendingCount: Int, firstErrorDescription: String?)

    var errorDescription: String? {
        switch self {
        case .invalidSettings(let message):
            return message
        case .databaseUnavailable(let message):
            return "Backup database unavailable: \(message)"
        case .missingFileRecordID(let relativePath):
            return "Missing database identifier for pending file: \(relativePath)"
        case .uploadsDeferred(let pendingCount, let firstErrorDescription):
            let normalizedPendingCount = max(pendingCount, 0)
            guard let firstErrorDescription, !firstErrorDescription.isEmpty else {
                return "Backup finished scanning, but \(normalizedPendingCount) file(s) remain pending after upload failures."
            }
            return "Backup finished scanning, but \(normalizedPendingCount) file(s) remain pending after upload failures. First error: \(firstErrorDescription)"
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
    private static let discoveryProgressUpdateBatchSize = 128
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

    /// Tracks recoverable upload-loop failures so the run can complete scanning and report pending work.
    private actor DeferredUploadFailureTracker {
        private var failureCount = 0
        private var firstErrorDescription: String?

        func record(_ error: Error) {
            failureCount += 1
            if firstErrorDescription == nil {
                firstErrorDescription = error.localizedDescription
            }
        }

        func summary() -> (failureCount: Int, firstErrorDescription: String?)? {
            guard failureCount > 0 else {
                return nil
            }
            return (failureCount, firstErrorDescription)
        }
    }

    /// Captures non-deferable upload errors that can be hidden by first-failure semantics.
    private final class UploadBatchFailureSignal: @unchecked Sendable {
        private let stateLock = NSLock()
        private var firstNonDeferableError: Error?

        func record(_ error: Error, isDeferable: Bool) {
            guard !isDeferable, !(error is CancellationError) else {
                return
            }

            stateLock.lock()
            if firstNonDeferableError == nil {
                firstNonDeferableError = error
            }
            stateLock.unlock()
        }

        func nonDeferableError() -> Error? {
            stateLock.lock()
            let recordedError = firstNonDeferableError
            stateLock.unlock()
            return recordedError
        }
    }

    /// Tracks per-batch completion so failed upload attempts can release in-flight progress state.
    private actor CompletedPlanTracker {
        private var completedRecordIDs: Set<Int64> = []

        func markCompleted(recordID: Int64) {
            completedRecordIDs.insert(recordID)
        }

        func incompleteRecordIDs(from plans: [PendingUploadPlan]) -> [Int64] {
            plans
                .map(\.recordID)
                .filter { !completedRecordIDs.contains($0) }
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
        private struct InFlightProgress: Sendable {
            let fileSize: Int64
            var uploadedBytes: Int64
        }

        private var totalFiles: Int = 0
        private var totalBytes: Int64 = 0
        private var completedFiles: Int = 0
        private var completedBytes: Int64 = 0
        private var trackedFileSizeByRecordID: [Int64: Int64] = [:]
        // Keep only active uploads in memory so long-running backups stay bounded.
        private var inFlightByRecordID: [Int64: InFlightProgress] = [:]
        private var inFlightBytesTotal: Int64 = 0

        func register(plans: [PendingUploadPlan]) -> UploadProgressSnapshot {
            for plan in plans {
                let normalizedFileSize = max(plan.fileSize, 0)
                if trackedFileSizeByRecordID[plan.recordID] == nil {
                    trackedFileSizeByRecordID[plan.recordID] = normalizedFileSize
                    totalFiles += 1
                    totalBytes += normalizedFileSize
                }

                if let existingProgress = inFlightByRecordID[plan.recordID] {
                    inFlightBytesTotal -= existingProgress.uploadedBytes
                }

                inFlightByRecordID[plan.recordID] = InFlightProgress(
                    fileSize: normalizedFileSize,
                    uploadedBytes: 0
                )
            }
            return snapshot()
        }

        func update(recordID: Int64, uploadedBytes: Int64) -> UploadProgressSnapshot {
            guard var inFlightProgress = inFlightByRecordID[recordID] else {
                return snapshot()
            }

            let boundedProgress = min(max(uploadedBytes, 0), inFlightProgress.fileSize)
            let nextProgress = max(inFlightProgress.uploadedBytes, boundedProgress)
            inFlightBytesTotal += nextProgress - inFlightProgress.uploadedBytes
            inFlightProgress.uploadedBytes = nextProgress
            inFlightByRecordID[recordID] = inFlightProgress
            return snapshot()
        }

        func markCompleted(recordID: Int64) -> UploadProgressSnapshot {
            guard let inFlightProgress = inFlightByRecordID.removeValue(forKey: recordID) else {
                return snapshot()
            }

            inFlightBytesTotal -= inFlightProgress.uploadedBytes
            completedFiles += 1
            completedBytes += inFlightProgress.fileSize
            return snapshot()
        }

        func markFailed(recordID: Int64) -> UploadProgressSnapshot {
            guard let inFlightProgress = inFlightByRecordID.removeValue(forKey: recordID) else {
                return snapshot()
            }

            inFlightBytesTotal -= inFlightProgress.uploadedBytes
            return snapshot()
        }

        private func snapshot() -> UploadProgressSnapshot {
            let uploadedBytes = min(totalBytes, max(completedBytes + inFlightBytesTotal, 0))
            return UploadProgressSnapshot(
                filesTotal: totalFiles,
                bytesTotal: totalBytes,
                filesUploaded: min(totalFiles, completedFiles),
                bytesUploaded: uploadedBytes
            )
        }
    }

    private actor UploadThroughputTracker {
        private var uploadedBytesByRecordID: [Int64: Int64] = [:]
        private var activeUploadRecordIDs: Set<Int64> = []
        private var totalTransferredBytes: Int64 = 0
        private var lastTimestamp: Date?
        private var lastTransferredBytes: Int64 = 0
        private var smoothedBytesPerSecond: Double = 0

        func observe(
            recordID: Int64,
            uploadedBytesForRecord: Int64,
            at timestamp: Date = Date()
        ) -> Double {
            let normalizedUploadedBytes = max(uploadedBytesForRecord, 0)
            let previousUploadedBytes = uploadedBytesByRecordID[recordID]
            activeUploadRecordIDs.insert(recordID)

            if let previousUploadedBytes {
                let transferredDelta = max(0, normalizedUploadedBytes - previousUploadedBytes)
                totalTransferredBytes += transferredDelta
                uploadedBytesByRecordID[recordID] = max(previousUploadedBytes, normalizedUploadedBytes)
            } else {
                // The first progress callback can include resumed bytes from a prior run.
                uploadedBytesByRecordID[recordID] = normalizedUploadedBytes
            }

            guard let lastTimestamp else {
                self.lastTimestamp = timestamp
                lastTransferredBytes = totalTransferredBytes
                return 0
            }

            let elapsed = timestamp.timeIntervalSince(lastTimestamp)
            guard elapsed > 0 else {
                return smoothedBytesPerSecond
            }

            let transferredDelta = max(0, totalTransferredBytes - lastTransferredBytes)
            let instantaneousBytesPerSecond = Double(transferredDelta) / elapsed
            if smoothedBytesPerSecond <= 0 {
                smoothedBytesPerSecond = instantaneousBytesPerSecond
            } else {
                smoothedBytesPerSecond = (smoothedBytesPerSecond * 0.8) + (instantaneousBytesPerSecond * 0.2)
            }

            self.lastTimestamp = timestamp
            lastTransferredBytes = totalTransferredBytes
            return smoothedBytesPerSecond
        }

        func markCompleted(recordID: Int64) -> Double {
            finishActiveUpload(recordID: recordID)
        }

        func markFailed(recordID: Int64) -> Double {
            finishActiveUpload(recordID: recordID)
        }

        private func finishActiveUpload(recordID: Int64) -> Double {
            activeUploadRecordIDs.remove(recordID)
            uploadedBytesByRecordID.removeValue(forKey: recordID)

            guard activeUploadRecordIDs.isEmpty else {
                return smoothedBytesPerSecond
            }

            smoothedBytesPerSecond = 0
            lastTimestamp = nil
            lastTransferredBytes = totalTransferredBytes
            return smoothedBytesPerSecond
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
                job.discoveredFiles = 0
                job.discoveredBytes = 0
                job.setDiscoveryEstimate(fileCount: nil, byteCount: nil)
                job.uploadBytesPerSecond = 0
                job.isScanInProgress = true
                job.resetDeferredUploadTelemetry()
            }

            try Task.checkCancellation()
            let sourceRootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true)
            let discoveryEstimate: FileScanner.InventoryStats?
            do {
                discoveryEstimate = try scanner.inventoryStats(
                    sourceRoot: sourceRoot,
                    abortCheck: { try Task.checkCancellation() }
                )
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                discoveryEstimate = nil
            }
            if let discoveryEstimate {
                await MainActor.run {
                    job.setDiscoveryEstimate(
                        fileCount: discoveryEstimate.fileCount,
                        byteCount: discoveryEstimate.totalBytes
                    )
                }
            }

            let progressTracker = UploadProgressTracker()
            let uploadThroughputTracker = UploadThroughputTracker()
            let remoteAuditUploadedBefore = Date()

            let scanToken = try database.beginScan(for: sourceRoot)
            let pendingPlanChannel = makePendingPlanStreamChannel()
            let maxBufferedPendingPlans = Self.resolvedMaxBufferedPendingPlans(settings: settings)
            let pendingPlanBackpressureGate = PendingPlanBackpressureGate(
                maxBufferedPlans: maxBufferedPendingPlans
            )
            let uploadLoopFailureSignal = UploadLoopFailureSignal()
            let deferredUploadFailureTracker = DeferredUploadFailureTracker()

            let uploadLoopTask = Task {
                do {
                    try await self.uploadPendingPlansFromStream(
                        pendingPlanStream: pendingPlanChannel.stream,
                        sourceRoot: sourceRoot,
                        sourceRootURL: sourceRootURL,
                        bucket: bucket,
                        settings: settings,
                        database: database,
                        glacierClient: glacierClient,
                        job: job,
                        progressTracker: progressTracker,
                        throughputTracker: uploadThroughputTracker,
                        backpressureGate: pendingPlanBackpressureGate,
                        deferredFailureTracker: deferredUploadFailureTracker
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
                        var discoveredFileCount = 0
                        var discoveredByteCount: Int64 = 0
                        let initialFlushBatchSize = max(1, min(Self.scanSyncBatchSize, settings.maxConcurrentFileUploads))

                        try await scanner.scanMetadata(sourceRoot: sourceRoot) { record in
                            try Task.checkCancellation()
                            try uploadLoopFailureSignal.throwIfFailed()
                            discoveredFileCount += 1
                            discoveredByteCount += max(record.fileSize, 0)
                            if discoveredFileCount % Self.discoveryProgressUpdateBatchSize == 0 {
                                await MainActor.run {
                                    job.markDiscovered(
                                        fileCount: discoveredFileCount,
                                        byteCount: discoveredByteCount
                                    )
                                }
                            }
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
                        await MainActor.run {
                            job.markDiscovered(fileCount: discoveredFileCount, byteCount: discoveredByteCount)
                            job.markScanCompleted()
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
                        try Task.checkCancellation()
                        try uploadLoopFailureSignal.throwIfFailed()
                        pendingPlanChannel.continuation.finish()
                        try await uploadLoopTask.value
                        if let deferredSummary = await deferredUploadFailureTracker.summary() {
                            let pendingCount = try database.pendingFileCount(for: sourceRoot)
                            if pendingCount > 0 {
                                await MainActor.run {
                                    job.setDeferredUploadPendingFiles(pendingCount)
                                    job.markDeferredRetryPassCompleted()
                                }
                                throw BackupEngineError.uploadsDeferred(
                                    pendingCount: pendingCount,
                                    firstErrorDescription: deferredSummary.firstErrorDescription
                                )
                            }
                        }
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
        return try makeUploadPlans(
            pendingFiles: pendingFiles,
            sourceRootURL: sourceRootURL,
            database: database
        )
    }

    private func uploadPendingPlansFromStream(
        pendingPlanStream: AsyncThrowingStream<[PendingUploadPlan], Error>,
        sourceRoot: String,
        sourceRootURL: URL,
        bucket: String,
        settings: AppState.Settings,
        database: DatabaseService,
        glacierClient: GlacierClient,
        job: BackupJob,
        progressTracker: UploadProgressTracker,
        throughputTracker: UploadThroughputTracker,
        backpressureGate: PendingPlanBackpressureGate,
        deferredFailureTracker: DeferredUploadFailureTracker
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
                    progressTracker: progressTracker,
                    throughputTracker: throughputTracker
                )
                await backpressureGate.release(slots: plans.count)
            } catch {
                await backpressureGate.release(slots: plans.count)
                if Self.shouldDeferUploadFailure(error) {
                    await recordDeferredUploadFailure(
                        error,
                        sourceRoot: sourceRoot,
                        database: database,
                        job: job,
                        deferredFailureTracker: deferredFailureTracker
                    )
                    continue
                }
                throw error
            }
        }

        if await deferredFailureTracker.summary() != nil {
            await MainActor.run {
                job.markDeferredRetryPassStarted()
            }
            do {
                try await retryDeferredPendingUploads(
                    sourceRoot: sourceRoot,
                    sourceRootURL: sourceRootURL,
                    bucket: bucket,
                    settings: settings,
                    database: database,
                    glacierClient: glacierClient,
                    job: job,
                    progressTracker: progressTracker,
                    throughputTracker: throughputTracker,
                    deferredFailureTracker: deferredFailureTracker
                )
                await refreshDeferredPendingCount(
                    sourceRoot: sourceRoot,
                    database: database,
                    job: job
                )
                await MainActor.run {
                    job.markDeferredRetryPassCompleted()
                }
            } catch {
                await MainActor.run {
                    job.markDeferredRetryPassCompleted()
                }
                throw error
            }
        }
    }

    private func retryDeferredPendingUploads(
        sourceRoot: String,
        sourceRootURL: URL,
        bucket: String,
        settings: AppState.Settings,
        database: DatabaseService,
        glacierClient: GlacierClient,
        job: BackupJob,
        progressTracker: UploadProgressTracker,
        throughputTracker: UploadThroughputTracker,
        deferredFailureTracker: DeferredUploadFailureTracker
    ) async throws {
        var lastRelativePath: String?

        while true {
            try Task.checkCancellation()
            let pendingFiles = try database.pendingFiles(
                for: sourceRoot,
                afterRelativePath: lastRelativePath,
                limit: Self.remoteAuditBatchSize
            )

            guard !pendingFiles.isEmpty else {
                return
            }

            let batchFirstRelativePath = pendingFiles.first?.relativePath
            let batchLastRelativePath = pendingFiles.last?.relativePath
            let retryPlans = try makeUploadPlans(
                pendingFiles: pendingFiles,
                sourceRootURL: sourceRootURL,
                database: database
            )

            guard !retryPlans.isEmpty else {
                lastRelativePath = batchLastRelativePath
                continue
            }

            do {
                try await uploadPendingFiles(
                    plans: retryPlans,
                    bucket: bucket,
                    settings: settings,
                    database: database,
                    glacierClient: glacierClient,
                    job: job,
                    progressTracker: progressTracker,
                    throughputTracker: throughputTracker
                )
                await refreshDeferredPendingCount(
                    sourceRoot: sourceRoot,
                    database: database,
                    job: job
                )
                lastRelativePath = batchLastRelativePath
            } catch {
                if Self.shouldDeferUploadFailure(error) {
                    await recordDeferredUploadFailure(
                        error,
                        sourceRoot: sourceRoot,
                        database: database,
                        job: job,
                        deferredFailureTracker: deferredFailureTracker
                    )
                    let pendingPlansAfterFailure = try pendingRetryPlansStillPending(
                        retryPlans,
                        database: database
                    )
                    guard !pendingPlansAfterFailure.isEmpty else {
                        lastRelativePath = batchLastRelativePath
                        continue
                    }

                    if let batchFirstRelativePath,
                       pendingPlansAfterFailure.first?.relativePath == batchFirstRelativePath,
                       let firstPendingPlan = pendingPlansAfterFailure.first {
                        do {
                            try await uploadPendingFiles(
                                plans: [firstPendingPlan],
                                bucket: bucket,
                                settings: settings,
                                database: database,
                                glacierClient: glacierClient,
                                job: job,
                                progressTracker: progressTracker,
                                throughputTracker: throughputTracker
                            )
                        } catch {
                            if Self.shouldDeferUploadFailure(error) {
                                await recordDeferredUploadFailure(
                                    error,
                                    sourceRoot: sourceRoot,
                                    database: database,
                                    job: job,
                                    deferredFailureTracker: deferredFailureTracker
                                )
                            } else {
                                throw error
                            }
                        }
                    }

                    // Move past the first path in this batch so a persistent failure does not
                    // starve sibling files that were canceled when concurrency was bounded.
                    lastRelativePath = batchFirstRelativePath
                    continue
                }
                throw error
            }
        }
    }

    private func pendingRetryPlansStillPending(
        _ plans: [PendingUploadPlan],
        database: DatabaseService
    ) throws -> [PendingUploadPlan] {
        var pendingPlans: [PendingUploadPlan] = []
        pendingPlans.reserveCapacity(plans.count)

        for plan in plans {
            guard let storedRecord = try database.fetchFile(id: plan.recordID) else {
                continue
            }
            if storedRecord.uploadedAt == nil {
                pendingPlans.append(plan)
            }
        }

        return pendingPlans
    }

    private func recordDeferredUploadFailure(
        _ error: Error,
        sourceRoot: String,
        database: DatabaseService,
        job: BackupJob,
        deferredFailureTracker: DeferredUploadFailureTracker
    ) async {
        await deferredFailureTracker.record(error)
        let pendingCount = (try? database.pendingFileCount(for: sourceRoot)) ?? 0
        await MainActor.run {
            job.markDeferredUploadFailure(error.localizedDescription, pendingFiles: pendingCount)
        }
    }

    private func refreshDeferredPendingCount(
        sourceRoot: String,
        database: DatabaseService,
        job: BackupJob
    ) async {
        let pendingCount = (try? database.pendingFileCount(for: sourceRoot)) ?? 0
        await MainActor.run {
            job.setDeferredUploadPendingFiles(pendingCount)
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

    private func makeUploadPlans(
        pendingFiles: [FileRecord],
        sourceRootURL: URL,
        database: DatabaseService
    ) throws -> [PendingUploadPlan] {
        let orderedPendingFiles = pendingFiles.sorted { lhs, rhs in
            lhs.relativePath < rhs.relativePath
        }

        var plans: [PendingUploadPlan] = []
        plans.reserveCapacity(orderedPendingFiles.count)

        for record in orderedPendingFiles {
            try Task.checkCancellation()
            guard let recordID = record.id else {
                throw BackupEngineError.missingFileRecordID(record.relativePath)
            }

            let localFileURL = sourceRootURL.appendingPathComponent(record.relativePath)
            guard let resolvedSHA256 = try resolvedPendingPlanSHA256(
                for: record,
                localFileURL: localFileURL,
                database: database
            ) else {
                continue
            }
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
                    sha256: resolvedSHA256
                )
            )
        }

        return plans
    }

    private func resolvedPendingPlanSHA256(
        for record: FileRecord,
        localFileURL: URL,
        database: DatabaseService
    ) throws -> String? {
        let normalizedSHA256 = record.sha256.trimmingCharacters(in: .whitespacesAndNewlines)
        if !normalizedSHA256.isEmpty {
            return normalizedSHA256
        }

        guard fileManager.fileExists(atPath: localFileURL.path) else {
            return nil
        }

        let computedSHA256: String
        do {
            computedSHA256 = try scanner.sha256Hex(for: localFileURL)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            return nil
        }
        guard let recordID = record.id else {
            throw BackupEngineError.missingFileRecordID(record.relativePath)
        }
        try database.updateFileSHA256(id: recordID, sha256: computedSHA256)
        return computedSHA256
    }

    private func uploadPendingFiles(
        plans: [PendingUploadPlan],
        bucket: String,
        settings: AppState.Settings,
        database: DatabaseService,
        glacierClient: GlacierClient,
        job: BackupJob,
        progressTracker: UploadProgressTracker,
        throughputTracker: UploadThroughputTracker
    ) async throws {
        guard !plans.isEmpty else {
            return
        }

        let fileConcurrency = max(1, min(settings.maxConcurrentFileUploads, plans.count))
        let multipartPartConcurrency = max(1, settings.maxConcurrentMultipartPartUploads)
        let completedPlanTracker = CompletedPlanTracker()
        let uploadBatchFailureSignal = UploadBatchFailureSignal()

        let registeredSnapshot = await progressTracker.register(plans: plans)
        await MainActor.run {
            Self.applyProgressSnapshot(registeredSnapshot, to: job)
        }

        do {
            try await BoundedTaskRunner.run(
                inputs: plans,
                maxConcurrentTasks: fileConcurrency,
                operation: { [self] plan in
                    do {
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
                                let uploadBytesPerSecond = await throughputTracker.observe(
                                    recordID: plan.recordID,
                                    uploadedBytesForRecord: uploadedBytesForCurrentFile
                                )
                                await MainActor.run {
                                    Self.applyProgressSnapshot(snapshot, to: job)
                                    job.setUploadRate(bytesPerSecond: uploadBytesPerSecond)
                                }
                            }
                            return plan
                        }
                    } catch {
                        uploadBatchFailureSignal.record(error, isDeferable: Self.shouldDeferUploadFailure(error))
                        throw error
                    }
                },
                onSuccess: { completedPlan in
                    do {
                        // Persist each completion immediately so interrupted runs remain resume-safe.
                        try database.markUploaded(id: completedPlan.recordID, glacierKey: completedPlan.objectKey)

                        let snapshot = await progressTracker.markCompleted(recordID: completedPlan.recordID)
                        let uploadBytesPerSecond = await throughputTracker.markCompleted(recordID: completedPlan.recordID)
                        await MainActor.run {
                            Self.applyProgressSnapshot(snapshot, to: job)
                            job.setUploadRate(bytesPerSecond: uploadBytesPerSecond)
                        }
                        await completedPlanTracker.markCompleted(recordID: completedPlan.recordID)
                    } catch {
                        uploadBatchFailureSignal.record(error, isDeferable: false)
                        throw error
                    }
                }
            )
        } catch {
            let incompleteRecordIDs = await completedPlanTracker.incompleteRecordIDs(from: plans)
            if !incompleteRecordIDs.isEmpty {
                var latestSnapshot: UploadProgressSnapshot?
                var latestUploadRate: Double?
                for recordID in incompleteRecordIDs {
                    latestSnapshot = await progressTracker.markFailed(recordID: recordID)
                    latestUploadRate = await throughputTracker.markFailed(recordID: recordID)
                }

                if let latestSnapshot, let latestUploadRate {
                    await MainActor.run {
                        Self.applyProgressSnapshot(latestSnapshot, to: job)
                        job.setUploadRate(bytesPerSecond: latestUploadRate)
                    }
                }
            }
            if let nonDeferableError = uploadBatchFailureSignal.nonDeferableError() {
                throw nonDeferableError
            }
            throw error
        }
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

    private static func shouldDeferUploadFailure(_ error: Error) -> Bool {
        if error is CancellationError {
            return false
        }

        guard let glacierError = error as? GlacierClientError else {
            return false
        }

        switch glacierError {
        case .s3OperationFailed(_, let underlying):
            if let statusCode = s3StatusCode(from: underlying),
               statusCode == 403 || statusCode == 404 {
                return false
            }
            return true
        case .invalidCredentials:
            return true
        case .invalidRegion,
                .invalidBucket,
                .invalidObjectKey,
                .fileNotFound,
                .unreadableFile,
                .unsupportedStorageClass,
                .missingMultipartUploadID,
                .missingMultipartETag,
                .incompleteFileRead:
            return false
        }
    }

    private static func s3StatusCode(from error: Error) -> Int? {
        (error as? any HTTPError)?.httpResponse.statusCode.rawValue
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
