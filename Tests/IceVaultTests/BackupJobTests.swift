import Foundation
import XCTest
@testable import IceVault

@MainActor
final class BackupJobTests: XCTestCase {
    func testProgressFractionsForEmptyTotalsAndCompletedState() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        XCTAssertEqual(job.fileProgressFraction, 0)
        XCTAssertEqual(job.byteProgressFraction, 0)
        XCTAssertFalse(job.isRunning)

        job.markCompleted()

        XCTAssertEqual(job.fileProgressFraction, 1)
        XCTAssertEqual(job.byteProgressFraction, 1)
    }

    func testPrimaryProgressFractionPrefersDiscoveryEstimateTotals() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            filesTotal: 2,
            filesUploaded: 1,
            bytesTotal: 200,
            bytesUploaded: 100
        )

        job.setDiscoveryEstimate(fileCount: 1_000, byteCount: 4_000)
        job.markDiscovered(fileCount: 250, byteCount: 1_000)

        XCTAssertEqual(job.fileProgressFraction, 0.5)
        XCTAssertEqual(job.primaryProgressFraction, 0.25)
    }

    func testPrimaryProgressFractionFallsBackToUploadProgressWithoutEstimate() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            filesTotal: 10,
            filesUploaded: 4
        )

        XCTAssertEqual(job.primaryProgressFraction, job.fileProgressFraction)
        XCTAssertEqual(job.primaryProgressFraction, 0.4)
    }

    func testSetScanTotalsClampsNegativeValuesAndResetsProgress() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            filesTotal: 10,
            filesUploaded: 3,
            bytesTotal: 100,
            bytesUploaded: 20
        )

        job.setScanTotals(fileCount: -5, byteCount: -100)

        XCTAssertEqual(job.status, .scanning)
        XCTAssertEqual(job.filesTotal, 0)
        XCTAssertEqual(job.bytesTotal, 0)
        XCTAssertEqual(job.filesUploaded, 0)
        XCTAssertEqual(job.bytesUploaded, 0)
    }

    func testMarkUploadedClampsToTotalsAndIgnoresNegativeInputs() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            status: .scanning,
            filesTotal: 2,
            filesUploaded: 1,
            bytesTotal: 10,
            bytesUploaded: 8
        )

        job.markUploaded(fileCount: 10, byteCount: 50)
        XCTAssertEqual(job.status, .uploading)
        XCTAssertEqual(job.filesUploaded, 2)
        XCTAssertEqual(job.bytesUploaded, 10)

        job.markUploaded(fileCount: -5, byteCount: -99)
        XCTAssertEqual(job.filesUploaded, 2)
        XCTAssertEqual(job.bytesUploaded, 10)
    }

    func testMarkDiscoveredIsMonotonicAndClampsNegativeValues() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        job.markDiscovered(fileCount: 4, byteCount: 40)
        XCTAssertEqual(job.discoveredFiles, 4)
        XCTAssertEqual(job.discoveredBytes, 40)

        job.markDiscovered(fileCount: 2, byteCount: 20)
        XCTAssertEqual(job.discoveredFiles, 4)
        XCTAssertEqual(job.discoveredBytes, 40)

        job.markDiscovered(fileCount: -10, byteCount: -99)
        XCTAssertEqual(job.discoveredFiles, 4)
        XCTAssertEqual(job.discoveredBytes, 40)
    }

    func testDiscoveryEstimateTracksPrepassAndStaysAtLeastDiscoveredCounts() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        XCTAssertFalse(job.hasDiscoveryEstimate)

        job.setDiscoveryEstimate(fileCount: 10, byteCount: 100)
        XCTAssertTrue(job.hasDiscoveryEstimate)
        XCTAssertEqual(job.discoveryEstimatedFiles, 10)
        XCTAssertEqual(job.discoveryEstimatedBytes, 100)

        job.markDiscovered(fileCount: 12, byteCount: 120)
        XCTAssertEqual(job.discoveryEstimatedFiles, 12)
        XCTAssertEqual(job.discoveryEstimatedBytes, 120)

        job.setDiscoveryEstimate(fileCount: nil, byteCount: nil)
        XCTAssertFalse(job.hasDiscoveryEstimate)
    }

    func testSetUploadRateClampsNegativeValues() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        job.setUploadRate(bytesPerSecond: 1_024)
        XCTAssertEqual(job.uploadBytesPerSecond, 1_024)

        job.setUploadRate(bytesPerSecond: -10)
        XCTAssertEqual(job.uploadBytesPerSecond, 0)
    }

    func testShouldShowDiscoveryRateWhileScanIsInProgress() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        XCTAssertFalse(job.shouldShowDiscoveryRate)

        job.setScanTotals(fileCount: 1, byteCount: 1)
        XCTAssertTrue(job.shouldShowDiscoveryRate)

        job.status = .uploading
        XCTAssertTrue(job.shouldShowDiscoveryRate)

        job.markCompleted()
        XCTAssertFalse(job.shouldShowDiscoveryRate)
    }

    func testMarkFailedSetsErrorAndCompletionDate() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        job.markFailed("boom")

        XCTAssertEqual(job.status, .failed)
        XCTAssertEqual(job.error, "boom")
        XCTAssertNotNil(job.completedAt)
    }

    func testDeferredUploadTelemetryTracksFailuresPendingCountsAndRetryPasses() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        XCTAssertFalse(job.hasDeferredUploadIssues)

        job.markDeferredUploadFailure("transient", pendingFiles: 3)
        XCTAssertEqual(job.deferredUploadFailureCount, 1)
        XCTAssertEqual(job.deferredUploadPendingFiles, 3)
        XCTAssertEqual(job.deferredUploadLastError, "transient")
        XCTAssertTrue(job.hasDeferredUploadIssues)

        job.markDeferredRetryPassStarted()
        XCTAssertEqual(job.deferredUploadRetryPassCount, 1)
        XCTAssertTrue(job.isRetryingDeferredUploads)

        job.markDeferredRetryPassCompleted()
        XCTAssertFalse(job.isRetryingDeferredUploads)

        job.setDeferredUploadPendingFiles(0)
        XCTAssertEqual(job.deferredUploadPendingFiles, 0)

        job.resetDeferredUploadTelemetry()
        XCTAssertEqual(job.deferredUploadFailureCount, 0)
        XCTAssertEqual(job.deferredUploadPendingFiles, 0)
        XCTAssertEqual(job.deferredUploadRetryPassCount, 0)
        XCTAssertNil(job.deferredUploadLastError)
        XCTAssertFalse(job.isRetryingDeferredUploads)
        XCTAssertFalse(job.hasDeferredUploadIssues)
    }

    func testHistoryEntryClampsNegativeCounts() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            filesUploaded: -10,
            bytesUploaded: -20
        )

        let entry = job.historyEntry()
        XCTAssertEqual(entry.filesUploaded, 0)
        XCTAssertEqual(entry.bytesUploaded, 0)
    }

    func testHistoryEntryPreservesDeferredUploadTelemetry() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            filesUploaded: 2,
            bytesUploaded: 20
        )
        job.markDeferredUploadFailure("timeout", pendingFiles: 4)
        job.markDeferredRetryPassStarted()
        job.markDeferredRetryPassCompleted()

        let entry = job.historyEntry()
        XCTAssertEqual(entry.deferredUploadFailureCount, 1)
        XCTAssertEqual(entry.deferredUploadPendingFiles, 4)
        XCTAssertEqual(entry.deferredUploadRetryPassCount, 1)
        XCTAssertEqual(entry.deferredUploadLastError, "timeout")
    }

    func testBackupHistoryEntryDecodesLegacyFileCountAndClampsNegativeValues() throws {
        let id = UUID(uuidString: "F9A9F7E5-1E8A-4A08-BBB5-C2C542C8B1A8")!
        let startedAt = Date(timeIntervalSince1970: 1_700_000_000)
        let json = """
        {
          "id": "\(id.uuidString)",
          "startedAt": \(startedAt.timeIntervalSince1970),
          "completedAt": null,
          "fileCount": -4,
          "bytesUploaded": -55,
          "status": "completed",
          "sourceRoot": "/tmp/source",
          "bucket": "bucket"
        }
        """

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .secondsSince1970
        let entry = try decoder.decode(BackupHistoryEntry.self, from: Data(json.utf8))

        XCTAssertEqual(entry.filesUploaded, 0)
        XCTAssertEqual(entry.bytesUploaded, 0)
        XCTAssertEqual(entry.fileCount, 0)
        XCTAssertEqual(entry.deferredUploadFailureCount, 0)
        XCTAssertEqual(entry.deferredUploadPendingFiles, 0)
        XCTAssertEqual(entry.deferredUploadRetryPassCount, 0)
        XCTAssertNil(entry.deferredUploadLastError)
    }

    func testBackupHistoryEntryDurationNeverNegative() {
        let startedAt = Date(timeIntervalSince1970: 1_700_000_100)
        let completedAt = Date(timeIntervalSince1970: 1_700_000_000)

        let entry = BackupHistoryEntry(
            id: UUID(),
            startedAt: startedAt,
            completedAt: completedAt,
            filesUploaded: 1,
            bytesUploaded: 1,
            status: .completed,
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            error: nil
        )

        XCTAssertEqual(entry.duration, 0)
        XCTAssertEqual(entry.displayDate, completedAt)
    }
}
