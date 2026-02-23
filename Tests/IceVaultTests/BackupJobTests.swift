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

    func testMarkFailedSetsErrorAndCompletionDate() {
        let job = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket")

        job.markFailed("boom")

        XCTAssertEqual(job.status, .failed)
        XCTAssertEqual(job.error, "boom")
        XCTAssertNotNil(job.completedAt)
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
