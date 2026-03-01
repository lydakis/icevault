import XCTest
@testable import IceVault

@MainActor
final class MenuBarViewProgressModelTests: XCTestCase {
    func testPrimaryUploadProgressFractionFallsBackToFileProgressWhenJobBytesAreZero() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            status: .uploading,
            filesTotal: 4,
            filesUploaded: 1,
            bytesTotal: 0,
            bytesUploaded: 0
        )

        let fraction = MenuBarViewProgressModel.primaryUploadProgressFraction(
            for: job,
            runningInventoryUploadProgress: nil
        )

        XCTAssertEqual(fraction, 0.25)
    }

    func testPrimaryUploadProgressFractionFallsBackToInventoryFileProgressWhenInventoryBytesAreZero() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            status: .uploading,
            filesTotal: 10,
            filesUploaded: 2,
            bytesTotal: 1_000,
            bytesUploaded: 200
        )
        let inventoryProgress = AppState.InventoryUploadProgress(
            uploadedFiles: 3,
            totalFiles: 10,
            uploadedBytes: 0,
            totalBytes: 0
        )

        let fraction = MenuBarViewProgressModel.primaryUploadProgressFraction(
            for: job,
            runningInventoryUploadProgress: inventoryProgress
        )

        XCTAssertEqual(fraction, 0.3)
    }

    func testEtaTextReturnsCalculatingWhileJobIsRunningAndRemainingBytesAreZero() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            status: .scanning,
            filesTotal: 10,
            filesUploaded: 0,
            bytesTotal: 0,
            bytesUploaded: 0,
            uploadBytesPerSecond: 1_024
        )

        let eta = MenuBarViewProgressModel.etaText(
            for: job,
            runningInventoryUploadProgress: nil
        )

        XCTAssertEqual(eta, "Calculating...")
    }

    func testEtaTextReturnsDoneWhenJobIsNotRunningAndRemainingBytesAreZero() {
        let job = BackupJob(
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            status: .completed,
            filesTotal: 1,
            filesUploaded: 1,
            bytesTotal: 0,
            bytesUploaded: 0
        )

        let eta = MenuBarViewProgressModel.etaText(
            for: job,
            runningInventoryUploadProgress: nil
        )

        XCTAssertEqual(eta, "Done")
    }
}
