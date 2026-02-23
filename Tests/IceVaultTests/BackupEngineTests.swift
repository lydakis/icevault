import Foundation
import XCTest
@testable import IceVault

final class BackupEngineTests: XCTestCase {
    func testRunFailsWhenDatabaseInitializationFailed() async throws {
        enum TestError: Error {
            case databaseFailed
            case shouldNotCreateGlacierClient
        }

        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: nil,
            databaseFactory: {
                throw TestError.databaseFailed
            },
            glacierClientFactory: { _, _, _, _, _ in
                throw TestError.shouldNotCreateGlacierClient
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "test-access-key",
            awsSecretKey: "test-secret-key",
            awsRegion: "us-east-1",
            bucket: "test-bucket",
            sourcePath: sourceRoot.path
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "test-bucket")
        }

        do {
            try await backupEngine.run(job: job, settings: settings)
            XCTFail("Expected database initialization failure")
        } catch let error as BackupEngineError {
            switch error {
            case .databaseUnavailable(let message):
                XCTAssertFalse(message.isEmpty)
            default:
                XCTFail("Unexpected backup engine error: \(error)")
            }
        } catch {
            XCTFail("Unexpected error type: \(error)")
        }

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
            XCTAssertNotNil(job.error)
        }
    }

    private func makeTempDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-Engine-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }
}
