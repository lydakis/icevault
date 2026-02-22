import Foundation

enum BackupEngineError: Error {
    case notImplemented
}

final class BackupEngine: @unchecked Sendable {
    private let scanner: FileScanner
    private let database: DatabaseService?

    init(
        scanner: FileScanner = FileScanner(),
        database: DatabaseService? = try? DatabaseService()
    ) {
        self.scanner = scanner
        self.database = database
    }

    func run(job: BackupJob, settings: AppState.Settings) async throws {
        _ = scanner
        _ = database
        _ = job
        _ = settings

        // TODO: Scan source directory and diff against local FileRecord inventory.
        // TODO: Persist scan results and determine pending files for upload.
        // TODO: Upload pending files with GlacierClient, updating BackupJob progress.
        // TODO: Support resume-safe behavior by marking uploaded files incrementally.
        throw BackupEngineError.notImplemented
    }
}
