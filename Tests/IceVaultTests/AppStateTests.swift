import AWSS3
import Foundation
import XCTest
@testable import IceVault

@MainActor
final class AppStateTests: XCTestCase {
    func testUpdateSettingsTrimsValuesAndClampsCustomInterval() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let appState = makeAppState(userDefaults: userDefaults)

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "  us-east-1  ",
                authenticationMethod: .staticKeys,
                bucket: "  bucket-name ",
                sourcePath: "  /tmp/source ",
                scheduledBackupsEnabled: true,
                scheduleInterval: .customHours,
                customIntervalHours: 999,
                maxConcurrentFileUploads: 999,
                maxConcurrentMultipartPartUploads: 0,
                maxBufferedPendingPlans: 999_999
            )
        )

        XCTAssertEqual(appState.settings.awsRegion, "us-east-1")
        XCTAssertEqual(appState.settings.bucket, "bucket-name")
        XCTAssertEqual(appState.settings.sourcePath, "/tmp/source")
        XCTAssertEqual(appState.settings.customIntervalHours, 168)
        XCTAssertEqual(appState.settings.maxConcurrentFileUploads, AppState.Settings.maximumConcurrentFileUploads)
        XCTAssertEqual(appState.settings.maxConcurrentMultipartPartUploads, AppState.Settings.minimumUploadConcurrency)
        XCTAssertEqual(appState.settings.maxBufferedPendingPlans, AppState.Settings.maximumBufferedPendingPlans)
    }

    func testApplyScheduledBackupsInstallsThenUninstalls() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-Home")
        defer { try? FileManager.default.removeItem(at: home) }

        let runner = LaunchctlCallRecorder()
        let launchAgent = LaunchAgent(
            fileManager: TestHomeFileManager(homeDirectory: home),
            launchctlRunner: runner.run
        )
        let appState = makeAppState(userDefaults: userDefaults, launchAgent: launchAgent)

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                bucket: "bucket",
                sourcePath: "/tmp/source",
                scheduledBackupsEnabled: true,
                scheduleInterval: .daily
            )
        )

        let installed = try appState.applyScheduledBackups()
        XCTAssertTrue(installed)
        XCTAssertTrue(runner.calls.contains(where: { $0.arguments.first == "bootstrap" }))

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                bucket: "bucket",
                sourcePath: "/tmp/source",
                scheduledBackupsEnabled: false
            )
        )
        let uninstalled = try appState.applyScheduledBackups()
        XCTAssertFalse(uninstalled)
        XCTAssertTrue(runner.calls.contains(where: { $0.arguments.first == "bootout" }))
    }

    func testStartBackupBlocksWhenSSOProfileMissing() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-SSO")
        defer { try? FileManager.default.removeItem(at: home) }

        let ssoMonitor = SSOTokenMonitor(
            fileManager: TestHomeFileManager(homeDirectory: home),
            userDefaults: userDefaults,
            autoStart: false
        )
        let appState = makeAppState(userDefaults: userDefaults, ssoTokenMonitor: ssoMonitor)

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                authenticationMethod: .ssoProfile,
                ssoProfileName: "dev",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )

        appState.startBackup()

        XCTAssertNil(appState.currentJob)
        XCTAssertNotNil(appState.authenticationPromptMessage)
        XCTAssertTrue(appState.authenticationPromptMessage?.contains("not found") == true)
    }

    func testStartManualBackupCompletesAndStoresHistory() async throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let sourceRoot = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-Source")
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("file.txt")
        let payload = Data("app-state-backup".utf8)
        try payload.write(to: fileURL)

        let databaseDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-DB")
        let database = try DatabaseService(databaseURL: databaseDirectory.appendingPathComponent("icevault.sqlite"))
        let mockS3 = MockAppStateS3Client()

        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: database,
            glacierClientFactory: { _, _, _, _, db in
                GlacierClient(s3Client: mockS3, database: db)
            }
        )

        let keychainStore = InMemoryKeychainStore(
            values: [
                "aws.access_key": "access",
                "aws.secret_key": "secret"
            ]
        )
        let keychainService = KeychainService(keychain: keychainStore)

        let appState = AppState(
            userDefaults: userDefaults,
            backupEngine: backupEngine,
            keychainService: keychainService,
            launchAgent: LaunchAgent(launchctlRunner: { _, _ in 0 }),
            ssoTokenMonitor: SSOTokenMonitor(userDefaults: userDefaults, autoStart: false)
        )

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                bucket: "bucket",
                sourcePath: sourceRoot.path
            )
        )

        await appState.startManualBackup()

        XCTAssertNil(appState.currentJob)
        XCTAssertNil(appState.authenticationPromptMessage)
        XCTAssertEqual(appState.history.count, 1)
        XCTAssertEqual(appState.history.first?.status, .completed)
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "file.txt")
        XCTAssertEqual(appState.credentialSource, .keychain)
    }

    func testSettingsCodableStripsCredentialsAndExposesDisplayMetadata() throws {
        XCTAssertEqual(AppState.Settings.AuthenticationMethod.staticKeys.id, "staticKeys")
        XCTAssertEqual(AppState.Settings.AuthenticationMethod.staticKeys.displayName, "Static Keys")
        XCTAssertEqual(AppState.Settings.AuthenticationMethod.ssoProfile.id, "ssoProfile")
        XCTAssertEqual(AppState.Settings.AuthenticationMethod.ssoProfile.displayName, "SSO Profile")
        XCTAssertEqual(AppState.Settings.ScheduleInterval.daily.id, "daily")
        XCTAssertEqual(AppState.Settings.ScheduleInterval.daily.displayName, "Daily")
        XCTAssertEqual(AppState.Settings.ScheduleInterval.weekly.displayName, "Weekly")
        XCTAssertEqual(AppState.Settings.ScheduleInterval.customHours.displayName, "Custom (Hours)")

        let settings = AppState.Settings(
            awsAccessKey: "AKIA_TEST",
            awsSecretKey: "secret",
            awsSessionToken: "session",
            awsRegion: "us-west-2",
            authenticationMethod: .ssoProfile,
            ssoProfileName: "dev",
            bucket: "bucket",
            sourcePath: "/tmp/source",
            scheduledBackupsEnabled: true,
            scheduleInterval: .customHours,
            customIntervalHours: 0,
            includeHiddenFiles: true,
            maxConcurrentFileUploads: 0,
            maxConcurrentMultipartPartUploads: 999,
            maxBufferedPendingPlans: 1
        )

        let encoded = try JSONEncoder().encode(settings)
        let decoded = try JSONDecoder().decode(AppState.Settings.self, from: encoded)

        XCTAssertEqual(decoded.awsAccessKey, "")
        XCTAssertEqual(decoded.awsSecretKey, "")
        XCTAssertEqual(decoded.awsSessionToken, "")
        XCTAssertEqual(decoded.authenticationMethod, .ssoProfile)
        XCTAssertEqual(decoded.ssoProfileName, "dev")
        XCTAssertEqual(decoded.bucket, "bucket")
        XCTAssertEqual(decoded.sourcePath, "/tmp/source")
        XCTAssertEqual(decoded.customIntervalHours, 1)
        XCTAssertTrue(decoded.includeHiddenFiles)
        XCTAssertEqual(decoded.maxConcurrentFileUploads, AppState.Settings.minimumUploadConcurrency)
        XCTAssertEqual(
            decoded.maxConcurrentMultipartPartUploads,
            AppState.Settings.maximumConcurrentMultipartPartUploads
        )
        XCTAssertEqual(decoded.maxBufferedPendingPlans, AppState.Settings.minimumBufferedPendingPlans)
    }

    func testStatusAndMenuBarReflectConfigurationHistoryAndCurrentJobState() {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let keychainService = KeychainService(
            keychain: InMemoryKeychainStore(
                values: [
                    "aws.access_key": "access",
                    "aws.secret_key": "secret"
                ]
            )
        )

        let appState = AppState(
            userDefaults: userDefaults,
            backupEngine: BackupEngine(
                scanner: FileScanner(),
                fileManager: .default,
                database: try? DatabaseService(
                    databaseURL: FileManager.default.temporaryDirectory
                        .appendingPathComponent("icevault-\(UUID().uuidString).sqlite")
                )
            ),
            keychainService: keychainService,
            launchAgent: LaunchAgent(launchctlRunner: { _, _ in 0 }),
            ssoTokenMonitor: SSOTokenMonitor(userDefaults: userDefaults, autoStart: false)
        )
        XCTAssertFalse(appState.isConfigured)
        XCTAssertEqual(appState.statusText, "Not Configured")
        XCTAssertEqual(appState.statusBadgeText, "SETUP")

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )

        XCTAssertTrue(appState.isConfigured)
        XCTAssertEqual(appState.statusText, "Idle")
        XCTAssertEqual(appState.statusBadgeText, "READY")
        XCTAssertEqual(appState.menuBarSystemImage, "archivebox")
        XCTAssertNil(appState.lastBackupDate)
        XCTAssertEqual(appState.idleStatusText, "Ready to back up")

        let historyEntry = BackupHistoryEntry(
            id: UUID(),
            startedAt: Date(timeIntervalSince1970: 1_700_000_000),
            completedAt: Date(timeIntervalSince1970: 1_700_000_300),
            filesUploaded: 1,
            bytesUploaded: 10,
            status: .completed,
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            error: nil
        )
        appState.history = [historyEntry]
        XCTAssertEqual(appState.statusText, "Idle")
        XCTAssertEqual(appState.statusBadgeText, "HEALTHY")
        XCTAssertEqual(appState.idleStatusText, "All backed up ✓")
        XCTAssertEqual(appState.lastBackupDate, historyEntry.displayDate)

        let failedHistoryEntry = BackupHistoryEntry(
            id: UUID(),
            startedAt: Date(timeIntervalSince1970: 1_700_001_000),
            completedAt: Date(timeIntervalSince1970: 1_700_001_100),
            filesUploaded: 0,
            bytesUploaded: 0,
            status: .failed,
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            error: "network down"
        )
        appState.history = [failedHistoryEntry]
        XCTAssertEqual(appState.statusText, "Idle")
        XCTAssertEqual(appState.statusBadgeText, "ATTENTION")
        XCTAssertEqual(appState.idleStatusText, "Last backup failed")
        XCTAssertNil(appState.lastBackupDate)

        let deferredFailedEntry = BackupHistoryEntry(
            id: UUID(),
            startedAt: Date(timeIntervalSince1970: 1_700_002_000),
            completedAt: Date(timeIntervalSince1970: 1_700_002_200),
            filesUploaded: 10,
            bytesUploaded: 10_000,
            status: .failed,
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            error: "upload deferred",
            deferredUploadFailureCount: 3,
            deferredUploadPendingFiles: 7,
            deferredUploadRetryPassCount: 1,
            deferredUploadLastError: "S3 putObject failed: timeout"
        )
        appState.history = [deferredFailedEntry]
        XCTAssertEqual(appState.statusText, "Idle")
        XCTAssertEqual(appState.statusBadgeText, "RETRY")
        XCTAssertEqual(appState.idleStatusText, "7 pending uploads")
        XCTAssertTrue(appState.hasPendingDeferredUploads)
        XCTAssertEqual(appState.latestDeferredUploadPendingFiles, 7)
        XCTAssertTrue(appState.latestDeferredUploadStatusText?.contains("7 file(s) still pending upload") == true)
        XCTAssertTrue(appState.latestDeferredUploadStatusText?.contains("Last error:") == true)
        XCTAssertNil(appState.lastBackupDate)

        let expectedImages: [(BackupJob.Status, String)] = [
            (.idle, "archivebox"),
            (.scanning, "magnifyingglass.circle"),
            (.uploading, "icloud.and.arrow.up"),
            (.completed, "checkmark.circle"),
            (.failed, "exclamationmark.triangle")
        ]
        for (status, expectedImage) in expectedImages {
            appState.currentJob = BackupJob(sourceRoot: "/tmp/source", bucket: "bucket", status: status)
            XCTAssertEqual(appState.statusText, status.displayName)
            XCTAssertEqual(appState.menuBarSystemImage, expectedImage)
        }
    }

    func testSaveAndDeleteCredentialsUpdatesCredentialState() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let keychainService = KeychainService(keychain: InMemoryKeychainStore())
        let appState = AppState(
            userDefaults: userDefaults,
            backupEngine: BackupEngine(
                scanner: FileScanner(),
                fileManager: .default,
                database: try? DatabaseService(
                    databaseURL: FileManager.default.temporaryDirectory
                        .appendingPathComponent("icevault-\(UUID().uuidString).sqlite")
                )
            ),
            keychainService: keychainService,
            launchAgent: LaunchAgent(launchctlRunner: { _, _ in 0 }),
            ssoTokenMonitor: SSOTokenMonitor(userDefaults: userDefaults, autoStart: false)
        )

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )
        XCTAssertFalse(appState.hasAvailableCredentials)
        XCTAssertNil(appState.credentialSource)

        try appState.saveCredentials(accessKey: "  access-key  ", secretKey: "  secret-key  ")
        XCTAssertTrue(appState.hasAvailableCredentials)
        XCTAssertEqual(appState.credentialSource, .keychain)

        let loaded = try appState.loadStoredCredentials()
        XCTAssertEqual(loaded?.accessKey, "access-key")
        XCTAssertEqual(loaded?.secretKey, "secret-key")

        try appState.deleteStoredCredentials()
        XCTAssertFalse(appState.hasAvailableCredentials)
        XCTAssertNil(appState.credentialSource)
        XCTAssertNil(try appState.loadStoredCredentials())
    }

    func testSSOSessionStateTextAndBackupBlockReasonUpdateFromMonitor() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-SSOState")
        defer { try? FileManager.default.removeItem(at: home) }

        try writeSSOProfile(named: "dev", startURL: "https://example.awsapps.com/start", in: home)

        let monitor = SSOTokenMonitor(
            fileManager: TestHomeFileManager(homeDirectory: home),
            userDefaults: userDefaults,
            autoStart: false
        )
        let appState = makeAppState(userDefaults: userDefaults, ssoTokenMonitor: monitor)
        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                authenticationMethod: .ssoProfile,
                ssoProfileName: "dev",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )

        let now = Date(timeIntervalSince1970: 1_700_000_000)
        monitor.refreshNow(now: now)
        XCTAssertEqual(appState.ssoSessionStatusText, "Session missing - refresh login")

        try writeSSOTokenCache(
            startURL: "https://example.awsapps.com/start",
            expiresAt: "2020-01-01T00:00:00Z",
            at: home
        )
        monitor.refreshNow(now: now)
        XCTAssertTrue(appState.isSSOSessionExpired)
        XCTAssertTrue(appState.isBackupBlockedBySSOExpiry)
        XCTAssertTrue(appState.backupBlockedReason?.contains("Backup paused: SSO session expired at") == true)
        XCTAssertEqual(appState.ssoTokenStatus(), appState.ssoSessionStatus)

        try writeSSOTokenCache(
            startURL: "https://example.awsapps.com/start",
            expiresAt: "2099-01-01T00:00:00Z",
            at: home
        )
        monitor.refreshNow(now: now)
        XCTAssertFalse(appState.isSSOSessionExpired)
        XCTAssertFalse(appState.isBackupBlockedBySSOExpiry)
        XCTAssertNil(appState.backupBlockedReason)
        XCTAssertTrue(appState.ssoSessionStatusText?.contains("Session valid (expires") == true)
    }

    func testStartBackupBlocksWhenSSOTokenMissingAndClearDismissHelpers() throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-SSOMissingToken")
        defer { try? FileManager.default.removeItem(at: home) }

        try writeSSOProfile(named: "dev", startURL: "https://example.awsapps.com/start", in: home)

        let monitor = SSOTokenMonitor(
            fileManager: TestHomeFileManager(homeDirectory: home),
            userDefaults: userDefaults,
            autoStart: false
        )
        let appState = makeAppState(userDefaults: userDefaults, ssoTokenMonitor: monitor)
        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                authenticationMethod: .ssoProfile,
                ssoProfileName: "dev",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )

        let historyEntry = BackupHistoryEntry(
            id: UUID(),
            startedAt: Date(),
            completedAt: Date(),
            filesUploaded: 1,
            bytesUploaded: 1,
            status: .completed,
            sourceRoot: "/tmp/source",
            bucket: "bucket",
            error: nil
        )
        appState.history = [historyEntry]

        appState.startBackup()

        XCTAssertNil(appState.currentJob)
        XCTAssertTrue(appState.authenticationPromptMessage?.contains("aws sso login --profile dev") == true)
        XCTAssertEqual(appState.ssoSessionStatusText, "Session missing - refresh login")
        XCTAssertTrue(appState.canRefreshSSOLogin)

        appState.clearHistory()
        XCTAssertTrue(appState.history.isEmpty)

        appState.dismissAuthenticationPrompt()
        XCTAssertNil(appState.authenticationPromptMessage)
    }

    func testRefreshSSOLoginRequiresProfileAndInvokesRunnerWhenAvailable() async throws {
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AppState-SSORefresh")
        defer { try? FileManager.default.removeItem(at: home) }

        let recorder = AppStateLoginRunnerRecorder()
        let monitor = SSOTokenMonitor(
            fileManager: TestHomeFileManager(homeDirectory: home),
            userDefaults: userDefaults,
            autoStart: false,
            ssoLoginRunner: recorder.run
        )
        let appState = makeAppState(userDefaults: userDefaults, ssoTokenMonitor: monitor)

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                authenticationMethod: .ssoProfile,
                ssoProfileName: " ",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )
        XCTAssertFalse(appState.canRefreshSSOLogin)
        appState.refreshSSOLogin()
        XCTAssertEqual(recorder.count, 0)

        appState.updateSettings(
            AppState.Settings(
                awsRegion: "us-east-1",
                authenticationMethod: .ssoProfile,
                ssoProfileName: "dev",
                bucket: "bucket",
                sourcePath: "/tmp/source"
            )
        )
        XCTAssertTrue(appState.canRefreshSSOLogin)
        appState.refreshSSOLogin()

        for _ in 0..<100 {
            if recorder.count == 1 {
                break
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTAssertEqual(recorder.count, 1)
        XCTAssertEqual(recorder.profiles, ["dev"])
    }

    private func makeAppState(
        userDefaults: UserDefaults,
        launchAgent: LaunchAgent = LaunchAgent(launchctlRunner: { _, _ in 0 }),
        ssoTokenMonitor: SSOTokenMonitor? = nil
    ) -> AppState {
        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: try? DatabaseService(databaseURL: FileManager.default.temporaryDirectory.appendingPathComponent("icevault-\(UUID().uuidString).sqlite"))
        )
        let keychain = KeychainService(keychain: InMemoryKeychainStore())
        return AppState(
            userDefaults: userDefaults,
            backupEngine: backupEngine,
            keychainService: keychain,
            launchAgent: launchAgent,
            ssoTokenMonitor: ssoTokenMonitor
        )
    }

    private func makeUserDefaults() -> (UserDefaults, String) {
        let suiteName = "IceVaultTests-AppState-\(UUID().uuidString)"
        return (UserDefaults(suiteName: suiteName) ?? .standard, suiteName)
    }

    private func clearUserDefaults(_ userDefaults: UserDefaults, suiteName: String) {
        userDefaults.removePersistentDomain(forName: suiteName)
    }

    private func writeSSOProfile(named profileName: String, startURL: String, in home: URL) throws {
        let awsDirectory = home.appendingPathComponent(".aws", isDirectory: true)
        try FileManager.default.createDirectory(at: awsDirectory, withIntermediateDirectories: true)

        let configContents = """
        [profile \(profileName)]
        sso_account_id = 123456789012
        sso_role_name = Admin
        sso_start_url = \(startURL)
        sso_region = us-east-1
        region = us-east-1
        """
        try configContents.data(using: .utf8)?
            .write(to: awsDirectory.appendingPathComponent("config"))
    }

    private func writeSSOTokenCache(startURL: String, expiresAt: String, at home: URL) throws {
        let cacheDirectory = home
            .appendingPathComponent(".aws", isDirectory: true)
            .appendingPathComponent("sso", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)
        try FileManager.default.createDirectory(at: cacheDirectory, withIntermediateDirectories: true)

        let tokenContents = """
        {
          "startUrl": "\(startURL)",
          "accessToken": "token",
          "expiresAt": "\(expiresAt)"
        }
        """
        try tokenContents.data(using: .utf8)?
            .write(to: cacheDirectory.appendingPathComponent("token.json"))
    }
}

private final class InMemoryKeychainStore: KeychainStore {
    private var values: [String: String]

    init(values: [String: String] = [:]) {
        self.values = values
    }

    func set(_ value: String, key: String) throws {
        values[key] = value
    }

    func get(_ key: String) throws -> String? {
        values[key]
    }

    func remove(_ key: String) throws {
        values.removeValue(forKey: key)
    }
}

private final class LaunchctlCallRecorder {
    struct Call {
        let arguments: [String]
    }

    var calls: [Call] = []

    func run(arguments: [String], allowFailure: Bool) throws -> Int32 {
        _ = allowFailure
        calls.append(Call(arguments: arguments))
        return 0
    }
}

private final class MockAppStateS3Client: GlacierS3Client {
    var putObjectInputs: [PutObjectInput] = []

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        putObjectInputs.append(input)
        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

private final class AppStateLoginRunnerRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private(set) var profiles: [String] = []

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return profiles.count
    }

    func run(profileName: String) -> Int32 {
        lock.lock()
        profiles.append(profileName)
        lock.unlock()

        Thread.sleep(forTimeInterval: 0.2)
        return -1
    }
}
