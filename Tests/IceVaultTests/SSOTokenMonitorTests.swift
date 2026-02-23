import Foundation
import XCTest
@testable import IceVault

@MainActor
final class SSOTokenMonitorTests: XCTestCase {
    func testRefreshNowUsesSSOTokenCacheAndReportsValidStatus() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-SSOMonitor")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        try writeSSOProfile(named: "dev", startURL: "https://example.awsapps.com/start", in: home)
        let expiry = try XCTUnwrap(ISO8601DateFormatter().date(from: "2027-01-15T00:00:00Z"))
        try writeSSOTokenCache(
            startURL: "https://example.awsapps.com/start",
            expiresAt: "2027-01-15T00:00:00Z",
            at: home
        )

        let monitor = SSOTokenMonitor(
            fileManager: fileManager,
            userDefaults: userDefaults,
            autoStart: false
        )
        monitor.configure(profileName: "dev")
        monitor.refreshNow(now: Date(timeIntervalSince1970: 1_700_000_000))

        guard case .valid(let expiresAt) = monitor.status else {
            return XCTFail("Expected valid SSO status, got \(String(describing: monitor.status))")
        }
        XCTAssertEqual(Int(expiresAt.timeIntervalSince1970), Int(expiry.timeIntervalSince1970))
        XCTAssertEqual(Int(monitor.sessionExpiresAt()?.timeIntervalSince1970 ?? 0), Int(expiry.timeIntervalSince1970))
        XCTAssertTrue(monitor.isSessionValid(now: Date(timeIntervalSince1970: 1_700_000_000)))
    }

    func testRefreshNowFallsBackToCLICacheWhenSSOTokenMissing() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-SSOMonitor")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        try writeSSOProfile(named: "dev", startURL: "https://example.awsapps.com/start", in: home)
        try writeCLICache(
            startURL: "https://example.awsapps.com/start",
            expiration: "2030-01-01T00:00:00Z",
            at: home
        )

        let monitor = SSOTokenMonitor(
            fileManager: fileManager,
            userDefaults: userDefaults,
            autoStart: false
        )
        monitor.configure(profileName: "dev")
        monitor.refreshNow(now: Date(timeIntervalSince1970: 1_700_000_000))

        guard case .valid = monitor.status else {
            return XCTFail("Expected valid status from CLI cache fallback, got \(String(describing: monitor.status))")
        }
    }

    func testRefreshLoginUsesInjectedRunnerAndDeduplicatesConcurrentRequests() async throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-SSOMonitor")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let (userDefaults, suiteName) = makeUserDefaults()
        defer { clearUserDefaults(userDefaults, suiteName: suiteName) }

        let recorder = LoginRunnerRecorder()
        let monitor = SSOTokenMonitor(
            fileManager: fileManager,
            userDefaults: userDefaults,
            autoStart: false,
            ssoLoginRunner: recorder.run
        )

        monitor.configure(profileName: "dev")
        monitor.refreshLogin()
        monitor.refreshLogin()

        for _ in 0..<100 {
            if recorder.count == 1 {
                break
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        XCTAssertEqual(recorder.count, 1)
        XCTAssertEqual(recorder.profiles, ["dev"])
    }

    private func makeUserDefaults() -> (UserDefaults, String) {
        let suiteName = "IceVaultTests-SSOMonitor-\(UUID().uuidString)"
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

    private func writeCLICache(startURL: String, expiration: String, at home: URL) throws {
        let cacheDirectory = home
            .appendingPathComponent(".aws", isDirectory: true)
            .appendingPathComponent("cli", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)
        try FileManager.default.createDirectory(at: cacheDirectory, withIntermediateDirectories: true)

        let cacheContents = """
        {
          "ProviderType": "sso",
          "Credentials": {
            "AccessKeyId": "AKIAEXAMPLE",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": "\(expiration)"
          },
          "StartUrl": "\(startURL)"
        }
        """
        try cacheContents.data(using: .utf8)?
            .write(to: cacheDirectory.appendingPathComponent("credentials.json"))
    }
}

private final class LoginRunnerRecorder: @unchecked Sendable {
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
