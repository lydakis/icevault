import Foundation
import XCTest
@testable import IceVault

final class AWSCLITests: XCTestCase {
    func testMakeEnvironmentAddsProfileAndFallbackPathWhenMissing() {
        let environment = AWSCLI.makeEnvironment(
            base: [:],
            profileName: " dev ",
            searchDirectories: ["/opt/homebrew/bin", "/usr/bin"]
        )

        XCTAssertEqual(environment["AWS_PROFILE"], "dev")
        XCTAssertEqual(environment["AWS_SDK_LOAD_CONFIG"], "1")
        XCTAssertEqual(environment["PATH"], "/opt/homebrew/bin:/usr/bin")
    }

    func testExecutableURLFindsAWSInFallbackDirectoriesWhenPathMissing() throws {
        let temporaryDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-AWSCLI")
        defer { try? FileManager.default.removeItem(at: temporaryDirectory) }

        let executablePath = try makeFakeAWSCLI(in: temporaryDirectory)
        let environment = AWSCLI.makeEnvironment(
            base: [:],
            searchDirectories: [temporaryDirectory.path]
        )

        XCTAssertEqual(
            AWSCLI.executableURL(
                environment: environment,
                searchDirectories: [temporaryDirectory.path]
            )?.path,
            executablePath
        )
    }

    func testExecutableURLPrefersOverridePath() throws {
        let temporaryDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-AWSCLI-Override")
        defer { try? FileManager.default.removeItem(at: temporaryDirectory) }

        let executablePath = try makeFakeAWSCLI(in: temporaryDirectory)
        let environment = [
            AWSCLI.executableOverrideEnvironmentKey: executablePath
        ]

        XCTAssertEqual(AWSCLI.executableURL(environment: environment)?.path, executablePath)
    }

    private func makeTemporaryDirectory(prefix: String) throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(prefix + "-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func makeFakeAWSCLI(in directory: URL) throws -> String {
        let executableURL = directory.appendingPathComponent("aws")
        let script = """
        #!/bin/sh
        exit 0
        """
        try script.data(using: .utf8)?.write(to: executableURL)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: executableURL.path)
        return executableURL.path
    }
}
