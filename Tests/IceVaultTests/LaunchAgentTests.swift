import Foundation
import XCTest
@testable import IceVault

final class LaunchAgentTests: XCTestCase {
    func testInstallWritesPlistAndRunsBootoutThenBootstrap() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-LaunchAgent")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let runner = LaunchctlRunnerRecorder()
        let launchAgent = LaunchAgent(fileManager: fileManager, launchctlRunner: runner.run)

        try launchAgent.install(
            label: "com.icevault.test",
            executablePath: "/usr/bin/true",
            interval: .daily(hour: 99, minute: -10)
        )

        let plistURL = home
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("LaunchAgents", isDirectory: true)
            .appendingPathComponent("com.icevault.test.plist")

        let plist = try loadPlistDictionary(at: plistURL)
        let programArguments = try XCTUnwrap(plist["ProgramArguments"] as? [String])
        XCTAssertEqual(programArguments, ["/usr/bin/true", "--backup"])

        let startCalendarInterval = try XCTUnwrap(plist["StartCalendarInterval"] as? [String: Any])
        XCTAssertEqual((startCalendarInterval["Hour"] as? NSNumber)?.intValue, 23)
        XCTAssertEqual((startCalendarInterval["Minute"] as? NSNumber)?.intValue, 0)

        XCTAssertEqual(runner.calls.count, 2)
        XCTAssertEqual(runner.calls[0].arguments.first, "bootout")
        XCTAssertTrue(runner.calls[0].allowFailure)
        XCTAssertEqual(runner.calls[1].arguments.first, "bootstrap")
        XCTAssertFalse(runner.calls[1].allowFailure)
    }

    func testInstallCustomHoursSetsStartInterval() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-LaunchAgent")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let launchAgent = LaunchAgent(fileManager: fileManager, launchctlRunner: { _, _ in 0 })

        try launchAgent.install(
            label: "com.icevault.custom",
            executablePath: "/usr/bin/true",
            interval: .customHours(6)
        )

        let plistURL = home
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("LaunchAgents", isDirectory: true)
            .appendingPathComponent("com.icevault.custom.plist")

        let plist = try loadPlistDictionary(at: plistURL)
        XCTAssertEqual((plist["StartInterval"] as? NSNumber)?.intValue, 21_600)
    }

    func testUninstallRemovesPlistAndRunsBootout() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-LaunchAgent")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let runner = LaunchctlRunnerRecorder()
        let launchAgent = LaunchAgent(fileManager: fileManager, launchctlRunner: runner.run)

        let plistDirectory = home
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("LaunchAgents", isDirectory: true)
        try FileManager.default.createDirectory(at: plistDirectory, withIntermediateDirectories: true)
        let plistURL = plistDirectory.appendingPathComponent("com.icevault.remove.plist")
        try Data("plist".utf8).write(to: plistURL)

        try launchAgent.uninstall(label: "com.icevault.remove")

        XCTAssertFalse(FileManager.default.fileExists(atPath: plistURL.path))
        XCTAssertEqual(runner.calls.count, 1)
        XCTAssertEqual(runner.calls[0].arguments.first, "bootout")
        XCTAssertTrue(runner.calls[0].allowFailure)
    }

    func testIsInstalledReflectsLaunchctlPrintStatus() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-LaunchAgent")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let runner = LaunchctlRunnerRecorder()
        runner.exitCodeBySubcommand["print"] = 0
        let launchAgent = LaunchAgent(fileManager: fileManager, launchctlRunner: runner.run)

        XCTAssertFalse(launchAgent.isInstalled(label: "com.icevault.missing"))

        let plistDirectory = home
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("LaunchAgents", isDirectory: true)
        try FileManager.default.createDirectory(at: plistDirectory, withIntermediateDirectories: true)
        let plistURL = plistDirectory.appendingPathComponent("com.icevault.present.plist")
        try Data("plist".utf8).write(to: plistURL)

        XCTAssertTrue(launchAgent.isInstalled(label: "com.icevault.present"))

        runner.exitCodeBySubcommand["print"] = 1
        XCTAssertFalse(launchAgent.isInstalled(label: "com.icevault.present"))
    }

    private func loadPlistDictionary(at url: URL) throws -> [String: Any] {
        let data = try Data(contentsOf: url)
        let plist = try PropertyListSerialization.propertyList(from: data, format: nil)
        guard let dictionary = plist as? [String: Any] else {
            XCTFail("Expected plist dictionary")
            return [:]
        }
        return dictionary
    }
}

private final class LaunchctlRunnerRecorder {
    struct Call {
        let arguments: [String]
        let allowFailure: Bool
    }

    var calls: [Call] = []
    var exitCodeBySubcommand: [String: Int32] = [:]

    func run(arguments: [String], allowFailure: Bool) throws -> Int32 {
        calls.append(Call(arguments: arguments, allowFailure: allowFailure))
        let subcommand = arguments.first ?? ""
        return exitCodeBySubcommand[subcommand] ?? 0
    }
}
