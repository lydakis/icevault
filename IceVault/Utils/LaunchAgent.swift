import Darwin
import Foundation

enum LaunchAgentError: LocalizedError {
    case invalidLabel
    case invalidExecutablePath
    case failedToWritePlist(Error)
    case failedToCreateDirectory(Error)
    case launchctlFailed(command: String, exitCode: Int32, output: String)

    var errorDescription: String? {
        switch self {
        case .invalidLabel:
            return "LaunchAgent label is required."
        case .invalidExecutablePath:
            return "Unable to resolve the IceVault executable path."
        case .failedToWritePlist(let error):
            return "Failed to write LaunchAgent plist: \(error.localizedDescription)"
        case .failedToCreateDirectory(let error):
            return "Failed to create LaunchAgent directory: \(error.localizedDescription)"
        case .launchctlFailed(let command, let exitCode, let output):
            if output.isEmpty {
                return "\(command) failed with exit code \(exitCode)."
            }
            return "\(command) failed with exit code \(exitCode): \(output)"
        }
    }
}

final class LaunchAgent {
    enum ScheduleInterval: Equatable, Sendable {
        case daily(hour: Int = 3, minute: Int = 0)
        case weekly(weekday: Int = 1, hour: Int = 3, minute: Int = 0)
        case customHours(Int)
    }

    private let fileManager: FileManager

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    func install(label: String, executablePath: String, interval: ScheduleInterval) throws {
        let normalizedLabel = trimmed(label)
        guard !normalizedLabel.isEmpty else {
            throw LaunchAgentError.invalidLabel
        }

        let normalizedExecutablePath = trimmed(executablePath)
        guard !normalizedExecutablePath.isEmpty else {
            throw LaunchAgentError.invalidExecutablePath
        }

        let launchAgentURL = try plistURL(for: normalizedLabel)
        let logFilePath = try logFileURL().path

        let plist = makePlist(
            label: normalizedLabel,
            executablePath: normalizedExecutablePath,
            logFilePath: logFilePath,
            interval: interval
        )

        do {
            let plistData = try PropertyListSerialization.data(
                fromPropertyList: plist,
                format: .xml,
                options: 0
            )
            try plistData.write(to: launchAgentURL, options: .atomic)
        } catch {
            throw LaunchAgentError.failedToWritePlist(error)
        }

        // Ensure old definitions are removed before loading an updated schedule.
        _ = try runLaunchctl(
            ["bootout", launchDomain, launchAgentURL.path],
            allowFailure: true
        )

        _ = try runLaunchctl(
            ["bootstrap", launchDomain, launchAgentURL.path]
        )
    }

    func uninstall(label: String) throws {
        let normalizedLabel = trimmed(label)
        guard !normalizedLabel.isEmpty else {
            throw LaunchAgentError.invalidLabel
        }

        let launchAgentURL = try plistURL(for: normalizedLabel)

        _ = try runLaunchctl(
            ["bootout", launchDomain, launchAgentURL.path],
            allowFailure: true
        )

        if fileManager.fileExists(atPath: launchAgentURL.path) {
            try fileManager.removeItem(at: launchAgentURL)
        }
    }

    func isInstalled(label: String) -> Bool {
        let normalizedLabel = trimmed(label)
        guard !normalizedLabel.isEmpty else {
            return false
        }

        guard let launchAgentURL = try? plistURL(for: normalizedLabel) else {
            return false
        }

        guard fileManager.fileExists(atPath: launchAgentURL.path) else {
            return false
        }

        guard
            let status = try? runLaunchctl(
                ["print", "\(launchDomain)/\(normalizedLabel)"],
                allowFailure: true
            )
        else {
            return false
        }
        return status == 0
    }

    private var launchDomain: String {
        "gui/\(getuid())"
    }

    private func plistURL(for label: String) throws -> URL {
        let directory = fileManager.homeDirectoryForCurrentUser
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("LaunchAgents", isDirectory: true)

        do {
            try fileManager.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
        } catch {
            throw LaunchAgentError.failedToCreateDirectory(error)
        }

        return directory.appendingPathComponent("\(label).plist")
    }

    private func logFileURL() throws -> URL {
        let directory = fileManager.homeDirectoryForCurrentUser
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("Logs", isDirectory: true)

        do {
            try fileManager.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
        } catch {
            throw LaunchAgentError.failedToCreateDirectory(error)
        }

        return directory.appendingPathComponent("IceVault.launchagent.log")
    }

    private func makePlist(
        label: String,
        executablePath: String,
        logFilePath: String,
        interval: ScheduleInterval
    ) -> [String: Any] {
        var plist: [String: Any] = [
            "Label": label,
            "ProgramArguments": [executablePath, "--backup"],
            "RunAtLoad": false,
            "StandardOutPath": logFilePath,
            "StandardErrorPath": logFilePath
        ]

        switch interval {
        case .daily(let hour, let minute):
            plist["StartCalendarInterval"] = [
                "Hour": max(0, min(23, hour)),
                "Minute": max(0, min(59, minute))
            ]
        case .weekly(let weekday, let hour, let minute):
            plist["StartCalendarInterval"] = [
                "Weekday": max(1, min(7, weekday)),
                "Hour": max(0, min(23, hour)),
                "Minute": max(0, min(59, minute))
            ]
        case .customHours(let hours):
            plist["StartInterval"] = max(1, hours) * 3600
        }

        return plist
    }

    @discardableResult
    private func runLaunchctl(_ arguments: [String], allowFailure: Bool = false) throws -> Int32 {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        process.arguments = arguments

        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe

        try process.run()
        process.waitUntilExit()

        let outputData = pipe.fileHandleForReading.readDataToEndOfFile()
        let output = String(data: outputData, encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let exitCode = process.terminationStatus

        if exitCode != 0 && !allowFailure {
            throw LaunchAgentError.launchctlFailed(
                command: "launchctl \(arguments.joined(separator: " "))",
                exitCode: exitCode,
                output: output
            )
        }

        return exitCode
    }

    private func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
