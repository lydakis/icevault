import Foundation

enum LaunchAgentError: Error {
    case notImplemented
}

final class LaunchAgent {
    func install(label: String, executablePath: String, interval: TimeInterval) throws {
        _ = label
        _ = executablePath
        _ = interval

        // TODO: Write LaunchAgent plist into ~/Library/LaunchAgents.
        // TODO: Load plist with launchctl and validate schedule.
        throw LaunchAgentError.notImplemented
    }

    func uninstall(label: String) throws {
        _ = label

        // TODO: Unload LaunchAgent plist and remove file from disk.
        throw LaunchAgentError.notImplemented
    }

    func isInstalled(label: String) -> Bool {
        _ = label

        // TODO: Check LaunchAgent plist existence and launchctl registration.
        return false
    }
}
