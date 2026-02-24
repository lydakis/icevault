import Foundation

enum AWSCLI {
    static let executableOverrideEnvironmentKey = "ICEVAULT_AWS_CLI_PATH"

    private static let defaultSearchDirectories: [String] = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin"
    ]

    static func makeEnvironment(
        base: [String: String] = ProcessInfo.processInfo.environment,
        profileName: String? = nil,
        searchDirectories: [String] = defaultSearchDirectories
    ) -> [String: String] {
        var environment = base
        environment["PATH"] = mergedPath(existingPath: base["PATH"], searchDirectories: searchDirectories)
        if let profileName {
            let normalizedProfileName = trimmed(profileName)
            if !normalizedProfileName.isEmpty {
                environment["AWS_PROFILE"] = normalizedProfileName
            }
        }
        environment["AWS_SDK_LOAD_CONFIG"] = "1"
        return environment
    }

    static func executableURL(
        environment: [String: String],
        fileManager: FileManager = .default,
        searchDirectories: [String] = defaultSearchDirectories
    ) -> URL? {
        if let overridePath = nonEmpty(environment[executableOverrideEnvironmentKey]),
           isExecutable(atPath: overridePath, fileManager: fileManager)
        {
            return URL(fileURLWithPath: overridePath)
        }

        let pathDirectories = splitPath(environment["PATH"])
        for directory in deduplicated(pathDirectories + searchDirectories) {
            let candidatePath = URL(fileURLWithPath: directory, isDirectory: true)
                .appendingPathComponent("aws")
                .path
            if isExecutable(atPath: candidatePath, fileManager: fileManager) {
                return URL(fileURLWithPath: candidatePath)
            }
        }

        return nil
    }

    private static func mergedPath(existingPath: String?, searchDirectories: [String]) -> String {
        let components = deduplicated(splitPath(existingPath) + searchDirectories)
        return components.joined(separator: ":")
    }

    private static func splitPath(_ path: String?) -> [String] {
        guard let path else {
            return []
        }

        return path
            .split(separator: ":", omittingEmptySubsequences: true)
            .map(String.init)
            .map(trimmed)
            .filter { !$0.isEmpty }
    }

    private static func deduplicated(_ values: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []

        for value in values.map(trimmed) where !value.isEmpty {
            if seen.insert(value).inserted {
                result.append(value)
            }
        }

        return result
    }

    private static func isExecutable(atPath path: String, fileManager: FileManager) -> Bool {
        fileManager.isExecutableFile(atPath: path)
    }

    private static func nonEmpty(_ value: String?) -> String? {
        let normalizedValue = trimmed(value ?? "")
        return normalizedValue.isEmpty ? nil : normalizedValue
    }

    private static func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
