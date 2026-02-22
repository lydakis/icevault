import Foundation
import KeychainAccess

struct AWSCredentials: Equatable, Sendable {
    let accessKey: String
    let secretKey: String
    let sessionToken: String?
    let expiration: Date?

    init(
        accessKey: String,
        secretKey: String,
        sessionToken: String? = nil,
        expiration: Date? = nil
    ) {
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.sessionToken = sessionToken
        self.expiration = expiration
    }
}

enum KeychainServiceError: LocalizedError {
    case incompleteCredentials

    var errorDescription: String? {
        switch self {
        case .incompleteCredentials:
            return "Both AWS access key and secret key are required."
        }
    }
}

final class KeychainService {
    private enum Keys {
        static let accessKey = "aws.access_key"
        static let secretKey = "aws.secret_key"
    }

    private let keychain: Keychain

    init(service: String = KeychainService.defaultServiceName) {
        self.keychain = Keychain(service: service)
            .accessibility(.whenUnlocked)
    }

    func save(accessKey: String, secretKey: String) throws {
        let trimmedAccessKey = accessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedSecretKey = secretKey.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !trimmedAccessKey.isEmpty, !trimmedSecretKey.isEmpty else {
            throw KeychainServiceError.incompleteCredentials
        }

        try keychain.set(trimmedAccessKey, key: Keys.accessKey)
        try keychain.set(trimmedSecretKey, key: Keys.secretKey)
    }

    func loadCredentials() throws -> AWSCredentials? {
        let accessKey = try keychain.get(Keys.accessKey)?.trimmingCharacters(in: .whitespacesAndNewlines)
        let secretKey = try keychain.get(Keys.secretKey)?.trimmingCharacters(in: .whitespacesAndNewlines)

        let normalizedAccessKey = accessKey ?? ""
        let normalizedSecretKey = secretKey ?? ""

        if normalizedAccessKey.isEmpty && normalizedSecretKey.isEmpty {
            return nil
        }

        guard !normalizedAccessKey.isEmpty, !normalizedSecretKey.isEmpty else {
            throw KeychainServiceError.incompleteCredentials
        }

        return AWSCredentials(
            accessKey: normalizedAccessKey,
            secretKey: normalizedSecretKey
        )
    }

    func deleteCredentials() throws {
        try keychain.remove(Keys.accessKey)
        try keychain.remove(Keys.secretKey)
    }

    static var defaultServiceName: String {
        if let bundleIdentifier = Bundle.main.bundleIdentifier, !bundleIdentifier.isEmpty {
            return "\(bundleIdentifier).credentials"
        }
        return "com.icevault.credentials"
    }
}
