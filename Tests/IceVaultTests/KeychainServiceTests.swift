import Foundation
import XCTest
@testable import IceVault

final class KeychainServiceTests: XCTestCase {
    func testSaveTrimsAndLoadReturnsCredentials() throws {
        let store = InMemoryKeychainStore()
        let service = KeychainService(keychain: store)

        try service.save(accessKey: "  access-key  ", secretKey: "  secret-key  ")
        let credentials = try service.loadCredentials()

        XCTAssertEqual(credentials?.accessKey, "access-key")
        XCTAssertEqual(credentials?.secretKey, "secret-key")
    }

    func testLoadReturnsNilWhenNoStoredCredentials() throws {
        let service = KeychainService(keychain: InMemoryKeychainStore())
        XCTAssertNil(try service.loadCredentials())
    }

    func testLoadThrowsWhenOnlyOneCredentialExists() throws {
        let store = InMemoryKeychainStore(values: ["aws.access_key": "access-only"])
        let service = KeychainService(keychain: store)

        XCTAssertThrowsError(try service.loadCredentials()) { error in
            XCTAssertEqual(error as? KeychainServiceError, .incompleteCredentials)
        }
    }

    func testDeleteCredentialsRemovesStoredValues() throws {
        let store = InMemoryKeychainStore(
            values: [
                "aws.access_key": "access",
                "aws.secret_key": "secret"
            ]
        )
        let service = KeychainService(keychain: store)

        try service.deleteCredentials()
        XCTAssertNil(try service.loadCredentials())
    }

    func testSaveRejectsEmptyCredentials() throws {
        let service = KeychainService(keychain: InMemoryKeychainStore())

        XCTAssertThrowsError(try service.save(accessKey: " ", secretKey: "secret")) { error in
            XCTAssertEqual(error as? KeychainServiceError, .incompleteCredentials)
        }
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
