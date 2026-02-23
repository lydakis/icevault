import Foundation
import XCTest

final class SecurityHardeningTests: XCTestCase {
    func testBuildScriptDoesNotEnableGlobalArbitraryLoads() throws {
        let scriptURL = repositoryRootURL()
            .appendingPathComponent("scripts", isDirectory: true)
            .appendingPathComponent("build-app.sh")
        let scriptContents = try String(contentsOf: scriptURL, encoding: .utf8)

        XCTAssertFalse(
            scriptContents.contains("NSAllowsArbitraryLoads"),
            "Packaging script should not disable ATS globally."
        )
    }
}
