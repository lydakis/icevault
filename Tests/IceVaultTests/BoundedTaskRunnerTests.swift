import Foundation
import XCTest
@testable import IceVault

final class BoundedTaskRunnerTests: XCTestCase {
    func testRunProcessesAllInputsWithBoundedConcurrency() async throws {
        let tracker = ConcurrentCallTracker()
        let collector = ValueCollector<Int>()

        try await BoundedTaskRunner.run(
            inputs: Array(0..<6),
            maxConcurrentTasks: 2,
            operation: { value in
                await tracker.startCall()
                do {
                    try await Task.sleep(nanoseconds: 80_000_000)
                    await tracker.finishCall()
                    return value
                } catch {
                    await tracker.finishCall()
                    throw error
                }
            },
            onSuccess: { output in
                await collector.append(output)
            }
        )

        let completedOutputs = await collector.allValues()
        XCTAssertEqual(completedOutputs.count, 6)
        XCTAssertEqual(Set(completedOutputs), Set(0..<6))
        let snapshot = await tracker.snapshot()
        XCTAssertEqual(snapshot.maxInFlight, 2)
    }

    func testRunDoesNotFailForLateCancellationAfterFinalResult() async throws {
        let finalInputStartedSignal = AsyncSignal()
        let releaseFinalInputSignal = AsyncSignal()
        let collector = ValueCollector<Int>()

        let task = Task {
            try await BoundedTaskRunner.run(
                inputs: [1, 2],
                maxConcurrentTasks: 1,
                operation: { value in
                    if value == 2 {
                        await finalInputStartedSignal.signal()
                        await releaseFinalInputSignal.wait()
                    }
                    return value
                },
                onSuccess: { output in
                    await collector.append(output)
                }
            )
        }

        await finalInputStartedSignal.wait()
        task.cancel()
        await releaseFinalInputSignal.signal()

        do {
            try await task.value
        } catch {
            XCTFail("Expected late cancellation to preserve success, got \(error)")
        }

        let completedOutputs = await collector.allValues()
        XCTAssertEqual(completedOutputs, [1, 2])
    }

    func testRunDoesNotFailForLateCancellationAfterAllInputsAlreadyScheduled() async throws {
        let firstInputStartedSignal = AsyncSignal()
        let secondInputStartedSignal = AsyncSignal()
        let releaseInputsSignal = AsyncSignal()
        let collector = ValueCollector<Int>()

        let task = Task {
            try await BoundedTaskRunner.run(
                inputs: [1, 2],
                maxConcurrentTasks: 2,
                operation: { value in
                    if value == 1 {
                        await firstInputStartedSignal.signal()
                    } else if value == 2 {
                        await secondInputStartedSignal.signal()
                    }
                    await releaseInputsSignal.wait()
                    return value
                },
                onSuccess: { output in
                    await collector.append(output)
                }
            )
        }

        await firstInputStartedSignal.wait()
        await secondInputStartedSignal.wait()
        task.cancel()
        await releaseInputsSignal.signal()

        do {
            try await task.value
        } catch {
            XCTFail("Expected late cancellation to preserve success, got \(error)")
        }

        let completedOutputs = await collector.allValues()
        XCTAssertEqual(completedOutputs.count, 2)
        XCTAssertEqual(Set(completedOutputs), Set([1, 2]))
    }
}
