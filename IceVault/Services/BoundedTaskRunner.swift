import Foundation

enum BoundedTaskRunner {
    static func run<Input: Sendable, Output: Sendable>(
        inputs: [Input],
        maxConcurrentTasks: Int,
        operation: @escaping @Sendable (Input) async throws -> Output,
        onSuccess: @escaping (Output) async throws -> Void
    ) async throws {
        guard !inputs.isEmpty else {
            return
        }

        let concurrency = max(1, min(maxConcurrentTasks, inputs.count))
        try await withThrowingTaskGroup(of: Output.self) { group in
            var nextInputIndex = 0
            var completedInputCount = 0
            var firstError: Error?

            func scheduleTask(for input: Input) {
                group.addTask {
                    try Task.checkCancellation()
                    return try await operation(input)
                }
            }

            for _ in 0..<concurrency {
                guard nextInputIndex < inputs.count else {
                    break
                }
                scheduleTask(for: inputs[nextInputIndex])
                nextInputIndex += 1
            }

            while let result = await group.nextResult() {
                switch result {
                case .success(let output):
                    do {
                        try await onSuccess(output)
                        completedInputCount += 1
                    } catch {
                        if firstError == nil {
                            firstError = error
                            group.cancelAll()
                        }
                    }

                case .failure(let error):
                    if firstError == nil {
                        firstError = error
                        group.cancelAll()
                    }
                }

                if firstError == nil {
                    let hasUnscheduledInputs = nextInputIndex < inputs.count
                    if Task.isCancelled, hasUnscheduledInputs {
                        firstError = CancellationError()
                        group.cancelAll()
                    } else if hasUnscheduledInputs {
                        scheduleTask(for: inputs[nextInputIndex])
                        nextInputIndex += 1
                    }
                }
            }

            if let firstError {
                throw firstError
            }
        }
    }
}
