import Foundation

struct ConcurrentCallSnapshot: Sendable {
    let callCount: Int
    let maxInFlight: Int
}

actor ConcurrentCallTracker {
    private var inFlight = 0
    private var maxInFlight = 0
    private var totalCalls = 0

    func startCall() {
        totalCalls += 1
        inFlight += 1
        maxInFlight = max(maxInFlight, inFlight)
    }

    func finishCall() {
        inFlight = max(inFlight - 1, 0)
    }

    func snapshot() -> ConcurrentCallSnapshot {
        ConcurrentCallSnapshot(callCount: totalCalls, maxInFlight: maxInFlight)
    }
}

actor InvocationCounter {
    private var total = 0

    func increment() {
        total += 1
    }

    func incrementAndGet() -> Int {
        total += 1
        return total
    }

    func value() -> Int {
        total
    }
}

actor AsyncSignal {
    private var signaled = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func signal() {
        guard !signaled else {
            return
        }
        signaled = true
        let pendingWaiters = waiters
        waiters.removeAll()
        for waiter in pendingWaiters {
            waiter.resume()
        }
    }

    func wait() async {
        guard !signaled else {
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

actor ValueCollector<Value: Sendable> {
    private var values: [Value] = []

    func append(_ value: Value) {
        values.append(value)
    }

    func allValues() -> [Value] {
        values
    }
}
