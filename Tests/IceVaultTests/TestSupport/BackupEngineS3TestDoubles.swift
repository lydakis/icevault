import Foundation
import AWSS3
import protocol ClientRuntime.HTTPError
import class SmithyHTTPAPI.HTTPResponse
import enum SmithyHTTPAPI.HTTPStatusCode
@testable import IceVault

final class MockBackupEngineS3Client: GlacierS3Client {
    var putObjectInputs: [PutObjectInput] = []
    var headObjectInputs: [HeadObjectInput] = []
    var headObjectOutputsByKey: [String: HeadObjectOutput] = [:]
    var headObjectErrorsByKey: [String: Error] = [:]
    var defaultHeadObjectOutput = HeadObjectOutput()
    var defaultHeadObjectError: Error?

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        putObjectInputs.append(input)
        return PutObjectOutput()
    }

    func headObject(input: HeadObjectInput) async throws -> HeadObjectOutput {
        headObjectInputs.append(input)
        let key = input.key ?? ""
        if let error = headObjectErrorsByKey[key] {
            throw error
        }
        if let output = headObjectOutputsByKey[key] {
            return output
        }
        if let defaultHeadObjectError {
            throw defaultHeadObjectError
        }
        return defaultHeadObjectOutput
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class FinalUploadGatedBackupEngineS3Client: GlacierS3Client {
    private let gatedKey: String
    private let finalUploadStartedSignal = AsyncSignal()
    private let releaseFinalUploadSignal = AsyncSignal()
    private let putObjectCallCounter = InvocationCounter()

    init(gatedKey: String) {
        self.gatedKey = gatedKey
    }

    func waitUntilFinalUploadStarts() async {
        await finalUploadStartedSignal.wait()
    }

    func releaseFinalUpload() async {
        await releaseFinalUploadSignal.signal()
    }

    func putObjectCallCount() async -> Int {
        await putObjectCallCounter.value()
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        await putObjectCallCounter.increment()
        if input.key == gatedKey {
            await finalUploadStartedSignal.signal()
            await releaseFinalUploadSignal.wait()
        }
        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class ConcurrencyTrackingBackupEngineS3Client: GlacierS3Client {
    private let putObjectDelayNanoseconds: UInt64
    private let tracker = ConcurrentCallTracker()

    init(putObjectDelayNanoseconds: UInt64 = 100_000_000) {
        self.putObjectDelayNanoseconds = putObjectDelayNanoseconds
    }

    func stats() async -> (putObjectCallCount: Int, maxInFlightPutObjectCalls: Int) {
        let snapshot = await tracker.snapshot()
        return (snapshot.callCount, snapshot.maxInFlight)
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        await tracker.startCall()
        do {
            try await Task.sleep(nanoseconds: putObjectDelayNanoseconds)
            await tracker.finishCall()
            return PutObjectOutput()
        } catch {
            await tracker.finishCall()
            throw error
        }
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class DeterministicFailureBackupEngineS3Client: GlacierS3Client {
    private let blockedKey: String
    private let failingKey: String
    private let failureTriggeredSignal = AsyncSignal()
    private let releaseBlockedUploadSignal = AsyncSignal()

    init(blockedKey: String, failingKey: String) {
        self.blockedKey = blockedKey
        self.failingKey = failingKey
    }

    func waitUntilFailureTriggered() async {
        await failureTriggeredSignal.wait()
    }

    func releaseBlockedUpload() async {
        await releaseBlockedUploadSignal.signal()
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        if input.key == failingKey {
            await failureTriggeredSignal.signal()
            throw DeterministicFailureS3Error.syntheticFailure
        }

        if input.key == blockedKey {
            await releaseBlockedUploadSignal.wait()
        }

        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

enum DeterministicFailureS3Error: Error {
    case syntheticFailure
}

struct MockHTTPStatusCodeError: Error, HTTPError {
    let httpResponse: HTTPResponse

    init(statusCode: HTTPStatusCode) {
        self.httpResponse = HTTPResponse(statusCode: statusCode)
    }
}

final class CancelIgnoringSequencedBackupEngineS3Client: GlacierS3Client {
    private let firstUploadStartedSignal = AsyncSignal()
    private let releaseFirstUploadSignal = AsyncSignal()
    private let putObjectCallCounter = InvocationCounter()

    func waitUntilFirstUploadStarts() async {
        await firstUploadStartedSignal.wait()
    }

    func releaseFirstUpload() async {
        await releaseFirstUploadSignal.signal()
    }

    func putObjectCallCount() async -> Int {
        await putObjectCallCounter.value()
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        let callIndex = await putObjectCallCounter.incrementAndGet()
        if callIndex == 1 {
            await firstUploadStartedSignal.signal()
        }
        if (input.key ?? "") == "one.txt" {
            await releaseFirstUploadSignal.wait()
        }
        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}
