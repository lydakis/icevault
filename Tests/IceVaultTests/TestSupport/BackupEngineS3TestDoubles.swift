import Foundation
import AWSS3
import protocol AWSClientRuntime.AWSServiceError
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

final class ForbiddenFailureBackupEngineS3Client: GlacierS3Client {
    private let failingKey: String
    private let statusCode: HTTPStatusCode
    private let failureMessage: String?
    private let failureErrorCode: String?
    private let failureError: Error?
    private let failureTriggeredSignal = AsyncSignal()

    init(
        failingKey: String,
        statusCode: HTTPStatusCode = .forbidden,
        failureMessage: String? = nil,
        failureErrorCode: String? = nil,
        failureError: Error? = nil
    ) {
        self.failingKey = failingKey
        self.statusCode = statusCode
        self.failureMessage = failureMessage
        self.failureErrorCode = failureErrorCode
        self.failureError = failureError
    }

    func waitUntilFailureTriggered() async {
        await failureTriggeredSignal.wait()
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
            if let failureError {
                throw failureError
            }
            throw MockHTTPStatusCodeError(
                statusCode: statusCode,
                message: failureMessage,
                awsErrorCode: failureErrorCode
            )
        }

        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class FailOnceBackupEngineS3Client: GlacierS3Client {
    private actor FailedKeyTracker {
        private var failedKeys: Set<String> = []

        func shouldFail(for key: String) -> Bool {
            if failedKeys.contains(key) {
                return false
            }
            failedKeys.insert(key)
            return true
        }
    }

    private let transientFailureKey: String
    private let putObjectCallCounter = InvocationCounter()
    private let failedKeyTracker = FailedKeyTracker()

    init(transientFailureKey: String) {
        self.transientFailureKey = transientFailureKey
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
        let key = input.key ?? ""
        if key == transientFailureKey {
            let shouldFail = await failedKeyTracker.shouldFail(for: key)
            if shouldFail {
                throw DeterministicFailureS3Error.syntheticFailure
            }
        }
        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class DelayedSuccessAndDeterministicFailureBackupEngineS3Client: GlacierS3Client {
    private let delayedKey: String
    private let failingKey: String
    private let delayNanoseconds: UInt64

    init(
        delayedKey: String,
        failingKey: String,
        delayNanoseconds: UInt64 = 500_000_000
    ) {
        self.delayedKey = delayedKey
        self.failingKey = failingKey
        self.delayNanoseconds = delayNanoseconds
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
        let key = input.key ?? ""
        if key == delayedKey {
            try await Task.sleep(nanoseconds: delayNanoseconds)
            return PutObjectOutput()
        }

        if key == failingKey {
            throw DeterministicFailureS3Error.syntheticFailure
        }

        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class RecoverableAndMultipartFailureBackupEngineS3Client: GlacierS3Client {
    private let recoverableFailureKey: String
    private let multipartFailureKey: String
    private let multipartPartStartedSignal = AsyncSignal()
    private let releaseMultipartFailureSignal = AsyncSignal()
    private let recoverableFailureTriggeredSignal = AsyncSignal()

    init(recoverableFailureKey: String, multipartFailureKey: String) {
        self.recoverableFailureKey = recoverableFailureKey
        self.multipartFailureKey = multipartFailureKey
    }

    func waitUntilRecoverableFailureTriggered() async {
        await recoverableFailureTriggeredSignal.wait()
    }

    func releaseMultipartFailure() async {
        await releaseMultipartFailureSignal.signal()
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        if input.key == multipartFailureKey {
            await multipartPartStartedSignal.signal()
            await releaseMultipartFailureSignal.wait()
            // Return no ETag so GlacierClient surfaces .missingMultipartETag.
            return UploadPartOutput()
        }
        return UploadPartOutput(eTag: "\"etag\"")
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
        if input.key == recoverableFailureKey {
            // Ensure multipart upload is already in flight so this recoverable failure is observed first.
            await multipartPartStartedSignal.wait()
            await recoverableFailureTriggeredSignal.signal()
            throw DeterministicFailureS3Error.syntheticFailure
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

struct MockHTTPStatusCodeError: Error, HTTPError, LocalizedError, AWSServiceError, CustomStringConvertible, CustomDebugStringConvertible {
    let httpResponse: HTTPResponse
    private let detailMessage: String?
    private let awsErrorCode: String?

    var typeName: String? { awsErrorCode }
    var message: String? { detailMessage }
    var requestID: String? { nil }

    init(statusCode: HTTPStatusCode, message: String? = nil, awsErrorCode: String? = nil) {
        self.httpResponse = HTTPResponse(statusCode: statusCode)
        detailMessage = message
        self.awsErrorCode = awsErrorCode
    }

    var errorDescription: String? {
        detailMessage
    }

    var description: String {
        "MockHTTPStatusCodeError(statusCode: \(httpResponse.statusCode.rawValue))"
    }

    var debugDescription: String {
        description
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

final class CancelIgnoringRemoteValidationBackupEngineS3Client: GlacierS3Client {
    private let validationStartedSignal = AsyncSignal()
    private let releaseValidationSignal = AsyncSignal()
    private let headObjectCallCounter = InvocationCounter()
    private let output: HeadObjectOutput

    init(output: HeadObjectOutput) {
        self.output = output
    }

    func waitUntilRemoteValidationStarts() async {
        await validationStartedSignal.wait()
    }

    func releaseRemoteValidation() async {
        await releaseValidationSignal.signal()
    }

    func headObjectCallCount() async -> Int {
        await headObjectCallCounter.value()
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
        PutObjectOutput()
    }

    func headObject(input: HeadObjectInput) async throws -> HeadObjectOutput {
        await headObjectCallCounter.increment()
        await validationStartedSignal.signal()
        await releaseValidationSignal.wait()
        return output
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}
