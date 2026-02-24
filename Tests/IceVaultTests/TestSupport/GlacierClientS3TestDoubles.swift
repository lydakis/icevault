import Foundation
import AWSS3
@testable import IceVault

final class MockGlacierS3Client: GlacierS3Client {
    var createMultipartUploadInputs: [CreateMultipartUploadInput] = []
    var uploadPartInputs: [UploadPartInput] = []
    var completeMultipartUploadInputs: [CompleteMultipartUploadInput] = []
    var abortMultipartUploadInputs: [AbortMultipartUploadInput] = []
    var listPartsInputs: [ListPartsInput] = []
    var putObjectInputs: [PutObjectInput] = []
    var headBucketInputs: [HeadBucketInput] = []

    var createMultipartUploadOutput = CreateMultipartUploadOutput(uploadId: "upload-id")
    var uploadPartOutput = UploadPartOutput(eTag: "\"etag-uploaded\"")
    var completeMultipartUploadOutput = CompleteMultipartUploadOutput()
    var abortMultipartUploadOutput = AbortMultipartUploadOutput()
    var putObjectOutput = PutObjectOutput()
    var headBucketOutput = HeadBucketOutput()
    var listPartsOutputs: [ListPartsOutput] = [ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])]
    var createMultipartUploadError: Error?
    var uploadPartError: Error?
    var completeMultipartUploadError: Error?
    var abortMultipartUploadError: Error?
    var listPartsError: Error?
    var putObjectError: Error?
    var headBucketError: Error?

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        createMultipartUploadInputs.append(input)
        if let createMultipartUploadError {
            throw createMultipartUploadError
        }
        return createMultipartUploadOutput
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        uploadPartInputs.append(input)
        if let uploadPartError {
            throw uploadPartError
        }
        return uploadPartOutput
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        completeMultipartUploadInputs.append(input)
        if let completeMultipartUploadError {
            throw completeMultipartUploadError
        }
        return completeMultipartUploadOutput
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        abortMultipartUploadInputs.append(input)
        if let abortMultipartUploadError {
            throw abortMultipartUploadError
        }
        return abortMultipartUploadOutput
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        listPartsInputs.append(input)
        if let listPartsError {
            throw listPartsError
        }
        if !listPartsOutputs.isEmpty {
            return listPartsOutputs.removeFirst()
        }
        return ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        putObjectInputs.append(input)
        if let putObjectError {
            throw putObjectError
        }
        return putObjectOutput
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        headBucketInputs.append(input)
        if let headBucketError {
            throw headBucketError
        }
        return headBucketOutput
    }
}

enum MockS3Error: Error {
    case syntheticFailure
}

final class MultipartConcurrencyTrackingS3Client: GlacierS3Client {
    private let uploadPartDelayNanoseconds: UInt64
    private let tracker = ConcurrentCallTracker()

    init(uploadPartDelayNanoseconds: UInt64 = 100_000_000) {
        self.uploadPartDelayNanoseconds = uploadPartDelayNanoseconds
    }

    func uploadPartStats() async -> (uploadPartCallCount: Int, maxInFlightUploadPartCalls: Int) {
        let snapshot = await tracker.snapshot()
        return (snapshot.callCount, snapshot.maxInFlight)
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        await tracker.startCall()
        do {
            try await Task.sleep(nanoseconds: uploadPartDelayNanoseconds)
            await tracker.finishCall()
            return UploadPartOutput(eTag: "\"etag-\(input.partNumber ?? 0)\"")
        } catch {
            await tracker.finishCall()
            throw error
        }
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

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class FailFastMultipartConcurrencyTrackingS3Client: GlacierS3Client {
    private let failingPartNumber: Int
    private let failureDelayNanoseconds: UInt64
    private let nonFailDelayNanoseconds: UInt64
    private let tracker = ConcurrentCallTracker()

    init(
        failingPartNumber: Int,
        failureDelayNanoseconds: UInt64,
        nonFailDelayNanoseconds: UInt64
    ) {
        self.failingPartNumber = failingPartNumber
        self.failureDelayNanoseconds = failureDelayNanoseconds
        self.nonFailDelayNanoseconds = nonFailDelayNanoseconds
    }

    func uploadPartStats() async -> (uploadPartCallCount: Int, maxInFlightUploadPartCalls: Int) {
        let snapshot = await tracker.snapshot()
        return (snapshot.callCount, snapshot.maxInFlight)
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        let partNumber = input.partNumber ?? 0
        await tracker.startCall()

        if partNumber == failingPartNumber {
            do {
                try await Task.sleep(nanoseconds: failureDelayNanoseconds)
            } catch {
                await tracker.finishCall()
                throw error
            }
            await tracker.finishCall()
            throw MockS3Error.syntheticFailure
        }

        do {
            try await Task.sleep(nanoseconds: nonFailDelayNanoseconds)
        } catch {
            await tracker.finishCall()
            throw error
        }

        await tracker.finishCall()
        return UploadPartOutput(eTag: "\"etag-\(partNumber)\"")
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

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}

final class DeterministicMultipartFailureS3Client: GlacierS3Client {
    private let blockedPartNumber: Int
    private let failingPartNumber: Int
    private let failureTriggeredSignal = AsyncSignal()
    private let releaseBlockedPartSignal = AsyncSignal()

    init(blockedPartNumber: Int, failingPartNumber: Int) {
        self.blockedPartNumber = blockedPartNumber
        self.failingPartNumber = failingPartNumber
    }

    func waitUntilFailureTriggered() async {
        await failureTriggeredSignal.wait()
    }

    func releaseBlockedPart() async {
        await releaseBlockedPartSignal.signal()
    }

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        let partNumber = input.partNumber ?? 0

        if partNumber == failingPartNumber {
            await failureTriggeredSignal.signal()
            throw MockS3Error.syntheticFailure
        }

        if partNumber == blockedPartNumber {
            await releaseBlockedPartSignal.wait()
        }

        return UploadPartOutput(eTag: "\"etag-\(partNumber)\"")
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

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}
