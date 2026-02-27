import CryptoKit
import Foundation

class FileScanner: @unchecked Sendable {
    struct InventoryStats: Equatable, Sendable {
        let fileCount: Int
        let totalBytes: Int64

        static let zero = InventoryStats(fileCount: 0, totalBytes: 0)

        func adding(_ other: InventoryStats) -> InventoryStats {
            InventoryStats(
                fileCount: fileCount + other.fileCount,
                totalBytes: totalBytes + other.totalBytes
            )
        }
    }

    private static let resourceKeys: Set<URLResourceKey> = [
        .isDirectoryKey,
        .isRegularFileKey,
        .isSymbolicLinkKey,
        .fileSizeKey,
        .contentModificationDateKey
    ]

    private let fileManager: FileManager

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    func scan(
        sourceRoot: String,
        onRecord: (FileRecord) throws -> Void
    ) throws {
        let rootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true).standardizedFileURL

        try scanDirectory(
            at: rootURL,
            rootURL: rootURL,
            sourceRoot: sourceRoot,
            keys: Self.resourceKeys,
            onRecord: onRecord
        )
    }

    func scan(
        sourceRoot: String,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        let rootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true).standardizedFileURL

        try await scanDirectory(
            at: rootURL,
            rootURL: rootURL,
            sourceRoot: sourceRoot,
            keys: Self.resourceKeys,
            onRecord: onRecord
        )
    }

    func scan(sourceRoot: String) throws -> [FileRecord] {
        var records: [FileRecord] = []
        try scan(sourceRoot: sourceRoot) { record in
            records.append(record)
        }

        return records.sorted { lhs, rhs in
            lhs.relativePath.localizedStandardCompare(rhs.relativePath) == .orderedAscending
        }
    }

    func scanMetadata(
        sourceRoot: String,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        let rootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true).standardizedFileURL

        try await scanDirectoryMetadata(
            at: rootURL,
            rootURL: rootURL,
            sourceRoot: sourceRoot,
            keys: Self.resourceKeys,
            onRecord: onRecord
        )
    }

    func sha256Hex(for fileURL: URL) throws -> String {
        try Self.sha256Hex(for: fileURL)
    }

    func inventoryStats(
        sourceRoot: String,
        abortCheck: (() throws -> Void)? = nil
    ) throws -> InventoryStats {
        let rootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true).standardizedFileURL
        return try inventoryStats(
            at: rootURL,
            keys: Self.resourceKeys,
            abortCheck: abortCheck
        )
    }

    private func shouldSkip(fileURL: URL) -> Bool {
        fileURL.lastPathComponent == ".DS_Store"
    }

    private func inventoryStats(
        at directoryURL: URL,
        keys: Set<URLResourceKey>,
        abortCheck: (() throws -> Void)?
    ) throws -> InventoryStats {
        try abortCheck?()

        let entries: [URL]
        do {
            entries = try fileManager.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: Array(keys),
                options: []
            )
        } catch {
            // Ignore inaccessible directories so one folder doesn't fail the whole scan.
            return .zero
        }

        var stats = InventoryStats.zero

        for entryURL in entries {
            try abortCheck?()

            if shouldSkip(fileURL: entryURL) {
                continue
            }

            let values: URLResourceValues
            do {
                values = try entryURL.resourceValues(forKeys: keys)
            } catch {
                continue
            }

            if values.isDirectory == true {
                if values.isSymbolicLink == true {
                    continue
                }
                let nestedStats = try inventoryStats(
                    at: entryURL,
                    keys: keys,
                    abortCheck: abortCheck
                )
                stats = stats.adding(nestedStats)
                continue
            }

            guard values.isRegularFile == true else {
                continue
            }

            let fileSize = Int64(values.fileSize ?? 0)
            stats = stats.adding(
                InventoryStats(
                    fileCount: 1,
                    totalBytes: max(fileSize, 0)
                )
            )
        }

        return stats
    }

    private func scanDirectory(
        at directoryURL: URL,
        rootURL: URL,
        sourceRoot: String,
        keys: Set<URLResourceKey>,
        onRecord: (FileRecord) throws -> Void
    ) throws {
        let entries: [URL]
        do {
            entries = try fileManager.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: Array(keys),
                options: []
            )
        } catch {
            // Ignore inaccessible directories so one folder doesn't fail the whole scan.
            return
        }

        let sortedEntries = entries.sorted { lhs, rhs in
            lhs.lastPathComponent.localizedStandardCompare(rhs.lastPathComponent) == .orderedAscending
        }

        for entryURL in sortedEntries {
            if shouldSkip(fileURL: entryURL) {
                continue
            }

            let values: URLResourceValues
            do {
                values = try entryURL.resourceValues(forKeys: keys)
            } catch {
                continue
            }

            if values.isDirectory == true {
                if values.isSymbolicLink == true {
                    continue
                }
                try scanDirectory(
                    at: entryURL,
                    rootURL: rootURL,
                    sourceRoot: sourceRoot,
                    keys: keys,
                    onRecord: onRecord
                )
                continue
            }

            guard values.isRegularFile == true else {
                continue
            }

            let relativePath = self.relativePath(of: entryURL, from: rootURL)
            let modifiedAt = values.contentModificationDate ?? Date.distantPast
            let fileSize = Int64(values.fileSize ?? 0)
            guard let sha256 = try? Self.sha256Hex(for: entryURL) else {
                continue
            }

            try onRecord(
                FileRecord(
                    sourcePath: sourceRoot,
                    relativePath: relativePath,
                    fileSize: fileSize,
                    modifiedAt: modifiedAt,
                    sha256: sha256,
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }
    }

    private func scanDirectoryMetadata(
        at directoryURL: URL,
        rootURL: URL,
        sourceRoot: String,
        keys: Set<URLResourceKey>,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        let entries: [URL]
        do {
            entries = try fileManager.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: Array(keys),
                options: []
            )
        } catch {
            // Ignore inaccessible directories so one folder doesn't fail the whole scan.
            return
        }

        let sortedEntries = entries.sorted { lhs, rhs in
            lhs.lastPathComponent.localizedStandardCompare(rhs.lastPathComponent) == .orderedAscending
        }

        for entryURL in sortedEntries {
            if shouldSkip(fileURL: entryURL) {
                continue
            }

            let values: URLResourceValues
            do {
                values = try entryURL.resourceValues(forKeys: keys)
            } catch {
                continue
            }

            if values.isDirectory == true {
                if values.isSymbolicLink == true {
                    continue
                }
                try await scanDirectoryMetadata(
                    at: entryURL,
                    rootURL: rootURL,
                    sourceRoot: sourceRoot,
                    keys: keys,
                    onRecord: onRecord
                )
                continue
            }

            guard values.isRegularFile == true else {
                continue
            }

            let relativePath = self.relativePath(of: entryURL, from: rootURL)
            let modifiedAt = values.contentModificationDate ?? Date.distantPast
            let fileSize = Int64(values.fileSize ?? 0)

            try await onRecord(
                FileRecord(
                    sourcePath: sourceRoot,
                    relativePath: relativePath,
                    fileSize: fileSize,
                    modifiedAt: modifiedAt,
                    sha256: "",
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }
    }

    private func scanDirectory(
        at directoryURL: URL,
        rootURL: URL,
        sourceRoot: String,
        keys: Set<URLResourceKey>,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        let entries: [URL]
        do {
            entries = try fileManager.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: Array(keys),
                options: []
            )
        } catch {
            // Ignore inaccessible directories so one folder doesn't fail the whole scan.
            return
        }

        let sortedEntries = entries.sorted { lhs, rhs in
            lhs.lastPathComponent.localizedStandardCompare(rhs.lastPathComponent) == .orderedAscending
        }

        for entryURL in sortedEntries {
            if shouldSkip(fileURL: entryURL) {
                continue
            }

            let values: URLResourceValues
            do {
                values = try entryURL.resourceValues(forKeys: keys)
            } catch {
                continue
            }

            if values.isDirectory == true {
                if values.isSymbolicLink == true {
                    continue
                }
                try await scanDirectory(
                    at: entryURL,
                    rootURL: rootURL,
                    sourceRoot: sourceRoot,
                    keys: keys,
                    onRecord: onRecord
                )
                continue
            }

            guard values.isRegularFile == true else {
                continue
            }

            let relativePath = self.relativePath(of: entryURL, from: rootURL)
            let modifiedAt = values.contentModificationDate ?? Date.distantPast
            let fileSize = Int64(values.fileSize ?? 0)
            guard let sha256 = try? Self.sha256Hex(for: entryURL) else {
                continue
            }

            try await onRecord(
                FileRecord(
                    sourcePath: sourceRoot,
                    relativePath: relativePath,
                    fileSize: fileSize,
                    modifiedAt: modifiedAt,
                    sha256: sha256,
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }
    }

    private func relativePath(of fileURL: URL, from rootURL: URL) -> String {
        let rootPath = rootURL.path
        let filePath = fileURL.standardizedFileURL.path

        guard filePath.hasPrefix(rootPath) else {
            return fileURL.lastPathComponent
        }

        let startIndex = filePath.index(filePath.startIndex, offsetBy: rootPath.count)
        let suffix = filePath[startIndex...]
        if suffix.hasPrefix("/") {
            return String(suffix.dropFirst())
        }

        return String(suffix)
    }

    private static func sha256Hex(for fileURL: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer {
            try? handle.close()
        }

        var hasher = SHA256()
        let chunkSize = 1024 * 1024

        while true {
            let chunk = try handle.read(upToCount: chunkSize) ?? Data()
            if chunk.isEmpty {
                break
            }
            hasher.update(data: chunk)
        }

        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }
}
