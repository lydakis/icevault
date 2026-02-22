import CryptoKit
import Foundation

final class FileScanner {
    private let fileManager: FileManager

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    func scan(sourceRoot: String) throws -> [FileRecord] {
        let rootURL = URL(fileURLWithPath: sourceRoot, isDirectory: true).standardizedFileURL
        let keys: Set<URLResourceKey> = [
            .isRegularFileKey,
            .fileSizeKey,
            .contentModificationDateKey
        ]

        guard let enumerator = fileManager.enumerator(
            at: rootURL,
            includingPropertiesForKeys: Array(keys),
            options: [],
            errorHandler: { _, _ in
                // Ignore inaccessible paths so one file doesn't fail the whole scan.
                true
            }
        ) else {
            return []
        }

        var records: [FileRecord] = []

        for case let fileURL as URL in enumerator {
            if shouldSkip(fileURL: fileURL) {
                continue
            }

            let values = try fileURL.resourceValues(forKeys: keys)
            guard values.isRegularFile == true else {
                continue
            }

            let relativePath = self.relativePath(of: fileURL, from: rootURL)
            let modifiedAt = values.contentModificationDate ?? Date.distantPast
            let fileSize = Int64(values.fileSize ?? 0)
            guard let sha256 = try? Self.sha256Hex(for: fileURL) else {
                // Keep scanning when a single file becomes unreadable.
                continue
            }

            records.append(
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

        return records.sorted { lhs, rhs in
            lhs.relativePath.localizedStandardCompare(rhs.relativePath) == .orderedAscending
        }
    }

    private func shouldSkip(fileURL: URL) -> Bool {
        fileURL.lastPathComponent == ".DS_Store"
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
