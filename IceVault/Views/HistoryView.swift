import AppKit
import SwiftUI

struct HistoryView: View {
    @EnvironmentObject private var appState: AppState

    private static let byteFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    private static let durationFormatter: DateComponentsFormatter = {
        let formatter = DateComponentsFormatter()
        formatter.allowedUnits = [.hour, .minute, .second]
        formatter.unitsStyle = .abbreviated
        formatter.zeroFormattingBehavior = [.dropLeading]
        return formatter
    }()

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("Backup History")
                            .font(.title2.weight(.semibold))
                        Text("Recent jobs, outcomes, and transfer stats.")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    Spacer()

                    Button("Clear History") {
                        appState.clearHistory()
                    }
                    .buttonStyle(.bordered)
                    .disabled(appState.history.isEmpty)
                }

                historySummaryCards

                if appState.history.isEmpty {
                    ContentUnavailableView(
                        "No Backups Yet",
                        systemImage: "clock.arrow.trianglehead.counterclockwise.rotate.90",
                        description: Text("Run Backup Now to record your first backup job.")
                    )
                    .frame(maxWidth: .infinity, minHeight: 220)
                    .background(
                        Color(nsColor: .controlBackgroundColor),
                        in: RoundedRectangle(cornerRadius: 14, style: .continuous)
                    )
                } else {
                    LazyVStack(alignment: .leading, spacing: 10) {
                        ForEach(Array(appState.history.prefix(100))) { entry in
                            historyEntryCard(entry)
                        }
                    }
                }
            }
            .padding(18)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(
            LinearGradient(
                colors: [Color(nsColor: .windowBackgroundColor), Color.accentColor.opacity(0.05)],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
        )
    }

    private var historySummaryCards: some View {
        let recentEntries = Array(appState.history.prefix(100))
        let completedCount = recentEntries.filter { $0.status == .completed }.count
        let failedCount = recentEntries.filter { $0.status == .failed }.count
        let totalBytesUploaded = recentEntries.reduce(0) { partial, entry in
            partial + max(entry.bytesUploaded, 0)
        }

        return HStack(spacing: 10) {
            summaryCard(
                title: "Completed",
                value: formattedCount(completedCount),
                systemImage: "checkmark.circle.fill",
                color: .green
            )
            summaryCard(
                title: "Failed",
                value: formattedCount(failedCount),
                systemImage: "xmark.circle.fill",
                color: .red
            )
            summaryCard(
                title: "Data Uploaded",
                value: formattedBytes(totalBytesUploaded),
                systemImage: "tray.full.fill",
                color: .blue
            )
        }
    }

    @ViewBuilder
    private func summaryCard(
        title: String,
        value: String,
        systemImage: String,
        color: Color
    ) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Label(title, systemImage: systemImage)
                .font(.caption)
                .foregroundStyle(color)
            Text(value)
                .font(.title3.weight(.semibold))
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(nsColor: .controlBackgroundColor), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    @ViewBuilder
    private func historyEntryCard(_ entry: BackupHistoryEntry) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(entry.displayDate.formatted(date: .abbreviated, time: .shortened))
                        .font(.headline)
                    Text(entry.sourceRoot)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }

                Spacer()

                statusBadge(for: entry.status)
            }

            HStack(spacing: 14) {
                Label("\(formattedCount(entry.filesUploaded)) files", systemImage: "doc")
                Label(formattedBytes(entry.bytesUploaded), systemImage: "externaldrive")
                Label(formattedDuration(entry.duration), systemImage: "timer")
            }
            .font(.subheadline)
            .foregroundStyle(.secondary)

            if entry.deferredUploadPendingFiles > 0 {
                Label(
                    "Deferred pending uploads: \(formattedCount(entry.deferredUploadPendingFiles))",
                    systemImage: "arrow.triangle.2.circlepath.circle.fill"
                )
                .font(.subheadline)
                .foregroundStyle(.orange)
            }

            if entry.deferredUploadFailureCount > 0 {
                Label(
                    "Recoverable upload errors: \(formattedCount(entry.deferredUploadFailureCount))",
                    systemImage: "exclamationmark.triangle.fill"
                )
                .font(.subheadline)
                .foregroundStyle(.orange)
            }
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(nsColor: .controlBackgroundColor), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
    }

    private func formattedCount(_ value: Int) -> String {
        value.formatted(.number.grouping(.automatic))
    }

    private func formattedBytes(_ value: Int64) -> String {
        Self.byteFormatter.string(fromByteCount: max(value, 0))
    }

    private func formattedDuration(_ interval: TimeInterval) -> String {
        Self.durationFormatter.string(from: max(interval, 0)) ?? "0s"
    }

    private func statusBadge(for status: BackupJob.Status) -> some View {
        let style = statusStyle(for: status)
        return Label(style.label, systemImage: style.systemImage)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .foregroundStyle(style.color)
            .background(style.color.opacity(0.16))
            .clipShape(Capsule())
    }

    private func statusStyle(for status: BackupJob.Status) -> (label: String, systemImage: String, color: Color) {
        switch status {
        case .idle:
            return ("Idle", "pause.circle.fill", .secondary)
        case .scanning:
            return ("Scanning", "magnifyingglass.circle.fill", .yellow)
        case .uploading:
            return ("Uploading", "arrow.up.circle.fill", .blue)
        case .completed:
            return ("Completed", "checkmark.circle.fill", .green)
        case .failed:
            return ("Failed", "xmark.circle.fill", .red)
        }
    }
}
