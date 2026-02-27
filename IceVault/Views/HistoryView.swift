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
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Backup History")
                    .font(.title3.weight(.semibold))

                Spacer()

                Button("Clear History") {
                    appState.clearHistory()
                }
                .buttonStyle(.bordered)
                .disabled(appState.history.isEmpty)
            }

            if appState.history.isEmpty {
                ContentUnavailableView(
                    "No Backups Yet",
                    systemImage: "clock.arrow.trianglehead.counterclockwise.rotate.90",
                    description: Text("Run Backup Now to record your first backup job.")
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(Array(appState.history.prefix(100))) { entry in
                    HStack(alignment: .top, spacing: 12) {
                        VStack(alignment: .leading, spacing: 4) {
                            Text(entry.displayDate.formatted(date: .abbreviated, time: .shortened))
                                .font(.headline)

                            Text("Duration: \(formattedDuration(entry.duration))")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)

                            Text("\(formattedCount(entry.filesUploaded)) files uploaded")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)

                            Text("\(formattedBytes(entry.bytesUploaded)) uploaded")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)

                            if entry.deferredUploadPendingFiles > 0 {
                                Text("Deferred pending uploads: \(formattedCount(entry.deferredUploadPendingFiles))")
                                    .font(.subheadline)
                                    .foregroundStyle(.orange)
                            }

                            if entry.deferredUploadFailureCount > 0 {
                                Text("Recoverable upload errors: \(formattedCount(entry.deferredUploadFailureCount))")
                                    .font(.subheadline)
                                    .foregroundStyle(.orange)
                            }
                        }

                        Spacer()

                        statusBadge(for: entry.status)
                    }
                    .padding(.vertical, 4)
                }
                .listStyle(.inset)
            }
        }
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
