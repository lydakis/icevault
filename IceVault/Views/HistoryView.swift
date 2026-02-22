import SwiftUI

struct HistoryView: View {
    @EnvironmentObject private var appState: AppState

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Backup History")
                .font(.title3)
                .fontWeight(.semibold)

            if appState.history.isEmpty {
                ContentUnavailableView(
                    "No Backups Yet",
                    systemImage: "archivebox",
                    description: Text("Run \"Backup Now\" to create your first backup history entry.")
                )
            } else {
                List(Array(appState.history.prefix(100))) { entry in
                    HStack {
                        VStack(alignment: .leading, spacing: 3) {
                            Text(entry.displayDate.formatted(date: .abbreviated, time: .shortened))
                            Text(entry.sourceRoot)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                        }

                        Spacer()

                        Text("\(entry.fileCount) files")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)

                        Text(entry.status.displayName)
                            .font(.subheadline.weight(.medium))
                            .foregroundStyle(statusColor(entry.status))
                    }
                    .padding(.vertical, 2)
                }
                .listStyle(.inset)
            }
        }
    }

    private func statusColor(_ status: BackupJob.Status) -> Color {
        switch status {
        case .idle:
            return .secondary
        case .scanning:
            return .yellow
        case .uploading:
            return .blue
        case .completed:
            return .green
        case .failed:
            return .red
        }
    }
}
