import AppKit
import SwiftUI

struct MenuBarView: View {
    @Environment(\.openWindow) private var openWindow
    @EnvironmentObject private var appState: AppState

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Image(systemName: appState.menuBarSystemImage)
                    .foregroundStyle(statusColor)
                Text(appState.statusText)
                    .font(.headline)
            }

            if let job = appState.currentJob {
                ProgressView(value: job.fileProgressFraction) {
                    Text("Backup Progress")
                } currentValueLabel: {
                    Text("\(job.filesUploaded)/\(job.filesTotal) files")
                }

                let totalBytes = max(job.bytesTotal, 0)
                Text("\(job.bytesUploaded.formatted(.byteCount(style: .file))) of \(totalBytes.formatted(.byteCount(style: .file)))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("No active backup job")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            if let lastBackupDate = appState.lastBackupDate {
                Text("Last backup: \(lastBackupDate.formatted(date: .abbreviated, time: .shortened))")
                    .font(.subheadline)
            } else {
                Text("Last backup: Never")
                    .font(.subheadline)
            }

            Button("Backup Now") {
                Task {
                    await appState.startManualBackup()
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(appState.currentJob?.isRunning == true)

            Divider()

            HStack {
                Button("Settings") {
                    NSApp.sendAction(Selector(("showSettingsWindow:")), to: nil, from: nil)
                }
                Button("History") {
                    openWindow(id: "history")
                }
            }
        }
        .padding(14)
        .frame(width: 340)
    }

    private var statusColor: Color {
        switch appState.currentJob?.status ?? .idle {
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
