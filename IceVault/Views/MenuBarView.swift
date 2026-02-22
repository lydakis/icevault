import AppKit
import SwiftUI

struct MenuBarView: View {
    @Environment(\.openSettings) private var openSettings
    @Environment(\.openWindow) private var openWindow
    @EnvironmentObject private var appState: AppState

    private static let byteFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Image(systemName: appState.menuBarSystemImage)
                    .foregroundStyle(statusColor)
                Text(appState.statusText)
                    .font(.headline)
            }

            if let ssoSessionStatusText = appState.ssoSessionStatusText {
                Text(ssoSessionStatusText)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(appState.isSSOSessionExpired ? .red : .secondary)
            }

            if let job = appState.currentJob {
                ProgressView(value: job.fileProgressFraction)

                Text("\(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files")
                    .font(.subheadline)

                Text("\(formattedBytes(job.bytesUploaded)) / \(formattedBytes(job.bytesTotal))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                if let lastBackupDate = appState.lastBackupDate {
                    Text("Last backup: \(lastBackupDate.formatted(date: .abbreviated, time: .shortened))")
                        .font(.subheadline)
                } else {
                    Text("Last backup: Never")
                        .font(.subheadline)
                }

                Text(appState.isConfigured ? "All backed up ✓" : "Not configured")
                    .font(.subheadline.weight(.medium))
                    .foregroundStyle(appState.isConfigured ? .green : .secondary)
            }

            if appState.usesSSOAuthentication {
                Button("Refresh Login") {
                    appState.refreshSSOLogin()
                }
                .buttonStyle(.bordered)
                .disabled(!appState.canRefreshSSOLogin)
            }

            Button("Backup Now") {
                appState.startBackup()
            }
            .buttonStyle(.borderedProminent)
            .disabled(
                appState.currentJob?.isRunning == true
                    || !appState.isConfigured
                    || appState.isBackupBlockedBySSOExpiry
            )

            if let backupBlockedReason = appState.backupBlockedReason {
                Text(backupBlockedReason)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            if let authenticationPromptMessage = appState.authenticationPromptMessage {
                Text(authenticationPromptMessage)
                    .font(.caption)
                    .foregroundStyle(.red)
            }

            if appState.currentJob?.isRunning == true {
                Button("Cancel Backup") {
                    appState.cancelBackup()
                }
                .buttonStyle(.bordered)
            }

            Divider()

            HStack {
                Button("Settings") {
                    openSettings()
                }
                Button("History") {
                    openWindow(id: "history")
                }
            }
        }
        .padding(14)
        .frame(width: 340)
        .onAppear {
            appState.refreshSSOSessionStatus()
        }
        .alert(
            "Authentication Required",
            isPresented: Binding(
                get: { appState.authenticationPromptMessage != nil },
                set: { shouldShow in
                    if !shouldShow {
                        appState.dismissAuthenticationPrompt()
                    }
                }
            )
        ) {
            Button("Open Settings") {
                openSettings()
            }
            Button("OK", role: .cancel) {
                appState.dismissAuthenticationPrompt()
            }
        } message: {
            Text(appState.authenticationPromptMessage ?? "")
        }
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

    private func formattedCount(_ value: Int) -> String {
        value.formatted(.number.grouping(.automatic))
    }

    private func formattedBytes(_ value: Int64) -> String {
        Self.byteFormatter.string(fromByteCount: max(value, 0))
    }
}
