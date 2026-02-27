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
            statusHeader
            ssoSessionStatusView
            backupContent
            actionButtons
            backupBlockedReasonView
            authenticationPromptView
            cancelButton

            Divider()
            footerButtons
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

    private var statusHeader: some View {
        HStack(spacing: 8) {
            Image(systemName: appState.menuBarSystemImage)
                .foregroundStyle(statusColor)
            Text(appState.statusText)
                .font(.headline)

            Spacer()

            Text(appState.statusBadgeText)
                .font(.caption2.weight(.semibold))
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(statusBadgeColor.opacity(0.2), in: Capsule())
                .foregroundStyle(statusBadgeColor)
        }
    }

    @ViewBuilder
    private var ssoSessionStatusView: some View {
        if let ssoSessionStatusText = appState.ssoSessionStatusText {
            Text(ssoSessionStatusText)
                .font(.caption.weight(.medium))
                .foregroundStyle(appState.isSSOSessionExpired ? .red : .secondary)
        }
    }

    @ViewBuilder
    private var backupContent: some View {
        if let job = appState.currentJob {
            runningBackupContent(job: job)
        } else {
            idleBackupContent
        }
    }

    private func runningBackupContent(job: BackupJob) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            ProgressView(value: job.fileProgressFraction)

            Text("\(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files")
                .font(.subheadline)

            Text("\(formattedBytes(job.bytesUploaded)) / \(formattedBytes(job.bytesTotal))")
                .font(.caption)
                .foregroundStyle(.secondary)

            Text(discoverySummaryText(for: job))
                .font(.caption)
                .foregroundStyle(.secondary)

            if job.shouldShowDiscoveryRate {
                Text("Discovering at \(formattedRate(job.discoveryFilesPerSecond)) files/s (\(formattedBytes(Int64(job.discoveryBytesPerSecond)))/s)")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            if job.uploadBytesPerSecond > 0 {
                Text("Uploading at \(formattedBytes(Int64(job.uploadBytesPerSecond)))/s")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var idleBackupContent: some View {
        VStack(alignment: .leading, spacing: 6) {
            if let lastBackupDate = appState.lastBackupDate {
                Text("Last successful backup: \(lastBackupDate.formatted(date: .abbreviated, time: .shortened))")
                    .font(.subheadline)
            } else {
                Text("Last successful backup: Never")
                    .font(.subheadline)
            }

            Text(appState.idleStatusText)
                .font(.subheadline.weight(.medium))
                .foregroundStyle(idleStatusColor)
        }
    }

    private var actionButtons: some View {
        VStack(alignment: .leading, spacing: 8) {
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
        }
    }

    @ViewBuilder
    private var backupBlockedReasonView: some View {
        if let backupBlockedReason = appState.backupBlockedReason {
            Text(backupBlockedReason)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    @ViewBuilder
    private var authenticationPromptView: some View {
        if let authenticationPromptMessage = appState.authenticationPromptMessage {
            Text(authenticationPromptMessage)
                .font(.caption)
                .foregroundStyle(.red)
        }
    }

    @ViewBuilder
    private var cancelButton: some View {
        if appState.currentJob?.isRunning == true {
            Button("Cancel Backup") {
                appState.cancelBackup()
            }
            .buttonStyle(.bordered)
        }
    }

    private var footerButtons: some View {
        HStack {
            Button("Settings") {
                openSettings()
            }
            Button("History") {
                openWindow(id: "history")
            }
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

    private var statusBadgeColor: Color {
        if let job = appState.currentJob {
            switch job.status {
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

        guard appState.isConfigured else {
            return .secondary
        }

        switch appState.latestBackupStatus {
        case .some(.completed):
            return .green
        case .some(.failed):
            return .red
        case .some(.idle), .some(.scanning), .some(.uploading), .none:
            return .secondary
        }
    }

    private var idleStatusColor: Color {
        guard appState.isConfigured else {
            return .secondary
        }

        switch appState.latestBackupStatus {
        case .some(.completed):
            return .green
        case .some(.failed):
            return .red
        case .some(.idle), .some(.scanning), .some(.uploading), .none:
            return .secondary
        }
    }

    private func formattedCount(_ value: Int) -> String {
        value.formatted(.number.grouping(.automatic))
    }

    private func formattedBytes(_ value: Int64) -> String {
        Self.byteFormatter.string(fromByteCount: max(value, 0))
    }

    private func formattedRate(_ value: Double) -> String {
        value.formatted(.number.precision(.fractionLength(1)))
    }

    private func discoverySummaryText(for job: BackupJob) -> String {
        if
            let estimatedFiles = job.discoveryEstimatedFiles,
            let estimatedBytes = job.discoveryEstimatedBytes
        {
            return "Discovered: \(formattedCount(job.discoveredFiles)) / \(formattedCount(estimatedFiles)) files (\(formattedBytes(job.discoveredBytes)) / \(formattedBytes(estimatedBytes)))"
        }

        return "Discovered: \(formattedCount(job.discoveredFiles)) files (\(formattedBytes(job.discoveredBytes)))"
    }
}
