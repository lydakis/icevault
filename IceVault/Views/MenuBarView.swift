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
            ProgressView(value: job.primaryProgressFraction)

            Text(scopeFileProgressText(for: job))
                .font(.subheadline)
                .monospacedDigit()

            Text(scopeByteProgressText(for: job))
                .font(.caption)
                .foregroundStyle(.secondary)
                .monospacedDigit()

            Text("Upload queue: \(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files")
                .font(.caption)
                .foregroundStyle(.secondary)
                .monospacedDigit()

            Text(discoveryRateText(for: job))
                .font(.caption2)
                .foregroundStyle(.secondary)
                .monospacedDigit()

            Text(uploadRateText(for: job))
                .font(.caption2)
                .foregroundStyle(.secondary)
                .monospacedDigit()

            if job.hasDeferredUploadIssues {
                deferredUploadTelemetryContent(job: job)
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

            if let latestDeferredUploadStatusText = appState.latestDeferredUploadStatusText {
                Label(latestDeferredUploadStatusText, systemImage: "exclamationmark.triangle.fill")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
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
        if appState.currentJob?.hasDeferredUploadIssues == true {
            return .orange
        }

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
            if job.hasDeferredUploadIssues {
                return .orange
            }

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
            if appState.hasPendingDeferredUploads {
                return .orange
            }
            return .red
        case .some(.idle), .some(.scanning), .some(.uploading), .none:
            return .secondary
        }
    }

    private var idleStatusColor: Color {
        guard appState.isConfigured else {
            return .secondary
        }

        if appState.hasPendingDeferredUploads {
            return .orange
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

    @ViewBuilder
    private func deferredUploadTelemetryContent(job: BackupJob) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            if job.isRetryingDeferredUploads {
                Label(
                    "Retrying deferred uploads (pass \(formattedCount(job.deferredUploadRetryPassCount)))",
                    systemImage: "arrow.clockwise.circle.fill"
                )
            } else {
                Label("Deferred uploads detected", systemImage: "exclamationmark.triangle.fill")
            }

            Text("Recoverable upload errors: \(formattedCount(job.deferredUploadFailureCount))")
            Text("Pending uploads: \(formattedCount(job.deferredUploadPendingFiles))")

            if let deferredUploadLastError = job.deferredUploadLastError, !deferredUploadLastError.isEmpty {
                Text("Last error: \(deferredUploadLastError)")
                    .lineLimit(2)
            }
        }
        .font(.caption2)
        .foregroundStyle(.orange)
        .padding(8)
        .background(Color.orange.opacity(0.12), in: RoundedRectangle(cornerRadius: 8))
    }

    private func scopeFileProgressText(for job: BackupJob) -> String {
        if let estimatedFiles = job.discoveryEstimatedFiles {
            let normalizedEstimate = max(estimatedFiles, job.discoveredFiles)
            return "Scanned \(formattedCount(job.discoveredFiles)) / \(formattedCount(normalizedEstimate)) files"
        }
        return "Uploaded \(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files"
    }

    private func scopeByteProgressText(for job: BackupJob) -> String {
        if let estimatedBytes = job.discoveryEstimatedBytes {
            let normalizedEstimate = max(estimatedBytes, job.discoveredBytes)
            return "Scanned \(formattedBytes(job.discoveredBytes)) / \(formattedBytes(normalizedEstimate))"
        }
        return "Uploaded \(formattedBytes(job.bytesUploaded)) / \(formattedBytes(job.bytesTotal))"
    }

    private func discoveryRateText(for job: BackupJob) -> String {
        let filesPerSecond = job.isScanInProgress ? job.discoveryFilesPerSecond : 0
        let bytesPerSecond = job.isScanInProgress ? job.discoveryBytesPerSecond : 0
        return "Discovery rate: \(formattedRate(filesPerSecond)) files/s (\(formattedBytes(Int64(max(bytesPerSecond, 0))))/s)"
    }

    private func uploadRateText(for job: BackupJob) -> String {
        let bytesPerSecond = job.isRunning ? job.uploadBytesPerSecond : 0
        return "Upload rate: \(formattedBytes(Int64(max(bytesPerSecond, 0))))/s"
    }
}
