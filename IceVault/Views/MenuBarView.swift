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
        VStack(alignment: .leading, spacing: 8) {
            ProgressView(value: uploadedOfDiscoveredProgressFraction(for: job))

            Text(uploadedOfDiscoveredFileProgressText(for: job))
                .font(.subheadline)
                .monospacedDigit()

            Text(uploadByteProgressText(for: job))
                .font(.caption)
                .foregroundStyle(.secondary)
                .monospacedDigit()

            uploadRateMetric(job: job)

            if shouldShowDetailsDisclosure(for: job) {
                detailsDisclosure(job: job)
            }

            if job.hasDeferredUploadIssues {
                deferredUploadTelemetryContent(job: job)
            } else if job.deferredUploadFailureCount > 0 {
                deferredUploadRecoveryDisclosure(job: job)
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

    private func uploadRateMetric(job: BackupJob) -> some View {
        metricTile(
            title: "Upload Rate",
            value: uploadRateValueText(for: job),
            tint: .blue
        )
    }

    private func metricTile(
        title: String,
        value: String,
        tint: Color = .secondary
    ) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.caption.weight(.semibold))
                .foregroundStyle(tint)
                .monospacedDigit()
                .lineLimit(1)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(8)
        .background(Color.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
    }

    private func shouldShowDetailsDisclosure(for job: BackupJob) -> Bool {
        job.filesTotal > 0 || job.shouldShowDiscoveryRate || job.hasDiscoveryEstimate
    }

    @ViewBuilder
    private func detailsDisclosure(job: BackupJob) -> some View {
        DisclosureGroup("Details") {
            VStack(alignment: .leading, spacing: 4) {
                Text("Upload queue: \(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files")
                    .monospacedDigit()

                if let scanFileProgressText = scanFileProgressText(for: job) {
                    Text(scanFileProgressText)
                        .monospacedDigit()
                }

                if let scanByteProgressText = scanByteProgressText(for: job) {
                    Text(scanByteProgressText)
                        .monospacedDigit()
                }

                Text(discoveryRateText(for: job))
                    .monospacedDigit()
            }
            .font(.caption2)
            .foregroundStyle(.secondary)
            .padding(.top, 2)
        }
        .font(.caption)
    }

    @ViewBuilder
    private func deferredUploadTelemetryContent(job: BackupJob) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if job.isRetryingDeferredUploads {
                Label(
                    "Retrying deferred uploads (pass \(formattedCount(job.deferredUploadRetryPassCount)))",
                    systemImage: "arrow.clockwise.circle.fill"
                )
            } else {
                Label(
                    "Deferred uploads detected (\(formattedCount(job.deferredUploadPendingFiles)) pending)",
                    systemImage: "exclamationmark.triangle.fill"
                )
            }

            DisclosureGroup("Recovery Details") {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Recoverable upload errors: \(formattedCount(job.deferredUploadFailureCount))")
                    Text("Pending uploads: \(formattedCount(job.deferredUploadPendingFiles))")

                    if let deferredUploadLastError = job.deferredUploadLastError, !deferredUploadLastError.isEmpty {
                        Text("Last error: \(deferredUploadLastError)")
                            .lineLimit(2)
                    }
                }
                .padding(.top, 2)
            }
        }
        .font(.caption)
        .foregroundStyle(.orange)
        .padding(8)
        .background(Color.orange.opacity(0.12), in: RoundedRectangle(cornerRadius: 8))
    }

    @ViewBuilder
    private func deferredUploadRecoveryDisclosure(job: BackupJob) -> some View {
        DisclosureGroup("Recovered Upload Warnings") {
            VStack(alignment: .leading, spacing: 4) {
                Text("Recoverable upload errors: \(formattedCount(job.deferredUploadFailureCount))")
                if let deferredUploadLastError = job.deferredUploadLastError, !deferredUploadLastError.isEmpty {
                    Text("Last error: \(deferredUploadLastError)")
                        .lineLimit(2)
                }
            }
            .font(.caption2)
            .foregroundStyle(.secondary)
            .padding(.top, 2)
        }
        .font(.caption2)
        .foregroundStyle(.secondary)
    }

    private func uploadFileProgressText(for job: BackupJob) -> String {
        if job.filesTotal > 0 {
            return "Uploaded \(formattedCount(job.filesUploaded)) / \(formattedCount(job.filesTotal)) files"
        }
        return "Uploaded \(formattedCount(job.filesUploaded)) files"
    }

    private func uploadedOfDiscoveredFileProgressText(for job: BackupJob) -> String {
        let discoveredCount = max(job.discoveredFiles, 0)
        if discoveredCount > 0 {
            let uploadedFromDiscovered = min(max(job.filesUploaded, 0), discoveredCount)
            return "Uploaded \(formattedCount(uploadedFromDiscovered)) / \(formattedCount(discoveredCount)) discovered files"
        }
        return uploadFileProgressText(for: job)
    }

    private func uploadedOfDiscoveredProgressFraction(for job: BackupJob) -> Double {
        let discoveredCount = max(job.discoveredFiles, 0)
        if discoveredCount > 0 {
            let uploadedFromDiscovered = min(max(job.filesUploaded, 0), discoveredCount)
            return min(1, Double(uploadedFromDiscovered) / Double(discoveredCount))
        }
        return job.fileProgressFraction
    }

    private func uploadByteProgressText(for job: BackupJob) -> String {
        return "Uploaded \(formattedBytes(job.bytesUploaded)) / \(formattedBytes(job.bytesTotal))"
    }

    private func scanFileProgressText(for job: BackupJob) -> String? {
        guard let estimatedFiles = job.discoveryEstimatedFiles else {
            return nil
        }
        let normalizedEstimate = max(estimatedFiles, job.discoveredFiles)
        return "Scanned \(formattedCount(job.discoveredFiles)) / \(formattedCount(normalizedEstimate)) files"
    }

    private func scanByteProgressText(for job: BackupJob) -> String? {
        guard let estimatedBytes = job.discoveryEstimatedBytes else {
            return nil
        }
        let normalizedEstimate = max(estimatedBytes, job.discoveredBytes)
        return "Scanned \(formattedBytes(job.discoveredBytes)) / \(formattedBytes(normalizedEstimate))"
    }

    private func discoveryRateText(for job: BackupJob) -> String {
        let filesPerSecond = job.isScanInProgress ? job.discoveryFilesPerSecond : 0
        let bytesPerSecond = job.isScanInProgress ? job.discoveryBytesPerSecond : 0
        return "Discovery rate: \(formattedRate(filesPerSecond)) files/s (\(formattedBytes(Int64(max(bytesPerSecond, 0))))/s)"
    }

    private func uploadRateValueText(for job: BackupJob) -> String {
        let bytesPerSecond = job.isRunning ? job.uploadBytesPerSecond : 0
        return "\(formattedBytes(Int64(max(bytesPerSecond, 0))))/s"
    }
}
