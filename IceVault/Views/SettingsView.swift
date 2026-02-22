import AppKit
import SwiftUI

struct SettingsView: View {
    @EnvironmentObject private var appState: AppState

    @State private var draft = AppState.Settings()
    @State private var accessKey = ""
    @State private var secretKey = ""
    @State private var savedAt: Date?
    @State private var saveMessage: String?
    @State private var saveSucceeded = false
    @State private var connectionMessage: String?
    @State private var connectionSucceeded = false
    @State private var isTestingConnection = false

    private static let commonRegions: [String] = [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ca-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-central-1",
        "eu-north-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "ap-south-1",
        "sa-east-1"
    ]

    var body: some View {
        Form {
            Section("AWS Credentials") {
                TextField("Access Key ID", text: $accessKey)
                    .textContentType(.username)

                SecureField("Secret Access Key", text: $secretKey)
                    .textContentType(.password)

                Text("Credentials are stored in macOS Keychain.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section("Storage") {
                TextField("Bucket Name", text: $draft.bucket)

                Picker("Region", selection: $draft.awsRegion) {
                    ForEach(regionOptions, id: \.self) { region in
                        Text(region).tag(region)
                    }
                }
                .pickerStyle(.menu)
            }

            Section("Source") {
                HStack(alignment: .top, spacing: 10) {
                    Text(draft.sourcePath.isEmpty ? "No folder selected" : draft.sourcePath)
                        .font(.subheadline)
                        .foregroundStyle(draft.sourcePath.isEmpty ? .secondary : .primary)
                        .lineLimit(2)
                        .textSelection(.enabled)

                    Spacer()

                    Button("Choose Folder…") {
                        chooseSourceFolder()
                    }
                }
            }

            Section {
                HStack(spacing: 10) {
                    Button("Test Connection") {
                        testConnection()
                    }
                    .buttonStyle(.bordered)
                    .disabled(isTestingConnection)

                    if isTestingConnection {
                        ProgressView()
                            .controlSize(.small)
                    }

                    Spacer()

                    Button("Save") {
                        saveSettings()
                    }
                    .buttonStyle(.borderedProminent)
                }

                if let connectionMessage {
                    Text(connectionMessage)
                        .font(.caption)
                        .foregroundStyle(connectionSucceeded ? .green : .red)
                }

                if let saveMessage {
                    Text(saveMessage)
                        .font(.caption)
                        .foregroundStyle(saveSucceeded ? .green : .red)
                }

                if let savedAt {
                    Text("Last saved \(savedAt.formatted(date: .abbreviated, time: .shortened))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .formStyle(.grouped)
        .padding()
        .frame(minWidth: 560)
        .onAppear {
            loadDraft()
        }
    }

    private var regionOptions: [String] {
        if Self.commonRegions.contains(draft.awsRegion) {
            return Self.commonRegions
        }
        return [draft.awsRegion].filter { !$0.isEmpty } + Self.commonRegions
    }

    private func loadDraft() {
        draft = appState.settings
        do {
            if let credentials = try appState.loadStoredCredentials() {
                accessKey = credentials.accessKey
                secretKey = credentials.secretKey
            } else {
                accessKey = ""
                secretKey = ""
            }
        } catch {
            accessKey = ""
            secretKey = ""
            saveMessage = error.localizedDescription
            saveSucceeded = false
        }
    }

    private func saveSettings() {
        do {
            let normalizedAccessKey = trimmed(accessKey)
            let normalizedSecretKey = trimmed(secretKey)

            if normalizedAccessKey.isEmpty && normalizedSecretKey.isEmpty {
                try appState.deleteStoredCredentials()
            } else {
                try appState.saveCredentials(
                    accessKey: normalizedAccessKey,
                    secretKey: normalizedSecretKey
                )
            }

            var normalizedSettings = draft
            normalizedSettings.bucket = trimmed(draft.bucket)
            normalizedSettings.awsRegion = trimmed(draft.awsRegion)
            normalizedSettings.sourcePath = trimmed(draft.sourcePath)
            appState.updateSettings(normalizedSettings)

            savedAt = Date()
            saveMessage = "Settings saved."
            saveSucceeded = true
        } catch {
            saveMessage = error.localizedDescription
            saveSucceeded = false
        }
    }

    private func testConnection() {
        isTestingConnection = true
        connectionMessage = nil

        let region = trimmed(draft.awsRegion)
        let bucket = trimmed(draft.bucket)
        let normalizedAccessKey = trimmed(accessKey)
        let normalizedSecretKey = trimmed(secretKey)

        Task { @MainActor in
            do {
                let credentials: AWSCredentials

                if normalizedAccessKey.isEmpty && normalizedSecretKey.isEmpty {
                    guard let storedCredentials = try appState.loadStoredCredentials() else {
                        throw KeychainServiceError.incompleteCredentials
                    }
                    credentials = storedCredentials
                } else {
                    credentials = AWSCredentials(
                        accessKey: normalizedAccessKey,
                        secretKey: normalizedSecretKey
                    )
                }

                try await appState.testConnection(
                    accessKey: credentials.accessKey,
                    secretKey: credentials.secretKey,
                    region: region,
                    bucket: bucket
                )

                connectionMessage = "Connection succeeded. Bucket is reachable."
                connectionSucceeded = true
            } catch {
                connectionMessage = "Connection failed: \(error.localizedDescription)"
                connectionSucceeded = false
            }

            isTestingConnection = false
        }
    }

    private func chooseSourceFolder() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.canCreateDirectories = false
        panel.allowsMultipleSelection = false
        panel.prompt = "Select"

        if panel.runModal() == .OK, let selectedURL = panel.url {
            draft.sourcePath = selectedURL.path
        }
    }

    private func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
