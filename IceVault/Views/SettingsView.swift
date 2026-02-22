import SwiftUI

struct SettingsView: View {
    @EnvironmentObject private var appState: AppState

    @State private var draft = AppState.Settings()
    @State private var savedAt: Date?

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Form {
                TextField("AWS Access Key", text: $draft.awsAccessKey)
                SecureField("AWS Secret Key", text: $draft.awsSecretKey)
                TextField("AWS Region", text: $draft.awsRegion)
                TextField("S3 Bucket", text: $draft.bucket)
                TextField("Source Path", text: $draft.sourcePath)
            }
            .formStyle(.grouped)

            HStack {
                Button("Save") {
                    appState.updateSettings(draft)
                    savedAt = Date()
                }
                .buttonStyle(.borderedProminent)

                if let savedAt {
                    Text("Saved \(savedAt.formatted(date: .omitted, time: .shortened))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .padding()
        .frame(minWidth: 520)
        .onAppear {
            draft = appState.settings
        }
    }
}
