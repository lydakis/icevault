import AppKit
import Darwin
import SwiftUI

@main
struct IceVaultApp: App {
    @StateObject private var appState: AppState

    init() {
        let runtimeAppState = AppState()
        _appState = StateObject(wrappedValue: runtimeAppState)
        let isHeadlessBackupMode = CommandLine.arguments.contains("--backup")

        if !isHeadlessBackupMode {
            runtimeAppState.requestNotificationPermissionIfNeeded()
        }

        if isHeadlessBackupMode {
            NSApplication.shared.setActivationPolicy(.prohibited)

            Task { @MainActor in
                await runtimeAppState.startManualBackup()

                let latestStatus = runtimeAppState.history.first?.status
                let latestError = runtimeAppState.history.first?.error ?? "Unknown backup error."

                if latestStatus == .completed {
                    print("IceVault backup completed.")
                    exit(0)
                }

                fputs("IceVault backup failed: \(latestError)\n", stderr)
                exit(1)
            }
        }
    }

    var body: some Scene {
        MenuBarExtra("IceVault", systemImage: "snowflake") {
            MenuBarView()
                .environmentObject(appState)
        }
        .menuBarExtraStyle(.window)

        Settings {
            SettingsView()
                .environmentObject(appState)
        }

        Window("Backup History", id: "history") {
            HistoryView()
                .environmentObject(appState)
                .frame(minWidth: 520, minHeight: 360)
                .padding()
        }
    }
}
