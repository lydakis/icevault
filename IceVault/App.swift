import SwiftUI

@main
struct IceVaultApp: App {
    @StateObject private var appState = AppState()

    var body: some Scene {
        MenuBarExtra("IceVault", systemImage: appState.menuBarSystemImage) {
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
