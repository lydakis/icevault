# IceVault 🧊

A lightweight, open-source macOS menu bar app for automated cold backups to AWS S3 Glacier.

## What It Does

- Syncs local drives/folders to S3 Glacier Deep Archive
- macOS menu bar UI showing backup status, progress, history
- Scheduled or manual backups via LaunchAgent
- Tracks file inventory locally (SQLite) to enable incremental syncs
- Cost-efficient: ~$1/TB/month for storage

## Tech Stack

- **Swift + SwiftUI** — native macOS menu bar app (MenuBarExtra)
- **AWS SDK for Swift** or shelling out to `aws` CLI for S3 operations
- **SQLite** (via swift-sqlite or GRDB) — local file inventory + backup history
- **LaunchAgent** — scheduled background syncs
- macOS 14+ (Sonoma), Swift 5.9+

## Project Structure

```
IceVault/
├── IceVault.xcodeproj/        # Xcode project
├── IceVault/
│   ├── App.swift              # @main, MenuBarExtra
│   ├── Views/
│   │   ├── MenuBarView.swift  # Main popover: status, progress, actions
│   │   ├── SettingsView.swift # AWS creds, sources, schedule config
│   │   └── HistoryView.swift  # Past backups log
│   ├── Models/
│   │   ├── BackupJob.swift    # Job state machine
│   │   ├── FileRecord.swift   # SQLite file inventory
│   │   └── AppState.swift     # ObservableObject for UI
│   ├── Services/
│   │   ├── BackupEngine.swift # Core sync logic (scan → diff → upload)
│   │   ├── GlacierClient.swift# AWS S3 Glacier operations
│   │   ├── FileScanner.swift  # Walk source dirs, hash files
│   │   └── Database.swift     # SQLite wrapper
│   ├── Utils/
│   │   └── LaunchAgent.swift  # Install/manage launchd plist
│   └── Resources/
│       └── Assets.xcassets     # Menu bar icon
├── IceVaultCLI/               # Optional CLI companion
│   └── main.swift
├── Tests/
├── README.md
├── LICENSE                    # MIT
└── BUILD_LOG.md
```

## Data Model

### FileRecord (SQLite)
- id: Int (auto)
- sourcePath: String
- relativePath: String (relative to source root)
- fileSize: Int64
- modifiedAt: Date
- sha256: String
- glacierKey: String (S3 key)
- uploadedAt: Date?
- storageClass: String (DEEP_ARCHIVE)

### BackupJob
- id: UUID
- sourceRoot: String
- bucket: String
- status: enum (scanning, uploading, completed, failed)
- filesTotal: Int
- filesUploaded: Int
- bytesTotal: Int64
- bytesUploaded: Int64
- startedAt: Date
- completedAt: Date?
- error: String?

### Settings (UserDefaults / plist)
- awsAccessKey, awsSecretKey, awsRegion
- sources: [SourceConfig] (path + bucket + prefix)
- schedule: ScheduleConfig (interval, enabled)
- notifications: Bool

## Key Principles

1. **Incremental by default** — only upload new/changed files (compare mtime + size, optionally SHA256)
2. **Resume-safe** — track upload state per-file so interrupted syncs pick up where they left off
3. **Glacier Deep Archive** — always use DEEP_ARCHIVE storage class for minimum cost
4. **Multipart uploads** — use multipart for files >100MB
5. **No secrets in code** — AWS creds in Keychain or config file
6. **Lightweight** — minimal CPU/memory when idle, throttle uploads to not saturate bandwidth
7. **Open source friendly** — MIT license, clean code, good README

## Build + Test

```bash
# Build
xcodebuild -project IceVault.xcodeproj -scheme IceVault -configuration Debug build

# Test
xcodebuild -project IceVault.xcodeproj -scheme IceVault -configuration Debug test

# Or open in Xcode
open IceVault.xcodeproj
```

## MVP Scope (v0.1)

**Build:**
- Menu bar icon + popover with status
- Settings: configure AWS creds + source folder + bucket
- Manual "Backup Now" button
- File scanning + incremental diff
- Upload to S3 Glacier Deep Archive
- Progress tracking in UI
- Basic history view

**NOT building (v0.1):**
- Restore UI (use AWS console or CLI for now)
- Multiple source configurations (one source for MVP)
- Bandwidth throttling
- File exclusion patterns
- LaunchAgent scheduling (manual trigger only for MVP)
