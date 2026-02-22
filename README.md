# IceVault 🧊

A lightweight macOS menu bar app for automated cold backups to AWS S3 Glacier Deep Archive.

**~$1/TB/month** for rock-solid backup of your photos, videos, and raw files.

## Features

- 🧊 **Glacier Deep Archive** — cheapest cloud storage available
- 📊 **Menu bar UI** — backup status, progress, and history at a glance
- ⚡ **Incremental sync** — only uploads new/changed files
- 🔒 **Resume-safe** — interrupted backups pick up where they left off
- 🪶 **Lightweight** — minimal resource usage when idle

## Why?

Most backup tools are either expensive (Arq, Backblaze), CLI-only, or overkill. IceVault does one thing well: gets your files into Glacier Deep Archive with a clean native UI.

## Requirements

- macOS 14+ (Sonoma)
- AWS account with S3 access
- Xcode 15+ (to build)

## Getting Started

1. Clone this repo
2. Open `IceVault.xcodeproj` in Xcode
3. Build and run
4. Click the 🧊 icon in your menu bar
5. Configure your AWS credentials and source folder
6. Hit "Backup Now"

## Cost

| Storage | Price |
|---------|-------|
| 1 TB | ~$1/month |
| 5 TB | ~$5/month |
| 10 TB | ~$10/month |

Retrieval from Deep Archive takes 12-48 hours and costs ~$10/TB. This is for backups you hope to never need.

## License

MIT
