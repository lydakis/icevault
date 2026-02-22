# IceVault 🧊

A lightweight macOS menu bar app for automated cold backups to AWS S3 Glacier Deep Archive.

IceVault focuses on low-cost archival backups with a native SwiftUI menu bar experience.

## Features

- Glacier Deep Archive uploads (`DEEP_ARCHIVE` storage class)
- Incremental sync using a local SQLite inventory
- Multipart uploads for large files (>100 MB)
- Resume-safe backup jobs with history
- Credential auto-detection (`Keychain` -> `~/.aws/credentials` -> env vars)
- Optional scheduled backups via LaunchAgent

## Requirements

- macOS 14+ (Sonoma)
- AWS account with S3 access
- Xcode 15+ (for Xcode workflow)
- Swift 5.9+ (for SwiftPM workflow)

## Build

```bash
swift build --disable-sandbox
```

## Run

```bash
swift run IceVault
```

Headless backup mode (used by LaunchAgent):

```bash
swift run IceVault --backup
```

## Quick Install (.app)

```bash
./scripts/build-app.sh && open build/IceVault.app
```

## Build DMG

```bash
./scripts/build-app.sh --dmg
```

## Manual Install

```bash
cp -R build/IceVault.app /Applications/
open /Applications/IceVault.app
```

## Least-Privilege IAM Policy

Replace `YOUR_BUCKET_NAME` with your bucket. This policy scopes access to one bucket and only upload-related actions.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IceVaultBucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:HeadBucket"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME"
    },
    {
      "Sid": "IceVaultObjectUploadAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:CreateMultipartUpload",
        "s3:UploadPart",
        "s3:CompleteMultipartUpload",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
    }
  ]
}
```

## AWS Credentials Setup

IceVault credential resolution order is:

1. Keychain (credentials saved from IceVault Settings)
2. `~/.aws/credentials` (`[default]` profile)
3. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` env vars

Region resolution uses:

1. Region set in IceVault Settings
2. `~/.aws/config` (`[default]` profile region)
3. `AWS_REGION` / `AWS_DEFAULT_REGION`

### Option A: `aws configure`

```bash
aws configure
```

This writes:

- `~/.aws/credentials`
- `~/.aws/config`

### Option B: Manual files

`~/.aws/credentials`

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

`~/.aws/config`

```ini
[default]
region = us-east-1
```

### Option C: Environment variables

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
export AWS_REGION=us-east-1
```

## Scheduled Backups

IceVault can install a LaunchAgent at:

- `~/Library/LaunchAgents/com.icevault.backup.plist`

The agent runs IceVault with:

- `--backup`

Supported intervals:

- Daily
- Weekly
- Custom (every N hours)

Configure this in the Settings UI under **Scheduling**.

## Screenshots

Screenshots will be added for:

- Menu bar status view
- Settings view (credentials + scheduling)
- Backup history view

## Contributing

1. Fork the repo.
2. Create a feature branch.
3. Make focused changes with clear commit messages.
4. Run `swift build --disable-sandbox` before opening a PR.
5. Open a pull request with a short summary and testing notes.

## License

MIT
