# Release + Packaging

IceVault uses a tag-triggered GitHub Actions release pipeline.

## What Happens On Tag Push

Workflow: `.github/workflows/release.yml`

When a tag like `v0.2.0` is pushed:

1. Build `arm64` DMG on `macos-15`
2. Build `x86_64` DMG on `macos-15-intel`
3. Sign app bundles with Developer ID Application certificate
4. Notarize and staple app bundles and DMGs
5. Publish both DMGs + `checksums.txt` to GitHub Releases
6. Update Homebrew tap cask in `lydakis/homebrew-icevault`

## One-Time Apple Setup

### 1) Export Developer ID certificate as `.p12`

Export your `Developer ID Application` certificate (including private key) from Keychain Access as `.p12`.

Convert it to base64 for GitHub secret storage:

```bash
base64 -i /path/to/DeveloperIDApplication.p12 | pbcopy
```

You also need the `.p12` export password.

Find the exact signing identity string:

```bash
security find-identity -v -p codesigning
```

Use the full `Developer ID Application: ...` value.

### 2) Create an App Store Connect API key

In App Store Connect:

1. Go to `Users and Access` -> `Keys`
2. Create an API key with app notarization access
3. Download the `.p8` file
4. Copy the `Key ID` and `Issuer ID`

Store the `.p8` file contents in a secret (raw file content is fine).

## Required GitHub Secrets

In `lydakis/icevault`, set:

- `APPLE_DEVELOPER_ID_CERTIFICATE_P12_BASE64`: base64-encoded `.p12`
- `APPLE_DEVELOPER_ID_CERTIFICATE_PASSWORD`: password used for `.p12` export
- `APPLE_DEVELOPER_ID_APPLICATION`: exact identity string
- `APP_STORE_CONNECT_API_KEY_P8`: `.p8` private key content
- `APP_STORE_CONNECT_KEY_ID`: App Store Connect key id
- `APP_STORE_CONNECT_ISSUER_ID`: App Store Connect issuer id
- `GORELEASER_TOKEN` (preferred) or `HOMEBREW_TAP_GITHUB_TOKEN`: token with `repo` access to both `lydakis/icevault` and `lydakis/homebrew-icevault`

## Build Locally

Unsigned ad-hoc (local dev):

```bash
./scripts/build-app.sh --dmg --version 0.2.0 --arch arm64 --output-dir dist
```

Signed + notarized (manual local release):

```bash
./scripts/build-app.sh \
  --dmg \
  --notarize \
  --version 0.2.0 \
  --arch arm64 \
  --output-dir dist \
  --sign-identity "Developer ID Application: Your Name (TEAMID)" \
  --asc-key-path /path/to/AuthKey_XXXXXX.p8 \
  --asc-key-id XXXXXXXX \
  --asc-issuer-id XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
```

For Intel, run again with `--arch x86_64`.

## Homebrew Tap Repo

Expected repository: `lydakis/homebrew-icevault`.

If it does not exist yet:

```bash
gh repo create lydakis/homebrew-icevault --public --description "Homebrew tap for IceVault"
```

It should have a `main` branch and at least one initial commit before running the release workflow.

## Release Steps

```bash
git checkout main
git pull

git tag v0.2.0
git push origin v0.2.0
```

Then verify:

1. GitHub Release contains both DMGs and `checksums.txt`
2. `lydakis/homebrew-icevault` has updated `Casks/icevault.rb`
3. Gatekeeper accepts the installed app:

```bash
spctl --assess --type execute --verbose=4 /Applications/IceVault.app
```
