# Release + Packaging

IceVault uses a tag-triggered GitHub Actions release pipeline.

## What Happens On Tag Push

Workflow: `.github/workflows/release.yml`

When a tag like `v0.2.0` is pushed:

1. Build `arm64` DMG on `macos-14`
2. Build `x86_64` DMG on `macos-15-intel`
3. Publish both DMGs + `checksums.txt` to GitHub Releases
4. Update Homebrew tap cask in `lydakis/homebrew-icevault`

The workflow explicitly selects the latest stable Xcode available on each runner.

## Build Locally

```bash
./scripts/build-app.sh --dmg --version 0.2.0 --arch arm64 --output-dir dist
```

For Intel:

```bash
./scripts/build-app.sh --dmg --version 0.2.0 --arch x86_64 --output-dir dist
```

## Required GitHub Secret

In `lydakis/icevault`, add:

- `GORELEASER_TOKEN` (preferred, same as JUL), or `HOMEBREW_TAP_GITHUB_TOKEN`

The token must have `repo` access to:

- `lydakis/icevault`
- `lydakis/homebrew-icevault`

## Homebrew Tap Repo

Expected repository: `lydakis/homebrew-icevault`

If it does not exist yet:

```bash
gh repo create lydakis/homebrew-icevault --public --description "Homebrew tap for IceVault"
```

It should have a `main` branch and at least one initial commit before running the release workflow.

Local scaffold path used by this project:

- `../homebrew-icevault`

Manual local cask update command:

```bash
./scripts/update-homebrew-cask.sh \
  --tap-dir ../homebrew-icevault \
  --version 0.2.0 \
  --repo-owner lydakis \
  --repo-name icevault \
  --arm64-sha <arm64_sha256> \
  --x86_64-sha <x86_64_sha256>
```

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
3. Install works:

```bash
brew tap lydakis/icevault
brew install --cask icevault
```
