#!/usr/bin/env bash
set -euo pipefail

APP_NAME="IceVault"
BUNDLE_ID="com.icevault.app"
DEFAULT_VERSION="0.1.0"
DEFAULT_BUILD_DIR="build"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CREATE_DMG=0
VERSION="${ICEVAULT_VERSION:-}"
TARGET_ARCH="${ICEVAULT_ARCH:-}"
BUILD_DIR="${ICEVAULT_OUTPUT_DIR:-${DEFAULT_BUILD_DIR}}"

usage() {
  cat <<USAGE
Usage: ./scripts/build-app.sh [options]

Options:
  --dmg                  Build a DMG containing the app bundle
  --version <version>    Override app version (default: tag or ${DEFAULT_VERSION})
  --arch <arm64|x86_64>  Build for target architecture (default: host architecture)
  --output-dir <path>    Output directory for .app/.dmg (default: ${DEFAULT_BUILD_DIR})
  -h, --help             Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dmg)
      CREATE_DMG=1
      shift
      ;;
    --version)
      [[ $# -ge 2 ]] || { echo "Missing value for --version" >&2; exit 1; }
      VERSION="$2"
      shift 2
      ;;
    --arch)
      [[ $# -ge 2 ]] || { echo "Missing value for --arch" >&2; exit 1; }
      TARGET_ARCH="$2"
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || { echo "Missing value for --output-dir" >&2; exit 1; }
      BUILD_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${VERSION}" ]]; then
  if TAG_NAME="$(git -C "${REPO_ROOT}" describe --tags --exact-match 2>/dev/null || true)" && [[ -n "${TAG_NAME}" ]]; then
    VERSION="${TAG_NAME#v}"
  else
    VERSION="${DEFAULT_VERSION}"
  fi
fi

if [[ -z "${TARGET_ARCH}" ]]; then
  TARGET_ARCH="$(uname -m)"
fi

case "${TARGET_ARCH}" in
  arm64|x86_64)
    ;;
  *)
    echo "Unsupported architecture: ${TARGET_ARCH}" >&2
    exit 1
    ;;
esac

if [[ "${BUILD_DIR}" = /* ]]; then
  BUILD_DIR_ABS="${BUILD_DIR}"
else
  BUILD_DIR_ABS="${REPO_ROOT}/${BUILD_DIR}"
fi

APP_DIR="${BUILD_DIR_ABS}/${APP_NAME}.app"
CONTENTS_DIR="${APP_DIR}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
INFO_PLIST_PATH="${CONTENTS_DIR}/Info.plist"
PKGINFO_PATH="${CONTENTS_DIR}/PkgInfo"

mkdir -p "${BUILD_DIR_ABS}"

cd "${REPO_ROOT}"
swift build -c release --disable-sandbox --arch "${TARGET_ARCH}"

BINARY_PATH=""
for candidate in \
  ".build/${TARGET_ARCH}-apple-macosx/release/${APP_NAME}" \
  ".build/release/${APP_NAME}" \
  ".build/apple/Products/Release/${APP_NAME}"
do
  if [[ -f "${candidate}" ]]; then
    BINARY_PATH="${candidate}"
    break
  fi
done

if [[ -z "${BINARY_PATH}" ]]; then
  BINARY_PATH="$(find .build -type f -name "${APP_NAME}" -path "*/release/*" -perm -111 -print -quit 2>/dev/null || true)"
fi

if [[ -z "${BINARY_PATH}" || ! -f "${BINARY_PATH}" ]]; then
  echo "Could not find built binary for ${APP_NAME}" >&2
  exit 1
fi

BINARY_DIR="$(cd "$(dirname "${BINARY_PATH}")" && pwd)"

rm -rf "${APP_DIR}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}"

cp "${BINARY_PATH}" "${MACOS_DIR}/${APP_NAME}"
chmod +x "${MACOS_DIR}/${APP_NAME}"

ICON_PATH="${REPO_ROOT}/assets/logos/IceVault.icns"
if [[ -f "${ICON_PATH}" ]]; then
  cp "${ICON_PATH}" "${RESOURCES_DIR}/IceVault.icns"
  echo "Icon included: IceVault.icns"
else
  echo "Warning: Icon not found at ${ICON_PATH}" >&2
fi

RESOURCE_BUNDLE_COUNT=0
while IFS= read -r bundle; do
  [[ -n "${bundle}" ]] || continue
  cp -R "${bundle}" "${RESOURCES_DIR}/"
  RESOURCE_BUNDLE_COUNT=$((RESOURCE_BUNDLE_COUNT + 1))
done < <(find "${BINARY_DIR}" -maxdepth 1 -type d -name "*.bundle" | sort)

cat > "${INFO_PLIST_PATH}" <<EOF_PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleIdentifier</key>
  <string>${BUNDLE_ID}</string>
  <key>CFBundleName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleDisplayName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleExecutable</key>
  <string>${APP_NAME}</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleIconFile</key>
  <string>IceVault.icns</string>
  <key>CFBundleVersion</key>
  <string>${VERSION}</string>
  <key>CFBundleShortVersionString</key>
  <string>${VERSION}</string>
  <key>LSMinimumSystemVersion</key>
  <string>14.0</string>
  <key>LSUIElement</key>
  <true/>
</dict>
</plist>
EOF_PLIST

printf "APPL????" > "${PKGINFO_PATH}"

echo "Built: ${APP_DIR}"
echo "Included resource bundles: ${RESOURCE_BUNDLE_COUNT}"

if [[ "${CREATE_DMG}" -eq 1 ]]; then
  DMG_PATH="${BUILD_DIR_ABS}/${APP_NAME}-${VERSION}-macos-${TARGET_ARCH}.dmg"
  STAGING_DIR="${BUILD_DIR_ABS}/.dmg-staging"

  rm -rf "${STAGING_DIR}"
  mkdir -p "${STAGING_DIR}"
  cp -R "${APP_DIR}" "${STAGING_DIR}/"
  ln -s /Applications "${STAGING_DIR}/Applications"

  rm -f "${DMG_PATH}"
  hdiutil create \
    -volname "${APP_NAME}" \
    -srcfolder "${STAGING_DIR}" \
    -ov \
    -format UDZO \
    "${DMG_PATH}"

  rm -rf "${STAGING_DIR}"
  echo "Built: ${DMG_PATH}"
fi
