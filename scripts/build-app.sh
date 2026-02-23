#!/usr/bin/env bash
set -euo pipefail

APP_NAME="IceVault"
BUNDLE_ID="com.icevault.app"
VERSION="0.1.0"
BUILD_DIR="build"
APP_DIR="${BUILD_DIR}/${APP_NAME}.app"
CONTENTS_DIR="${APP_DIR}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
INFO_PLIST_PATH="${CONTENTS_DIR}/Info.plist"
PKGINFO_PATH="${CONTENTS_DIR}/PkgInfo"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CREATE_DMG=0
for arg in "$@"; do
  case "${arg}" in
    --dmg)
      CREATE_DMG=1
      ;;
    -h|--help)
      echo "Usage: ./scripts/build-app.sh [--dmg]"
      exit 0
      ;;
    *)
      echo "Unknown option: ${arg}" >&2
      echo "Usage: ./scripts/build-app.sh [--dmg]" >&2
      exit 1
      ;;
  esac
done

cd "${REPO_ROOT}"
swift build -c release --disable-sandbox

BINARY_PATH=""
for candidate in \
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

rm -rf "${APP_DIR}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}"

cp "${BINARY_PATH}" "${MACOS_DIR}/${APP_NAME}"
chmod +x "${MACOS_DIR}/${APP_NAME}"

cat > "${INFO_PLIST_PATH}" <<EOF
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
  <key>LSUIElement</key>
  <true/>
  <key>CFBundleVersion</key>
  <string>${VERSION}</string>
  <key>CFBundleShortVersionString</key>
  <string>${VERSION}</string>
  <key>LSMinimumSystemVersion</key>
  <string>14.0</string>
</dict>
</plist>
EOF

printf "APPL????" > "${PKGINFO_PATH}"

echo "Built: ${APP_DIR}"

if [[ "${CREATE_DMG}" -eq 1 ]]; then
  DMG_PATH="${BUILD_DIR}/${APP_NAME}-${VERSION}.dmg"
  rm -f "${DMG_PATH}"
  hdiutil create \
    -volname "${APP_NAME}" \
    -srcfolder "${APP_DIR}" \
    -ov \
    -format UDZO \
    "${DMG_PATH}"
  echo "Built: ${DMG_PATH}"
fi
