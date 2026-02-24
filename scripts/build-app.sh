#!/usr/bin/env bash
set -euo pipefail

APP_NAME="IceVault"
BUNDLE_ID="com.icevault.app"
DEFAULT_VERSION="0.1.0"
DEFAULT_BUILD_DIR="build"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CREATE_DMG=0
NOTARIZE=0
VERSION="${ICEVAULT_VERSION:-}"
TARGET_ARCH="${ICEVAULT_ARCH:-}"
BUILD_DIR="${ICEVAULT_OUTPUT_DIR:-${DEFAULT_BUILD_DIR}}"
SIGN_IDENTITY="${ICEVAULT_SIGN_IDENTITY:-}"
ASC_KEY_PATH="${ICEVAULT_ASC_API_KEY_PATH:-}"
ASC_KEY_ID="${ICEVAULT_ASC_KEY_ID:-}"
ASC_ISSUER_ID="${ICEVAULT_ASC_ISSUER_ID:-}"
ENTITLEMENTS_PATH="${ICEVAULT_ENTITLEMENTS_PATH:-}"

usage() {
  cat <<USAGE
Usage: ./scripts/build-app.sh [options]

Options:
  --dmg                         Build a DMG containing the app bundle
  --notarize                    Notarize and staple the app (and DMG when --dmg is used)
  --version <version>           Override app version (default: tag or ${DEFAULT_VERSION})
  --arch <arm64|x86_64>         Build for target architecture (default: host architecture)
  --output-dir <path>           Output directory for .app/.dmg (default: ${DEFAULT_BUILD_DIR})
  --sign-identity <identity>    Codesign identity (defaults to ad-hoc if omitted)
  --entitlements <path>         Entitlements plist for release signing
  --asc-key-path <path>         App Store Connect API key (.p8) path for notarization
  --asc-key-id <key-id>         App Store Connect API key id for notarization
  --asc-issuer-id <issuer-id>   App Store Connect issuer id for notarization
  -h, --help                    Show this help message
USAGE
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${command_name}" >&2
    exit 1
  fi
}

write_default_entitlements_if_missing() {
  local entitlements_file="$1"
  local entitlements_dir
  entitlements_dir="$(dirname "${entitlements_file}")"
  mkdir -p "${entitlements_dir}"
  if [[ ! -f "${entitlements_file}" ]]; then
    cat > "${entitlements_file}" <<EOF_ENTITLEMENTS
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
</dict>
</plist>
EOF_ENTITLEMENTS
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dmg)
      CREATE_DMG=1
      shift
      ;;
    --notarize)
      NOTARIZE=1
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
    --sign-identity)
      [[ $# -ge 2 ]] || { echo "Missing value for --sign-identity" >&2; exit 1; }
      SIGN_IDENTITY="$2"
      shift 2
      ;;
    --entitlements)
      [[ $# -ge 2 ]] || { echo "Missing value for --entitlements" >&2; exit 1; }
      ENTITLEMENTS_PATH="$2"
      shift 2
      ;;
    --asc-key-path)
      [[ $# -ge 2 ]] || { echo "Missing value for --asc-key-path" >&2; exit 1; }
      ASC_KEY_PATH="$2"
      shift 2
      ;;
    --asc-key-id)
      [[ $# -ge 2 ]] || { echo "Missing value for --asc-key-id" >&2; exit 1; }
      ASC_KEY_ID="$2"
      shift 2
      ;;
    --asc-issuer-id)
      [[ $# -ge 2 ]] || { echo "Missing value for --asc-issuer-id" >&2; exit 1; }
      ASC_ISSUER_ID="$2"
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

if [[ -z "${ENTITLEMENTS_PATH}" ]]; then
  ENTITLEMENTS_PATH="${BUILD_DIR_ABS}/.codesign/${APP_NAME}.entitlements"
fi

if [[ "${NOTARIZE}" -eq 1 ]]; then
  if [[ -z "${SIGN_IDENTITY}" ]]; then
    echo "Notarization requires --sign-identity (or ICEVAULT_SIGN_IDENTITY)." >&2
    exit 1
  fi
  if [[ -z "${ASC_KEY_PATH}" || -z "${ASC_KEY_ID}" || -z "${ASC_ISSUER_ID}" ]]; then
    echo "Notarization requires --asc-key-path, --asc-key-id, and --asc-issuer-id." >&2
    exit 1
  fi
  if [[ ! -f "${ASC_KEY_PATH}" ]]; then
    echo "App Store Connect API key file not found: ${ASC_KEY_PATH}" >&2
    exit 1
  fi
fi

APP_DIR="${BUILD_DIR_ABS}/${APP_NAME}.app"
CONTENTS_DIR="${APP_DIR}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
INFO_PLIST_PATH="${CONTENTS_DIR}/Info.plist"
PKGINFO_PATH="${CONTENTS_DIR}/PkgInfo"

mkdir -p "${BUILD_DIR_ABS}"
write_default_entitlements_if_missing "${ENTITLEMENTS_PATH}"

require_command swift
require_command codesign
require_command xattr
require_command hdiutil
require_command ditto
if [[ "${NOTARIZE}" -eq 1 ]]; then
  require_command xcrun
fi

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

chmod -R u+w "${APP_DIR}"
xattr -cr "${APP_DIR}" || true
find "${APP_DIR}" -name '._*' -delete || true

if [[ -n "${SIGN_IDENTITY}" ]]; then
  CODESIGN_ARGS=(--force --timestamp --options runtime --sign "${SIGN_IDENTITY}")
  echo "Signing app bundle with identity: ${SIGN_IDENTITY}"
else
  CODESIGN_ARGS=(--force --sign "-")
  echo "Signing app bundle with ad-hoc identity."
fi

while IFS= read -r -d '' executable; do
  codesign "${CODESIGN_ARGS[@]}" "${executable}"
done < <(
  find "${CONTENTS_DIR}" \
    -type f \
    -perm -111 \
    ! -path "${MACOS_DIR}/${APP_NAME}" \
    -print0
)

while IFS= read -r -d '' dylib; do
  codesign "${CODESIGN_ARGS[@]}" "${dylib}"
done < <(find "${CONTENTS_DIR}" -type f -name "*.dylib" -print0)

while IFS= read -r -d '' framework; do
  codesign "${CODESIGN_ARGS[@]}" "${framework}"
done < <(find "${CONTENTS_DIR}" -type d -name "*.framework" -print0)

if [[ -n "${SIGN_IDENTITY}" ]]; then
  codesign "${CODESIGN_ARGS[@]}" --entitlements "${ENTITLEMENTS_PATH}" "${APP_DIR}"
else
  codesign "${CODESIGN_ARGS[@]}" "${APP_DIR}"
fi

codesign --verify --deep --strict --verbose=2 "${APP_DIR}"
if [[ -n "${SIGN_IDENTITY}" ]]; then
  spctl --assess --type execute --verbose=4 "${APP_DIR}"
fi

if [[ "${NOTARIZE}" -eq 1 ]]; then
  NOTARY_ZIP_PATH="${BUILD_DIR_ABS}/${APP_NAME}-${VERSION}-macos-${TARGET_ARCH}-notary.zip"
  rm -f "${NOTARY_ZIP_PATH}"
  ditto --norsrc -c -k --keepParent "${APP_DIR}" "${NOTARY_ZIP_PATH}"
  xcrun notarytool submit "${NOTARY_ZIP_PATH}" \
    --key "${ASC_KEY_PATH}" \
    --key-id "${ASC_KEY_ID}" \
    --issuer "${ASC_ISSUER_ID}" \
    --wait
  xcrun stapler staple "${APP_DIR}"
  xcrun stapler validate "${APP_DIR}"
  rm -f "${NOTARY_ZIP_PATH}"
fi

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

  if [[ "${NOTARIZE}" -eq 1 ]]; then
    xcrun notarytool submit "${DMG_PATH}" \
      --key "${ASC_KEY_PATH}" \
      --key-id "${ASC_KEY_ID}" \
      --issuer "${ASC_ISSUER_ID}" \
      --wait
    xcrun stapler staple "${DMG_PATH}"
    xcrun stapler validate "${DMG_PATH}"
  fi

  echo "Built: ${DMG_PATH}"
fi
