#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

"${SCRIPT_DIR}/build-app.sh"

SOURCE_APP="${REPO_ROOT}/build/IceVault.app"
DEST_APP="/Applications/IceVault.app"

if [[ ! -d "${SOURCE_APP}" ]]; then
  echo "Missing app bundle at ${SOURCE_APP}" >&2
  exit 1
fi

rm -rf "${DEST_APP}"
cp -R "${SOURCE_APP}" "${DEST_APP}"
open "${DEST_APP}"

echo "Installed: ${DEST_APP}"
