#!/usr/bin/env sh
set -eu

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(dirname -- "$script_dir")"
cd "$repo_root"

if ! command -v uvx >/dev/null 2>&1; then
    echo "uvx is required so release tooling and dependencies are pinned." >&2
    exit 1
fi

release_uv() {
    uvx --from "uv==0.11.28" uv "$@"
}

release_uv lock --check
release_python="${JOBBY_RELEASE_PYTHON:-3.13.14}"
release_environment="${JOBBY_RELEASE_ENVIRONMENT:-.venv-release}"
release_uv python install --managed-python "$release_python"
UV_PROJECT_ENVIRONMENT="$release_environment" release_uv sync \
    --frozen --all-extras --managed-python --python "$release_python"
python_bin="$release_environment/bin/python"
version="$("$python_bin" scripts/release_tools.py version)"
if [ -n "${JOBBY_RELEASE_TAG:-}" ]; then
    "$python_bin" scripts/release_tools.py check-tag "$JOBBY_RELEASE_TAG"
fi

source_date_epoch="${SOURCE_DATE_EPOCH:-315619200}"
macos_deployment_target="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
mkdir -p dist
rm -rf build dist/jobby
rm -f dist/jobby-*.whl dist/jobby-*.tar.gz dist/SHA256SUMS

if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" != "arm64" ]; then
    echo "The personal macOS release target is Apple Silicon (arm64)." >&2
    exit 1
fi
"$python_bin" -m pytest -q
SOURCE_DATE_EPOCH="$source_date_epoch" \
    "$python_bin" -m build --wheel --no-isolation --outdir dist
SOURCE_DATE_EPOCH="$source_date_epoch" \
MACOSX_DEPLOYMENT_TARGET="$macos_deployment_target" \
    "$python_bin" -m PyInstaller --noconfirm --clean jobby.spec
archive="dist/jobby-${version}-$(uname -s)-$(uname -m).tar.gz"
COPYFILE_DISABLE=1 tar -czf "$archive" -C dist jobby
wheel="$(find dist -maxdepth 1 -type f -name "jobby-${version}-*.whl" -print -quit)"
if [ -z "$wheel" ]; then
    echo "Built wheel was not found for version $version." >&2
    exit 1
fi
"$python_bin" scripts/release_tools.py checksums \
    --output dist/SHA256SUMS "$archive" "$wheel"

smoke_root="$(mktemp -d)"
trap 'rm -rf "$smoke_root"' EXIT
tar -xzf "$archive" -C "$smoke_root"
"$python_bin" scripts/release_tools.py verify \
    --bundle "$smoke_root/jobby" \
    --archive "$archive" \
    --checksums dist/SHA256SUMS \
    --expected-version "$version" \
    --max-macos-min-version "$macos_deployment_target"

echo "Release created in $archive"
echo "Wheel created in $wheel"
echo "Checksums created in dist/SHA256SUMS"
