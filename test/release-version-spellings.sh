#!/bin/sh
# Print the per-registry spellings of a release version, one key=value per
# line, suitable for appending to $GITHUB_OUTPUT in the release smoke tests
# workflow (or for sourcing from a shell):
#   semver        0.10.0-rc.1   (crates.io, the mvn-repo, the release tag)
#   pypi-version  0.10.0rc1     (PyPI & conda)
#   prerelease    true|false    (a semver prerelease suffix is present)
set -eu

version="${1:-}"
version="${version#v}"
if ! printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-(rc|alpha|beta)\.[0-9]+)?$'; then
    echo "error: '${1:-}' is not a supported release version" >&2
    exit 1
fi
pypi_version="$(printf '%s' "$version" | sed -E 's/-rc\.([0-9]+)$/rc\1/; s/-alpha\.([0-9]+)$/a\1/; s/-beta\.([0-9]+)$/b\1/')"
prerelease=false
case "$version" in
    *-*) prerelease=true ;;
esac
echo "semver=$version"
echo "pypi-version=$pypi_version"
echo "prerelease=$prerelease"
