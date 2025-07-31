#!/usr/bin/env bash
VERSION=$1
TAG=v$VERSION

if [[ -z "$VERSION" ]]; then
  echo "Usage: $0 <version>" >&2
  exit 1
fi

echo "Releasing version $VERSION..."

echo "Tagging and pushing..."
git tag -a "$TAG" -m "Release $VERSION"
git push origin "$TAG"

echo "Cleaning build dirs..."
rm -rf dist/ build/

echo "Building..."
python -m build

echo "Uploading to PyPi..."
twine upload dist/*

echo "Uploading to Github..."
gh release create "$TAG" dist/* --title "$TAG" --generate-notes