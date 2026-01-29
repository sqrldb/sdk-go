#!/bin/bash
set -e

cd "$(dirname "$0")"

if [ -z "$1" ]; then
  echo "Usage: ./release.sh <version>"
  echo "Example: ./release.sh v0.1.0"
  exit 1
fi

VERSION=$1

if [[ ! $VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: Version must be in format v0.0.0"
  exit 1
fi

echo "Releasing squirreldb-go ${VERSION}..."

echo "Running tests..."
go test ./...

echo "Creating git tag..."
git tag "${VERSION}"
git push origin "${VERSION}"

echo "Released github.com/squirreldb/squirreldb-go@${VERSION}"
echo ""
echo "Users can install with:"
echo "  go get github.com/squirreldb/squirreldb-go@${VERSION}"
