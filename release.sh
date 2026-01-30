#!/bin/bash
set -e

cd "$(dirname "$0")"

VERSION="v0.1.0"

echo "Releasing github.com/squirreldb/squirreldb-sdk-go ${VERSION}..."

echo "Running tests..."
go test ./...

echo "Released github.com/squirreldb/squirreldb-sdk-go@${VERSION}"
echo ""
echo "Users can install with:"
echo "  go get github.com/squirreldb/squirreldb-sdk-go@${VERSION}"
