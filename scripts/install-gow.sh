#!/bin/bash

echo "Installing gow (Go file watcher)..."

go install github.com/mitranim/gow@latest

if command -v gow &> /dev/null; then
    echo "gow installed successfully!"
    echo ""
    echo "Usage examples:"
    echo "  gow test ./pkg/tables/...     # Watch and test specific package"
    echo "  gow test ./...                # Watch and test all packages"
    echo "  gow -e go,mod test ./...      # Watch .go and .mod files"
    echo ""
else
    echo "Failed to install gow. Make sure GOPATH/bin is in your PATH."
fi
