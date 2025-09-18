#!/bin/bash

# Watch for changes in Go files and run tests automatically
# Usage: ./scripts/watch-tests.sh [package_path]
# Example: ./scripts/watch-tests.sh ./pkg/tables/...

PACKAGE_PATH=${1:-"./..."}

echo "Watching for changes in Go files..."
echo "Will run: go test $PACKAGE_PATH -v"
echo "Press Ctrl+C to stop"
echo ""

# Run tests initially
echo "Running initial tests..."
go test $PACKAGE_PATH -v
echo ""

# Watch for changes and run tests
while inotifywait -e modify,create,delete -r . --include='.*\.go$' 2>/dev/null; do
    echo "Files changed, running tests..."
    echo "$(date): Running go test $PACKAGE_PATH -v"
    go test $PACKAGE_PATH -v
    echo ""
done