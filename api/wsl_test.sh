#!/usr/bin/env bash
set -e

export PATH="$HOME/go-sdk/go/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin"
export GOPATH="$HOME/go"
export CGO_ENABLED=1

PROJ="/mnt/c/Users/0510t/OneDrive/Documents/nat-gas-terminal/api"

echo "Running tests..."
cd "$PROJ"
go test -v -timeout 30s ./...
