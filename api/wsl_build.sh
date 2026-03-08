#!/usr/bin/env bash
set -e

export PATH="$HOME/go-sdk/go/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin"
export GOPATH="$HOME/go"
export CGO_ENABLED=1

PROJ="/mnt/c/Users/0510t/OneDrive/Documents/nat-gas-terminal/api"

echo "Go version: $(go version)"
echo "GCC version: $(gcc --version | head -1)"
echo "Building..."
cd "$PROJ"
go build -o api .
echo "Build OK"
