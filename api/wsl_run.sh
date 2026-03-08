#!/usr/bin/env bash
set -e
export PATH="$HOME/go-sdk/go/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin"
export GOPATH="$HOME/go"
export CGO_ENABLED=1

cd /mnt/c/Users/0510t/OneDrive/Documents/nat-gas-terminal/api

export DB_PATH="../data/db/terminal.duckdb"
export PORT=8080
export ALLOWED_ORIGIN="http://localhost:3000"

exec ./api
