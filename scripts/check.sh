#!/bin/sh
# Static gate run inside the `check` container (docker compose run --rm check).
set -eu

echo "== go build ./..."
go build ./...

echo "== go vet ./..."
go vet ./...

echo "== go vet (e2e build tag)"
go vet -tags e2e ./e2e/...

echo "== gofmt -l ."
unformatted="$(gofmt -l .)"
if [ -n "$unformatted" ]; then
  echo "gofmt needed on:"
  echo "$unformatted"
  exit 1
fi

echo "== no time.Sleep in tests"
if grep -rn --include='*_test.go' 'time\.Sleep' . ; then
  echo "time.Sleep found in tests; synchronise with channels/barriers instead"
  exit 1
fi

echo "== no mutable package-level variables (sentinel errors, interface assertions and embed.FS are allowed; *_test.go excluded)"
# Production packages only: tests may keep sync.Once fixtures (e.g. shared testcontainers DSN).
if grep -rnE --include='*.go' --exclude='*_test.go' '^var ' . | grep -vE ':[0-9]+:var (_|Err[A-Za-z0-9]+) |embed\.FS$' ; then
  echo "package-level var found"
  exit 1
fi
if grep -rnE --include='*.go' --exclude='*_test.go' '^var \($' . ; then
  echo "grouped package-level var block found"
  exit 1
fi

echo "== all checks passed"
