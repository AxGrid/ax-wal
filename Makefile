GO ?= /opt/homebrew/opt/go@1.23/bin/go
export GOTOOLCHAIN=local

.PHONY: test test-race build bench bench-suite clean

test:
	$(GO) test -count=1 -timeout 90s ./...

test-race:
	$(GO) test -count=1 -timeout 180s -race ./...

build:
	$(GO) build ./...

bench:
	$(GO) test -count=1 -bench=. -benchmem -benchtime=2s ./...

clean:
	rm -rf /tmp/axwal-* /tmp/ax-wal-*
