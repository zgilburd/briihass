# briihass — make targets for local development.
# Container images are built from the Dockerfile; this Makefile is for the
# local dev loop.

BINARY        := briihass
PKG           := ./...
COMMIT        := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GO_BUILD_FLAGS := -trimpath -ldflags="-s -w -X main.commit=$(COMMIT)"

.PHONY: build test test-race test-postgres vet fmt fmt-check lint tidy clean run docker-build help

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the briihass binary into ./bin/.
	@mkdir -p bin
	go build $(GO_BUILD_FLAGS) -o bin/$(BINARY) ./cmd/briihass

test: ## Run unit tests.
	go test $(PKG)

test-race: ## Run unit tests with the race detector.
	go test -race $(PKG)

test-postgres: ## Spin up a throwaway Postgres in Docker and run the store integration tests.
	@docker rm -f briihass-pg-test 2>/dev/null || true
	@docker run -d --rm --name briihass-pg-test -p 5433:5432 -e POSTGRES_PASSWORD=test postgres:16-alpine >/dev/null
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  if docker exec briihass-pg-test pg_isready -U postgres >/dev/null 2>&1; then break; fi; \
	  sleep 1; \
	done
	@TEST_POSTGRES_DSN='postgres://postgres:test@localhost:5433/postgres?sslmode=disable' \
	  go test -race -v ./internal/store/...; rc=$$?; \
	  docker rm -f briihass-pg-test >/dev/null; \
	  exit $$rc

vet: ## go vet.
	go vet $(PKG)

fmt: ## gofmt -w all sources.
	gofmt -w .

fmt-check: ## Fail if any source is not gofmt-clean.
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
	  echo "gofmt needed:"; echo "$$unformatted"; exit 1; \
	fi

lint: vet fmt-check ## All static checks (vet + fmt).

tidy: ## go mod tidy.
	go mod tidy

clean: ## Remove build artifacts.
	rm -rf bin/

run: build ## Build and run with dev credentials sourced from the user file.
	@eval "$$(scripts/dev-creds.sh --print)"; \
	  BRIIHASS_CONFIG=$$HOME/.config/briihass/briihass.yaml \
	  ./bin/$(BINARY)

docker-build: ## Build the production container image locally.
	docker build --build-arg COMMIT=$(COMMIT) -t $(BINARY):$(COMMIT) .
