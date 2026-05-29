# ── Builder ───────────────────────────────────────────────────────────────────
FROM golang:1.25 AS builder

WORKDIR /build

# Copy module manifests first for better layer caching.
COPY go.mod go.sum* ./
RUN go mod download

# Copy source and build a fully static binary so the distroless final stage
# does not need libc. CGO disabled keeps it pure-Go and reproducible.
COPY . .

ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.commit=${COMMIT}" \
      -o /out/briihass \
      ./cmd/briihass

# ── Runtime ───────────────────────────────────────────────────────────────────
# distroless/static includes CA certificates + /etc/passwd with nonroot user
# (uid 65532) but no shell, no libc, no package manager.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/briihass /usr/local/bin/briihass

# 8080: ingest + heartbeat + health (expose via your ingress/reverse proxy)
# 8081: Prometheus metrics (keep internal-only)
EXPOSE 8080 8081

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/briihass"]
