# Multi-stage build shared by every SentinelFlow binary.
#
# The SERVICE argument selects which cmd/ package to compile, so one Dockerfile
# and one dependency cache serve all five images.

# ---- build stage -------------------------------------------------------------
FROM golang:1.26-alpine AS build

ARG SERVICE
ARG VERSION=dev

WORKDIR /src

# Dependencies change far less often than source, so resolve them in their own
# layer to keep rebuilds fast.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is disabled so the result is a static binary that runs on a bare image.
# The symbol table and DWARF data are stripped because nothing debugs in-image.
RUN test -n "$SERVICE" || (echo "SERVICE build argument is required" >&2; exit 1) && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/service \
        ./cmd/${SERVICE}

# ---- runtime stage -----------------------------------------------------------
FROM alpine:3.22 AS runtime

ARG SERVICE

# ca-certificates for outbound TLS, wget (busybox) for the container healthcheck.
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 -h /app sentinelflow

WORKDIR /app

COPY --from=build /out/service /app/service

# Run unprivileged: nothing here needs root, and a compromised ingestion path
# should not be a compromised container.
USER 10001:10001

LABEL org.opencontainers.image.title="sentinelflow-${SERVICE}" \
      org.opencontainers.image.source="https://github.com/acbspace/sentinel-flow-project"

ENTRYPOINT ["/app/service"]
