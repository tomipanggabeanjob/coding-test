# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.26

# base: Debian-based Go image. It ships gcc, which `go test -race` needs (cgo).
FROM golang:${GO_VERSION} AS base
WORKDIR /src
ENV CGO_ENABLED=1
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

# check: build, vet, gofmt and repository-hygiene rules.
FROM base AS check
# Tolerate Windows (CRLF) checkouts of the script.
RUN sed -i 's/\r$//' scripts/check.sh
CMD ["sh", "scripts/check.sh"]

# test: the Definition-of-Done gate. Needs DATABASE_URL for the Postgres tests
# (compose provides it); without it those tests are skipped.
FROM base AS test
CMD ["go", "test", "-race", "-count=10", "./..."]

FROM base AS bench
CMD ["go", "test", "-run", "^$", "-bench", ".", "-benchmem", "-count=3", "./internal/service/..."]

# e2e: black-box test against the running app + postgres + mock principal.
FROM base AS e2e
CMD ["go", "test", "-tags", "e2e", "-count=1", "-v", "./e2e/..."]

FROM base AS build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockprincipal ./cmd/mockprincipal

# Runtime images are distroless (no shell), so health checks use the binaries' -healthcheck flag.
FROM gcr.io/distroless/static-debian12:nonroot AS server
COPY --from=build /out/server /server
EXPOSE 8080
ENTRYPOINT ["/server"]

FROM gcr.io/distroless/static-debian12:nonroot AS mockprincipal
COPY --from=build /out/mockprincipal /mockprincipal
EXPOSE 9090
ENTRYPOINT ["/mockprincipal"]
