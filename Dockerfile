FROM golang:1.26.3-trixie@sha256:a085df697019cb63b40a70f6a92b948f7dc9df96dfcb2c20ba6eed25ce28f5b3 AS build

ENV GOTOOLCHAIN=local

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -buildvcs=false -o /out/gateway ./cmd/gateway \
    && go build -trimpath -buildvcs=false -o /out/ingester ./cmd/ingester

FROM debian:trixie-slim@sha256:109e2c65005bf160609e4ba6acf7783752f8502ad218e298253428690b9eaa4b

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build --chown=65532:65532 /out/gateway /app/gateway
COPY --from=build --chown=65532:65532 /out/ingester /app/ingester
COPY --chown=65532:65532 data /app/data
USER 65532:65532
