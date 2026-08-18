# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/proxy-pool ./cmd/proxy-pool

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S proxy-pool && adduser -S -G proxy-pool proxy-pool
WORKDIR /app
COPY --from=builder /out/proxy-pool /app/proxy-pool
COPY config.yaml /app/config.yaml
RUN mkdir -p /app/data /app/logs && chown -R proxy-pool:proxy-pool /app
USER proxy-pool
EXPOSE 8080 10000
VOLUME ["/app/data", "/app/logs"]
ENTRYPOINT ["/app/proxy-pool"]
CMD ["-config", "/app/config.yaml"]
