FROM golang:1.19.3-alpine AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=arm64

WORKDIR /build
COPY . .
RUN go mod download
RUN go build -o godis -ldflags="-s -w" -trimpath .

WORKDIR /dist
RUN cp /build/godis .

FROM alpine:latest
COPY --from=builder /dist/godis .

ENTRYPOINT ["/godis"]
CMD ["help"]