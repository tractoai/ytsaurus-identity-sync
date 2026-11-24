FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN go build -o /ytsaurus-identity-sync ./cmd/ytsaurus-identity-sync

FROM alpine:3.23

RUN apk add --no-cache curl
COPY --from=builder /ytsaurus-identity-sync /ytsaurus-identity-sync
CMD ["/ytsaurus-identity-sync"]
