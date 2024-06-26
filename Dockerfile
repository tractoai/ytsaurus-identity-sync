FROM --platform=linux/amd64 golang:1.22
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN GOOS=linux GOARCH=amd64 go build -o /ytsaurus-identity-sync

FROM golang:1.22

RUN apt-get update && apt-get install -y telnet curl strace lsof less gzip dnsutils gettext-base
COPY --from=0 /ytsaurus-identity-sync /ytsaurus-identity-sync
CMD ["/ytsaurus-identity-sync"]

