FROM golang:1.16-alpine as builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /go/src/bigbucket
COPY . /go/src/bigbucket

RUN go mod download

RUN go build -o /go/bin/bigbucket

# Runner
FROM alpine

LABEL org.opencontainers.image.source https://github.com/adrianchifor/bigbucket

RUN apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=builder /go/bin/bigbucket /

# Disable debug logs in Gin http server and listen over 0.0.0.0
ENV GIN_MODE release

ENTRYPOINT ["/bigbucket"]
