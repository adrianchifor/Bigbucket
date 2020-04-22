FROM golang:1.14-alpine as builder

RUN apk add --no-cache git

WORKDIR /go/src/bigbucket
COPY . /go/src/bigbucket

RUN go mod download

RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/bigbucket


# Runner
FROM alpine

RUN apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=builder /go/bin/bigbucket /

ENTRYPOINT ["/bigbucket"]
