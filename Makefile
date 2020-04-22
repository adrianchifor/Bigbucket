.PHONY: fmt download build docker docker_push clean

all: fmt download build

fmt:
	go fmt

download:
	go mod download

build:
	go build -o bin/bigbucket

docker:
	docker build -t bigbucket .

docker_push: docker
	docker tag bigbucket:latest adrianchifor/bigbucket:latest
	docker push adrianchifor/bigbucket:latest

clean:
	rm -rf bin/
	go clean -modcache
