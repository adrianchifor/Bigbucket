.PHONY: build download docker docker_push clean

all: download build

build:
	go build -o bin/bigbucket

download:
	go mod download

docker:
	docker build -t bigbucket .

docker_push: docker
	docker tag bigbucket:latest adrianchifor/bigbucket:latest
	docker push adrianchifor/bigbucket:latest

clean:
	rm -rf bin/
