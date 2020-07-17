.PHONY: fmt download build test docker clean

all: fmt download build

fmt:
	go fmt

download:
	go mod download

build:
	go build -o bin/bigbucket

test: fmt download build
ifeq ($(bucket),)
	@echo Please pass bucket name to use for tests e.g. make test bucket=gs://<bucket-name>
else
	tests/run_tests.sh $(bucket)
endif

docker:
	docker build -t bigbucket .

clean:
	rm -rf bin/
	go clean -modcache
