CGOENABLED:=1
GO15VENDOREXPERIMENT:=1
GO_VERSION=$(shell go version|awk '{print $3}')

GOTOOLS := \
	github.com/GeertJohan/fgt \
	github.com/mattn/goveralls

ifeq (,$(findstring go1.5,$(GO_VERSION)))
	GOTOOLS+=github.com/golang/lint/golint
endif

all: tools build validate

build:
	go build -a . ./plugin/...

vet:
	fgt go vet .

lint:
	fgt golint .

fmt:
	fgt gofmt -l .

test-ci: validate glide-install
	go test -v -race -coverprofile=profile.cov .
	go tool cover -html=profile.cov -o coverage.html

test: build
	go test -v -race .

tools:
	go get -u -v $(GOTOOLS)

validate: vet fmt

glide-install:
	glide install

.PHONY: all vet lint fmt test tools build validate glide-install
