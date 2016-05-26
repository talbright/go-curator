CGOENABLED:=1
GO15VENDOREXPERIMENT:=1

GOTOOLS = \
	github.com/golang/lint/golint \
	github.com/GeertJohan/fgt \
	github.com/mattn/goveralls

all: tools build validate

vet:
	fgt go vet .

lint:
	fgt golint .

fmt:
	fgt gofmt -l .

test: validate glide-install
	go test -v -race -coverprofile=profile.cov .
	go tool cover -html=profile.cov -o coverage.html

tools:
	go get -u -v $(GOTOOLS)

validate: vet fmt

glide-install:
	glide install

.PHONY: all vet lint fmt test tools build validate glide-install
