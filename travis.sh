#!/bin/bash

set -x

: ${ZK_VERSION="3.4.6"}

case $1 in
	-install)
		echo "install dependencies for travis"
		go get github.com/golang/lint/golint
		go get github.com/GeertJohan/fgt
		go get golang.org/x/tools/cmd/cover
		go get github.com/mattn/goveralls
		curl -L https://github.com/Masterminds/glide/releases/download/0.10.2/glide-0.10.2-linux-amd64.zip -o glide.zip
		unzip glide.zip
		mv ./linux-amd64/glide $GOPATH/bin/glide
		wget "http://apache.cs.utah.edu/zookeeper/zookeeper-${ZK_VERSION}/zookeeper-${ZK_VERSION}.tar.gz"
		tar -xvf "zookeeper-${ZK_VERSION}.tar.gz"
		mv zookeeper-$ZK_VERSION zk
		mv ./zk/conf/zoo_sample.cfg ./zk/conf/zoo.cfg
		./zk/bin/zkServer.sh start ./zk/conf/zoo.cfg 1> /dev/null
		glide install
		go install ./vendor/github.com/onsi/ginkgo/ginkgo
		;;
	-test)
		echo "run tests for travis"
		CGOENABLED=1 ginkgo -v -r -cover $(glide novendor)
		goveralls -service=travis-ci -repotoken $COVERALLS_TOKEN -coverprofile=go-curator.coverprofile
		fgt go vet $(glide novendor)
		fgt golint $(glide novendor)
		rm -rf ./vendor
		fgt gofmt -l .
		;;
esac
