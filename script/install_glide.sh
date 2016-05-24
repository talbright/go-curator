#!/bin/bash

set -x

: ${GLIDE_VERSION="0.10.2"}

case $1 in
  -travis)
    echo "install glide for travis"
    curl -L https://github.com/Masterminds/glide/releases/download/${GLIDE_VERSION}/glide-${GLIDE_VERSION}-linux-amd64.zip -o glide.zip
    unzip glide.zip
    mkdir -p $GOPATH/bin
    mv linux-amd64/glide $GOPATH/bin/glide
    ;;
esac
