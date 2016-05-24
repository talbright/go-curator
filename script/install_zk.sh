#!/bin/bash

set -x

: ${ZK_VERSION="3.4.6"}

case $1 in
  -travis)
    echo "install zk for travis"
    wget "http://apache.cs.utah.edu/zookeeper/zookeeper-${ZK_VERSION}/zookeeper-${ZK_VERSION}.tar.gz"
    tar -xf "zookeeper-${ZK_VERSION}.tar.gz"
    mv zookeeper-${ZK_VERSION} zk
    mv ./zk/conf/zoo_sample.cfg ./zk/conf/zoo.cfg
    ;;
esac
