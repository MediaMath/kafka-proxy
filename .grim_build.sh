#!/bin/bash

set -eu

. /opt/golang/preferred/bin/go_env.sh

export GOPATH="$(pwd)/go"
export PATH="$GOPATH/bin:$PATH"

export KAFKA_PROXY_TEST_URL=http://kafka-changes-qa.aws.infra.mediamath.com:8082
export KAFKA_PROXY_TEST_TOPIC=kafkaproxy
export KAFKA_PROXY_TEST_REQUIRED=true

export VERBOSITY=-v

cd "./$CLONE_PATH"

make test
