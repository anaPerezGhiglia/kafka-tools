#!/bin/bash

DIR="$(dirname "$(readlink -f "$0")")" #get the script directory even if called through symblink

KAFKA_TOOL_JAR="$DIR/target/scala-2.12/kafka-tools.jar"

if test -f "$KAFKA_TOOL_JAR"; then
    java -jar $KAFKA_TOOL_JAR $@
else
    echo "Run $DIR/install.sh script first"
    exit 1
fi

