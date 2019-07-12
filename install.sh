#!/bin/bash

command -v sbt >/dev/null 2>&1 || { echo >&2 "sbt is needed but it's not installed. Aborting... "; exit 1; }

ORIGINAL_DIR=$(pwd)
cd $(dirname $0)
sbt assembly
cd $ORIGINAL_DIR

