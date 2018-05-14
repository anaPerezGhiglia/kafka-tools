#!/bin/bash

function get_current_version () {
    echo $(cat build.sbt | grep "version" | awk -F= '{print $2}' | sed 's/\"//g')
}

function get_next_version () {
    local current=$1
    local last_num=`echo $current| sed -e 's/[0-9]*\.//g'`
    local next_num=$(($last_num+1))
    # Finally replace the last number in version string with the new one.
    local next_version=`echo $current | sed -e 's/[0-9][0-9]*\([^0-9]*\)$/'"$next_num"'/'`
    echo $next_version
}

CURRENT_VERSION=$(get_current_version)
RELEASE_VERSION=$(get_next_version $CURRENT_VERSION)

echo
echo "Current version : "$CURRENT_VERSION
echo "Release version : "$RELEASE_VERSION
echo

echo "Updating build.sbt version ..."
sed -i "s/version\s*:=\s*\".*\"/version := \"$RELEASE_VERSION\"/g" build.sbt

git commit -am "Updated version to $RELEASE_VERSION"
git tag -a $RELEASE_VERSION -m "Version $RELEASE_VERSION"
git push origin master
git push origin $RELEASE_VERSION