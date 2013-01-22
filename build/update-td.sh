#!/bin/bash
cd "$(dirname $0)"
chrev="$1"

if [ -d td-java/.git ];then
    rm -rf td-java/*
    cd td-java
    git checkout . || exit 1
    git pull || exit 1
else
    rm -rf td-java/
    git clone git@github.com:treasure-data/td-java.git td-java || exit 1
    cd td-java
fi
git checkout master

if [ -n "$chrev" ];then
    git checkout $chrev
fi

revname="$(git show --pretty=format:'%H %ad' | head -n 1)"

mvn package -Dmaven.test.skip=true || exit 1
cp target/td-0.1.1.jar ../../java/td-0.1.1.jar

if [ -n "$chrev" ];then
    git checkout master
fi

cd ../../

echo "$revname" > java/td_java.version

echo ""
echo "git commit ./java -m \"updated td-java $revname\""
git commit ./java/td-0.1.1.jar ./java/td_java.version -m "updated td-java $revname" || exit 1

