#!/bin/bash
cd "$(dirname $0)"
chrev="$1"

if [ -d td-bulk-import-java/.git ];then
    rm -rf td-bulk-import-java/*
    cd td-bulk-import-java
    git checkout . || exit 1
    git pull || exit 1
else
    rm -rf td-bulk-import-java/
    git clone git@github.com:treasure-data/td-bulk-import-java.git td-bulk-import-java || exit 1
    cd td-bulk-import-java
fi
git checkout master

if [ -n "$chrev" ];then
    git checkout $chrev
fi

revname="$(git show --pretty=format:'%H %ad' | head -n 1)"
vername="0.1.2-SNAPSHOT"

mvn package -Dmaven.test.skip=true || exit 1
cp target/td-bulk-import-${vername}.jar ../../java/td-bulk-import-${vername}.jar

if [ -n "$chrev" ];then
    git checkout master
fi

cd ../../

echo "$revname" > java/td-bulk-import-java.version

echo ""
echo "git commit ./java -m \"updated td-bulk-import-java $revname\""
git commit ./java/td-bulk-import-${vername}.jar ./java/td-bulk-import-java.version -m "updated td-bulk-import-java $revname" || exit 1

