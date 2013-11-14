#!/bin/bash
cd "$(dirname $0)"
chrev="$1"

if [ -d td-import-java/.git ];then
    rm -rf td-import-java/*
    cd td-import-java
    git checkout . || exit 1
    git pull || exit 1
else
    rm -rf td-import-java/
    git clone git@github.com:treasure-data/td-import-java.git td-import-java || exit 1
    cd td-import-java
fi
git checkout master

if [ -n "$chrev" ];then
    git checkout $chrev
fi

revname="$(git show --pretty=format:'%H %ad' | head -n 1)"
vername="0.4.1-SNAPSHOT"

mvn package -Dmaven.test.skip=true || exit 1
echo "copy td-import-${vername}.jar"
cp target/td-import-${vername}-jar-with-dependencies.jar ../../java/td-import.jar
echo "copy logging.properties"
cp src/test/resources/java/logging.properties ../../java/logging.properties
echo "create VERSION file"
echo "${vername}" > VERSION
mv VERSION ../../java/VERSION

if [ -n "$chrev" ];then
    git checkout master
fi

cd ../../

echo "$revname" > java/td-import-java.version

echo ""
echo "git commit ./java -m \"updated td-import-java $revname\""
git commit ./java/td-import.jar ./java/td-import-java.version -m "updated td-import-java $revname" || exit 1

