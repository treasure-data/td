#!/bin/bash -u
set -e

PACKAGER="/usr/local/td/ruby/bin"

git pull
${PACKAGER}/gem install bundler rubyzip --no-rdoc --no-ri
rbenv rehash
${PACKAGER}/bundle install
${PACKAGER}/rake pkg:clean
${PACKAGER}/rake pkg:build
