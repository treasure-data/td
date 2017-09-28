#!/bin/bash -u
set -e

bundle install
bundle exec rake pkg:clean
bundle exec rake pkg:build
