#! /bin/bash -u

bundle install
rake exe:clean
rake exe:build
