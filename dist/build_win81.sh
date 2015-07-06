#! /bin/bash -u

if [ $# -ne 1 ]; then
  echo "$0 TD_TOOLBELT_LOCAL_CLIENT_GEM" 1>&2
  exit 1
fi

bundle install
rake exe:clean
TD_TOOLBELT_LOCAL_CLIENT_GEM=$0 rake exe:build
