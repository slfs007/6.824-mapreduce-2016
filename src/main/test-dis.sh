#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
go test -v -run TestBasic mapreduce/...
