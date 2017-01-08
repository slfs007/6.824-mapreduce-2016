export "GOPATH=$PWD"  # go needs $GOPATH to be set to the project's working directory
echo "GOPATH=$GOPATH"
cd "$GOPATH/src/mapreduce"

go test -v -run Sequential mapreduce/...
