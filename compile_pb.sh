#!/bin/bash
# libprotoc 3.12.0
# protoc-gen-go v1.24.0

cd $GOMODULEPATH
for i in $(ls $GOMODULEPATH/github.com/amazingchow/photon-dance-vector-space-searcher/pb/*.proto); do
	fn=github.com/amazingchow/photon-dance-vector-space-searcher/pb/$(basename "$i")
	echo "compile" $fn
	/usr/local/bin/protoc -I/usr/local/include -I . \
		-I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--go_out=plugins=grpc:. "$fn"
	/usr/local/bin/protoc -I/usr/local/include -I . \
		-I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--grpc-gateway_out=logtostderr=true:. "$fn"
	/usr/local/bin/protoc -I/usr/local/include -I . \
		-I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--swagger_out=logtostderr=true:. "$fn"
done
