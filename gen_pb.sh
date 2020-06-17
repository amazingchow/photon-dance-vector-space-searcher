#!/bin/bash
# libprotoc 3.12.0
# protoc-gen-go v1.24.0

basepath=$(cd `dirname $0`; pwd)

cd $basepath
for i in $(ls $basepath/pb/*.proto); do
	echo $i
	fn=$(basename "$i")
	/usr/local/bin/protoc -I/usr/local/include -I. \
        --proto_path=$basepath/pb \
        --go_out=$basepath/ "$fn"
done
