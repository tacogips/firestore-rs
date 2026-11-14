#!/bin/bash

WORK_DIR=$(dirname $0)
cd ${WORK_DIR}/..

rm -rf proto
mkdir proto
rm -rf _work
mkdir _work

curl -L https://github.com/googleapis/googleapis/archive/master.zip  -o _work/master.zip && unzip -q _work/master.zip -d _work
mv  _work/googleapis-master/google ./proto
find proto -type f -not -name '*.proto' -delete
rm -rf _work
