#!/bin/bash
# 这个测试还是有问题
for i in {1..50}
do
    echo "Iteration: $i"
    go test ./raft -run TestBackup3B -count=1
done