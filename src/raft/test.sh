#!/bin/bash
# 这个测试还是有问题
for i in {1..5}
do
    echo "Iteration: $i"
    # go test ./raft -run TestBackup3B -count=1
    # go test ./raft -run TestFigure83C -count=1

    # go test ./raft -run TestRPCBytes3B -count=1
    # go test ./raft -run TestBasicAgree3B -count=1
    # go test ./raft -run TestFollowerFailure3B -count=1

    go test ./raft -run Test.*3D -count=1 
done