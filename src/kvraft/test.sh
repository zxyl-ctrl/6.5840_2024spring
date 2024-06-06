#!/bin/bash
for i in {1..200}
do
    echo "Iteration: $i"
    # go test ./kvraft -run TestSpeed4A -count=1
    # go test ./kvraft -run TestSpeed4B -count=1
    go test ./kvraft -run TestSnapshotUnreliableRecoverConcurrentPartition4B -count=1
    go test ./kvraft -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4B -count=1
done