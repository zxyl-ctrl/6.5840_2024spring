#!/bin/bash

for i in {1..500}
do
    echo "Iteration: $i"
	go test ./raft -run Test.*3A -count=1 
	go test ./raft -run Test.*3B -count=1 
	go test ./raft -run Test.*3C -count=1 
	go test ./raft -run Test.*3D -count=1 
done