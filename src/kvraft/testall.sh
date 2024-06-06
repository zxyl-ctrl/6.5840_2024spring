#!/bin/bash

for i in {1..500}
do
    echo "Iteration: $i"
	go test ./kvraft -run Test.*4A -count=1
	go test ./kvraft -run Test.*4B -count=1
done