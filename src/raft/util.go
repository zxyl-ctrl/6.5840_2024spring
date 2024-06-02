package raft

import (
	"log"
	"runtime"
	"strconv"
)

// Debugging
const isDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

func getGID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	id := parseGID(buf[:n])
	return id
}

func parseGID(stack []byte) int64 {
	const prefix = "goroutine "
	stack = stack[len(prefix):]
	for i, b := range stack {
		if b < '0' || b > '9' {
			stack = stack[:i]
			break
		}
	}
	id, _ := strconv.ParseInt(string(stack), 10, 64)
	return id
}
