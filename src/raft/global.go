package raft

import (
	"math"
)

// 不同操作的延时时间
const (
	MaxVoteTime int64 = 100
	MinVoteTime int64 = 75

	HeartbeatSleep = 35
	AppliedSleep   = 15
)

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getMinIndexInOneTerm(term int, endIndex int) (minIndex int) {
	if term == 0 {
		return 0
	}
	minIndex = math.MaxInt
	for index := endIndex; index >= 0; index-- {
		if rf.logs[index].Term != term {
			minIndex = index + 1
			break
		}
	}
	if minIndex == math.MaxInt {
		return 1
	}
	return minIndex
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
