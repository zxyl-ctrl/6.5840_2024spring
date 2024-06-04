package raft

// 不同操作的延时时间(ms)
const (
	MaxVoteTime int64 = 100
	MinVoteTime int64 = 75

	HeartbeatSleep = 35
	AppliedSleep   = 15
)

// 处理真实日志经过快照偏移得到的实际日志下标

func (rf *Raft) restoreLogTerm(curIndex int) int {
	return rf.logs[curIndex-rf.lastIncludedIndex].Term
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getMinIndexInOneTerm(term int, endIndex int) (minIndex int) {
	// 扫描获得不是一个任期的起始下标
	// 虽然在entry.go中，当扫描失败时可以一个一个递减，但那样很慢，可以调整为一个一个任期递减
	minIndex = rf.lastIncludedIndex + 1
	for index := endIndex; index > rf.lastIncludedIndex; index-- {
		if rf.restoreLogTerm(index) != term {
			minIndex = index + 1
			break
		}
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
