package raft

import (
	"crypto/rand"
	"math/big"
)

// 不同操作的延时时间(ms)
const (
	// 时间太小系统不稳定
	// LAB4 Quick but RPCs num over 60
	MaxVoteTime int64 = 100
	MinVoteTime int64 = 80

	HeartbeatSleep = 30
	AppliedSleep   = 15

	clockDriftBound = 2                                                 // 调整单次租约时间增加量的系数
	deltaAdd        = (MaxVoteTime + MinVoteTime) / 2 / clockDriftBound // 单次租约时间增加量

	// voteRange  int = 80 // vote概率上界
	// entryRange int = 80 // entry概率上界
	// voteDelta  int = 5  // 单次概率上涨的值
	// entryDelta int = 5  // 单次概率上涨的值
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

// 理想方法是提前生成好一批随机数保存在文件中，提前读取并存储
// 读完后，从一个随机的下标继续开始读取
func nrand() int {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return int(x % 100)
}
