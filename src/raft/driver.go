package raft

import (
	"math/rand"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		oldTime := time.Now()
		time.Sleep(time.Duration(rand.Int63()%(MaxVoteTime-MinVoteTime)+MinVoteTime) * time.Millisecond)
		rf.mu.Lock()
		// 设置Before防止因为执行其他事务，不便于进行选举
		if rf.heartbeatTime.Before(oldTime) && rf.state != LEADER {
			rf.state = CANDIDATE
			rf.votedFor = rf.me
			rf.votedNum = 1
			rf.currentTerm += 1
			rf.persist()
			rf.broadcastRequestVote()
			rf.heartbeatTime = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) SAE() {
	for !rf.killed() {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.broadcastAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) commitedTicker() { //第0条默认日志不需要提交，否则会报错
	for !rf.killed() {
		time.Sleep(time.Millisecond * AppliedSleep)
		rf.mu.Lock()

		msgs := []ApplyMsg{}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			log := rf.logs[rf.lastApplied-rf.lastIncludedIndex]
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      log.Command,
			})
		}

		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.chanApply <- msg
		}
	}
}
