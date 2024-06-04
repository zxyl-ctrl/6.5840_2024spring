package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type LogEntry struct {
	Term    int         // 客户端发送给领导者的任期
	Command interface{} // 客户端发送的Command
}

type STATUS int

const (
	LEADER STATUS = iota
	FOLLOWER
	CANDIDATE
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state    STATUS // 节点状态
	votedNum int    // 获得票数

	// 持久化变量
	currentTerm int        // 当前节点的任期
	votedFor    int        // 投票给谁
	logs        []LogEntry // 日志

	commitIndex int // commited的最高日志索引，表示已经被提交的最高日志条目的真实索引
	lastApplied int // 已经被应用到状态机的最大日志的真实索引, commitIndex>=lastApplied

	nextIndex  []int // 对于每一个服务器，下一个发送到不同服务器的日志的真实index
	matchIndex []int // 对于每一个服务器，已经发送到不同服务器的最大日志的真实Index

	chanApply     chan ApplyMsg // 应用command的通道
	heartbeatTime time.Time     // 心跳时间

	lastIncludedIndex int // 快照中包含的最后日志的真实Index
	lastIncludedTerm  int // 快照中包含的最后日志的真实Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) persistData() []byte {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted status.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any status?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm      int
		votedFor         int
		logs             []LogEntry
		lastIncludeIndex int
		lastIncludeTerm  int
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludeIndex
		rf.lastIncludedTerm = lastIncludeTerm
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 客户端向Leader发起请求
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != LEADER {
		return -1, -1, false
	}
	index, term := rf.getLastIndex()+1, rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.votedFor = -1

	initlog := LogEntry{0, nil} // 默认日志，后续的默认日志的Term用来存储rf.lastIncludedTerm
	rf.logs = []LogEntry{initlog}

	rf.chanApply = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.SAE()
	go rf.commitedTicker()

	// go rf.monitor()
	return rf
}

func (rf *Raft) monitor() {
	for {
		time.Sleep(1000 * time.Millisecond)
		fmt.Println(getGID(), rf.commitIndex, rf.logs, rf.state, rf.currentTerm)
	}
}
