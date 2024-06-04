package raft

import "time"

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// 输入的index表示真实日志下标
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.lastIncludedIndex >= index || rf.commitIndex < index {
			return
		}

		rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex].Term
		slogs := []LogEntry{}
		slogs = append(slogs, LogEntry{Term: rf.lastIncludedTerm, Command: nil})
		slogs = append(slogs, rf.logs[index+1-rf.lastIncludedIndex:]...)

		rf.lastIncludedIndex = index
		rf.logs = slogs
		rf.persister.Save(rf.persistData(), snapshot)
	}
}

// 领导者发送快照给状态落后的Follower
func (rf *Raft) sendSnapShot(server int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludedIndex,
		LastIncludeTerm:  rf.lastIncludedTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != LEADER || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm { // reply的任期大，更新自己为Follower
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.votedNum = 0
			rf.persist()
			rf.heartbeatTime = time.Now()
			return
		}

		// Follower快照更新，需要更新相关的logs
		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 根据Leader的快照更新自己的快照，状态，并舍弃部分/全部自己的日志
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term
	rf.state = FOLLOWER
	rf.persist()
	rf.heartbeatTime = time.Now()

	if rf.lastIncludedIndex >= args.LastIncludeIndex { // Follow的快照更新(more new)
		return
	}

	index := args.LastIncludeIndex
	templog := []LogEntry{}
	templog = append(templog, LogEntry{Term: args.LastIncludeTerm, Command: nil})

	// 尝试复制在快照后的日志，如果最后的日志的下标快照前，则不进行复制
	// index+1表示快照的最后一条日志的下标+1，即原先日志的第一条
	if index+1 <= rf.getLastIndex() {
		templog = append(templog, rf.logs[index+1-rf.lastIncludedIndex:]...)
	}

	rf.logs = templog
	rf.lastIncludedTerm = args.LastIncludeTerm
	rf.lastIncludedIndex = args.LastIncludeIndex

	// 快照复制成功，更新自己的状态
	rf.commitIndex = maxInt(rf.commitIndex, index)
	rf.lastApplied = maxInt(rf.lastApplied, index)

	rf.persister.Save(rf.persistData(), args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.chanApply <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
