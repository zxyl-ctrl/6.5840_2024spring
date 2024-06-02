package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        // 新日志条目之前一个日志条目的索引
	PrevLogTerm  int        // 新日志条目之前一个日志条目的任期
	Entries      []LogEntry // 需要复制的日志条目，当用于发送heartbeat消息时为空
	LeaderCommit int        // 领导者已经提交的最大日志索引
}

type AppendEntriesReply struct {
	Term        int // 用于当接收方发现发送方任期陈旧时，以使得Leader自己退出和更新任期
	Success     bool
	UpNextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(getGID(), "RF status:", rf.logs, args.Term, rf.currentTerm)

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.UpNextIndex = -1
		reply.Term = rf.currentTerm
		return
	}

	// fmt.Println(getGID(), "0")
	reply.UpNextIndex = -1
	reply.Term = args.Term

	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	// rf.votedFor = -1
	rf.votedNum = 0
	rf.heartbeatTime = time.Now()

	// fmt.Println(getGID(), "1", args, rf.logs)

	// 检查日志一致性
	// 如果传递参数的最小index比当前节点的日志的最大Index都大，或者最后一个日志的任期不同，
	// 表明该节点的日志在之前就不同，Leader需要调整更小的PrevLogIndex
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.UpNextIndex = rf.getLastIndex()
		return
	}
	// fmt.Println(getGID(), "2")
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 如果上一条日志的任期不同，需要向前搜索，找到任期相同的日志
		reply.UpNextIndex = rf.getMinIndexInOneTerm(rf.logs[args.PrevLogIndex].Term, args.PrevLogIndex)
		return
	}

	// fmt.Println(getGID(), "3", rf.logs)
	reply.Success = true
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = minInt(args.LeaderCommit, rf.getLastIndex())
	}

	// fmt.Println(getGID(), "status:", rf.commitIndex, rf.logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	// fmt.Println("Start Entries", len(rf.logs), rf.logs, rf.currentTerm, rf.matchIndex, rf.nextIndex, rf.me)
	for server := range rf.peers {
		if server != rf.me && rf.state == LEADER {
			go func(server int) {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.logs[prevLogIndex].Term
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: rf.commitIndex,
				}
				if rf.nextIndex[server] <= rf.getLastIndex() {
					args.Entries = rf.logs[rf.nextIndex[server]:]
				} else {
					args.Entries = []LogEntry{}
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// fmt.Println("args:", getGID(), args)
					// fmt.Println("reply:", getGID(), reply)
					if rf.state != LEADER || reply.Term < rf.currentTerm || rf.currentTerm != args.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.votedNum = 0
						rf.heartbeatTime = time.Now()
						return
					}

					if reply.Success {
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						// fmt.Println("Goroutine:", getGID(), rf.matchIndex[server], rf.nextIndex[server])

						for index := rf.getLastIndex(); index > 0; index-- {
							count := 1
							for i := range rf.peers {
								if i != rf.me && rf.matchIndex[i] >= index {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.logs[index].Term == rf.currentTerm {
								rf.commitIndex = index
								break
							}
						}
					} else {
						if reply.UpNextIndex != -1 {
							rf.nextIndex[server] = reply.UpNextIndex
						}
					}
				}
			}(server)
		}
	}
}
