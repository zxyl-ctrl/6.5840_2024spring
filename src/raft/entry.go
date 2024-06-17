package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // 领导者的当前任期
	LeaderId     int        // 领导者id
	PrevLogIndex int        // 发送日志上一条日志的Index
	PrevLogTerm  int        // 发送日志上一条日志的Term
	Entries      []LogEntry // Log数组，当发送心跳时为空
	LeaderCommit int        // 领导者已经Commit的最大日志索引

	LeaseTime time.Time // 领导者当前已知的最长租约时间
}

type AppendEntriesReply struct {
	Term        int  // Follower的当前Term，用于当接收方发现发送方任期陈旧时，以使得Leader自己退出和更新任期
	Success     bool // 是否成功复制日志
	UpNextIndex int  // 用于失败时返回Leader下一个应当发送的Index，为-1时白哦是当前操作不合理
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

	reply.UpNextIndex = -1
	reply.Term = args.Term

	rf.state = FOLLOWER

	rf.currentTerm = args.Term
	rf.persist()
	rf.heartbeatTime = time.Now() // 设置心跳，避免处理时投票

	if rf.leaseTime.Before(args.LeaseTime) {
		rf.leaseTime = args.LeaseTime
	}

	// fmt.Println(getGID(), "1", args, rf.logs)

	// 处理发送Entry延迟但是已经更新过Snapshot的情况
	// 可以赋值给下面两个值均可，前者在探查正确的Index时需要耗费时间，后者在复制上需要耗费时间
	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.UpNextIndex = rf.getLastIndex() // rf.lastIncludedIndex + 1
		return
	}

	// NOTE：在不能成功完成日志的复制时而进行不断的搜索时，args.PrevLohIndex必然是不断减少的
	// 因为重复的日志即使多复制一点也不会引起状态改变

	// 如果当前节点的最后一个日志的下标比上一条发送的日志的下标要小，设置返回的reply.UpNextIndex
	// 为当前最后的日志下标 + 1
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.UpNextIndex = rf.getLastIndex() + 1 // rf.getLastIndex()
		return
	}
	if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// 如果上一条日志的任期不同，需要向前搜索，找到任期不同的日志对应的下标
		reply.UpNextIndex = rf.getMinIndexInOneTerm(rf.restoreLogTerm(args.PrevLogIndex),
			args.PrevLogIndex)
		return
	}

	// fmt.Println(getGID(), "3", rf.logs)
	reply.Success = true
	rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()
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
	for server := range rf.peers {
		if server != rf.me { // 不能够判断是否是LEADER，因为没上锁

			// 只有这个函数会使用rf.entryP，不需要上锁
			// if nrand() > rf.entryP[server] {
			// 	rf.entryP[server] += entryDelta
			// 	continue
			// } else {
			// 	rf.entryP[server] = entryRange
			// }

			go func(server int) {
				rf.mu.Lock()

				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				// 发现Follower的matchIndex[i]落后自己，发送快照
				if rf.nextIndex[server]-1 < rf.lastIncludedIndex {
					go rf.sendSnapShot(server)
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.restoreLogTerm(prevLogIndex)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: rf.commitIndex,
					LeaseTime:    rf.leaseTime,
				}
				if rf.nextIndex[server] <= rf.getLastIndex() {
					args.Entries = rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]
				} else {
					args.Entries = []LogEntry{}
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 不是领导者；或者响应延迟导致reply的任期<当前任期
					if rf.state != LEADER || reply.Term < rf.currentTerm {
						return
					}

					if reply.Term > rf.currentTerm { // 任期落后，需要更新
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.votedNum = 0
						rf.persist()
						rf.heartbeatTime = time.Now()
						return
					}

					if rf.currentTerm != args.Term {
						return
					}

					rf.leaseCount++

					if reply.Success {
						// 不考虑快照偏移的情况下
						// rf.matchIndex顶多为len(rf.logs) - 1, rf.matchIndex()顶多为len(rf.logs)
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1

						for index := rf.getLastIndex(); index > rf.lastIncludedIndex; index-- {
							count := 1
							for i := range rf.peers {
								if i != rf.me && rf.matchIndex[i] >= index {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.restoreLogTerm(index) == rf.currentTerm {
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

	// 更新租约属性
	rf.mu.Lock()
	if rf.leaseCount > len(rf.peers) {
		// rf.leaseTime = rf.heartStartTime.Add(time.Duration(deltaAdd) * time.Millisecond)
		rf.leaseTime = rf.leaseTime.Add(time.Duration(deltaAdd) * time.Millisecond)
	}
	rf.leaseCount = 0
	rf.mu.Unlock()
}
