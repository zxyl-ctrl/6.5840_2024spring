package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选者任期
	CandidatedId int // 候选者的ID
	LastLogIndex int // 候选者最后一条日志的索引
	LastLogTerm  int // 候选者最后一条日志的任期
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 处理请求节点的任期号，用于候选者更新自己任期
	VoteGranted bool // 标记候选者是否能够获取选票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 候选者任期比自己小
	if args.Term < rf.currentTerm {
		return
	}
	// 候选者任期比自己大，将自己转化为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.votedNum = 0
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidatedId {
		if rf.getLastTerm() > args.LastLogTerm ||
			(rf.getLastTerm() == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex) {
			return
		}
		rf.votedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.heartbeatTime = time.Now()
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	for server := range rf.peers {
		if server != rf.me {

			// 只有这个函数会使用rf.voteP，不需要上锁
			// if nrand() > rf.voteP[server] {
			// 	rf.voteP[server] += entryDelta
			// 	continue
			// } else {
			// 	rf.voteP[server] = entryRange
			// }

			go func(server int) {
				rf.mu.Lock()
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidatedId: rf.me,
					LastLogIndex: rf.getLastIndex(),
					LastLogTerm:  rf.getLastTerm(),
				}
				rf.mu.Unlock()
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)

				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != CANDIDATE || args.Term < rf.currentTerm {
						return
					}

					if args.Term < reply.Term {
						if rf.currentTerm < reply.Term {
							rf.currentTerm = reply.Term
						}
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.votedNum = 0
						rf.persist()
						return
					}
					if reply.VoteGranted && rf.currentTerm == args.Term {
						rf.votedNum++
						if rf.votedNum > len(rf.peers)/2 {
							rf.state = LEADER
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							tnextIndex := rf.getLastIndex() + 1
							for i := range rf.nextIndex {
								rf.nextIndex[i] = tnextIndex
							}
							rf.matchIndex[rf.me] = rf.getLastIndex()

							rf.leaseTime = time.Now()
							rf.leaseCount = 0

							// for i := range rf.entryP {
							// 	rf.entryP[i] = entryRange
							// }
						}
					}
				}
			}(server)
		}
	}
}
