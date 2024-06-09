package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	reqSet map[int64]int   // ClientID -> reqID
	chMap  map[int]chan Op // reqID -> chan Op

	rubbishCh chan int
}

type Op struct {
	// Your data here.
	ReqID    int
	ClientID int64
	OpType   int
	// Data     interface{} gob不支持map结构体的interface{}
	JoinData  map[int][]string
	LeaveData []int
	MoveData  []int
	QueryNum  int
}

// 负载均衡，按照GroupMap的长度及逆行均衡划分
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	n := len(GroupMap)
	ave, remainder := NShards/n, NShards%n

	gidSorts := []int{}
	for gid := range GroupMap {
		gidSorts = append(gidSorts, gid)
	}
	sort.Slice(gidSorts, func(i, j int) bool {
		return GroupMap[gidSorts[i]] > GroupMap[gidSorts[j]] ||
			GroupMap[gidSorts[i]] == GroupMap[gidSorts[j]] && gidSorts[i] < gidSorts[j]
	})

	for _, gid := range gidSorts {
		target := ave
		if remainder > 0 {
			target++
			remainder--
		}

		delta := GroupMap[gid] - target
		if delta > 0 {
			for shard, tgid := range lastShards {
				if delta <= 0 {
					break
				}
				if gid == tgid {
					lastShards[shard] = 0
					delta--
				}
			}
			GroupMap[gid] = target
		} else {
			delta = -delta
			for shard, tgid := range lastShards {
				if delta <= 0 { // 为什么需要小于0
					break
				}
				if tgid == 0 {
					lastShards[shard] = gid
					delta--
				}
			}
			GroupMap[gid] = target
		}
	}

	return lastShards
}

func (sc *ShardCtrler) process(t int, op Op, reply *Reply) {
	lastIndex, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(lastIndex)
	timer := time.NewTimer(times[t] * time.Millisecond)

	select {
	case replyOp := <-ch:
		if op.ClientID != replyOp.ClientID || op.ReqID != replyOp.ReqID {
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			if replyOp.OpType == QUERY {
				sc.mu.Lock()
				sc.reqSet[op.ClientID] = op.ReqID
				queryNum := op.QueryNum
				if queryNum == -1 || queryNum >= len(sc.configs) {
					reply.Config = sc.configs[len(sc.configs)-1]
				} else {
					reply.Config = sc.configs[queryNum]
				}
				sc.mu.Unlock()
			}
		}
	case <-timer.C:
		reply.WrongLeader = true
	}

	timer.Stop()
	sc.rubbishCh <- lastIndex
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *Reply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{ReqID: args.ReqID, ClientID: args.ClientID, OpType: JOIN, JoinData: args.Servers}
	sc.process(0, op, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *Reply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{ReqID: args.ReqID, ClientID: args.ClientID, OpType: LEAVE, LeaveData: args.GIDs}
	sc.process(1, op, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *Reply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{ReqID: args.ReqID, ClientID: args.ClientID, OpType: MOVE, MoveData: []int{args.GID, args.Shard}}
	sc.process(2, op, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *Reply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{ReqID: args.ReqID, ClientID: args.ClientID, OpType: QUERY, QueryNum: args.Num}
	sc.process(3, op, reply)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.reqSet = map[int64]int{}
	sc.chMap = map[int]chan Op{}

	sc.rubbishCh = make(chan int, 1<<16)

	go sc.applyMsgHandlerLoop()
	go sc.deletemap()

	return sc
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.chMap[index]
	if !exist {
		sc.chMap[index] = make(chan Op, 1)
		ch = sc.chMap[index]
	}
	return ch
}

func (sc *ShardCtrler) deletemap() {
	for {
		time.Sleep(400 * time.Millisecond)
		sc.mu.Lock()
		flag := true
		for flag {
			select {
			case id := <-sc.rubbishCh:
				delete(sc.chMap, id)
			default:
				flag = false
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			sc.mu.Lock()
			lastReqID, ok := sc.reqSet[op.ClientID]
			if !ok || lastReqID < op.ReqID {
				switch op.OpType {
				case JOIN:
					sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinData))
				case LEAVE:
					sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveData))
				case MOVE:
					sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveData))
				}
				sc.reqSet[op.ClientID] = op.ReqID
			}
			sc.mu.Unlock()
			sc.getWaitCh(index) <- op
		}
	}
}

// 重新分片的要求，尽可能平均的分配分片，并尽可能少的移动分片，并尽可能允许gid重用
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]

	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	// 创建有关所有的GroupMap，包含新的GroupMap的所有的gid，并统计旧的shards中包含的信息
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	newShards := sc.loadBalance(GroupMap, lastConfig.Shards)
	return &Config{
		Num:    len(sc.configs),
		Shards: newShards,
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}
	lastConfig := sc.configs[len(sc.configs)-1]

	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		if _, ok := leaveMap[gid]; !ok {
			newGroups[gid] = serverList
		}
	}

	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}
	newShard := lastConfig.Shards
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				GroupMap[gid]++
			}
		}
	}
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) MoveHandler(tdata []int) *Config {
	gid, shard := tdata[0], tdata[1]
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{Num: len(sc.configs),
		Shards: lastConfig.Shards,
		Groups: map[int][]string{}}

	newConfig.Shards[shard] = gid

	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}

	return &newConfig
}
