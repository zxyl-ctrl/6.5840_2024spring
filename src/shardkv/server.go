package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	ReqID    int
	OpType   Operation
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardID  int
	Shard    Shard
	ReqSet   map[int64]int
}

type Shard struct {
	StateMachine map[string]string // KV MAP
	ConfigNum    int               // Shard对应的版本配置号
}

type OpReply struct {
	ClientID int64
	SeqID    int
	Err      Err
}

const (
	UpConfigLoopInterval = 100 * time.Millisecond
	GetTimeout           = 500 * time.Millisecond
	AppOrPutTimeout      = 500 * time.Millisecond
	UpConfigTimeout      = 500 * time.Millisecond
	AddShardsTimeout     = 500 * time.Millisecond
	RemoveShardsTimeout  = 500 * time.Millisecond
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // 标注每个服务器属于的群组
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32

	LastConfig shardctrler.Config
	Config     shardctrler.Config

	shardsPersist []Shard // 持久化存储的Shard
	chMap         map[int]chan OpReply
	ReqSet        map[int64]int
	mck           *shardctrler.Clerk // sck is a client used to contact shard master

	rubbishCh chan int
}

func (kv *ShardKV) Get(args *GetArgs, reply *Reply) {
	// Your code here.
	shardID := key2shard(args.Key)

	kv.mu.Lock()
	// 检查当前服务器是否属于该分片的组，或者没有初始化
	if kv.Config.Shards[shardID] != kv.gid || kv.shardsPersist[shardID].StateMachine == nil {
		fmt.Println("[ERROR] somes errors in shard or shard is not initialize")
		reply.Err = ShardError
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{OpType: GetType, ClientID: args.ClientID, ReqID: args.RequestID, Key: args.Key}
	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}

	kv.mu.Lock()
	if kv.Config.Shards[shardID] != kv.gid || kv.shardsPersist[shardID].StateMachine == nil {
		fmt.Println("[ERROR] somes errors in shard or shard is not initialize")
		reply.Err = ShardError
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardID].StateMachine[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) {
	// Your code here.
	shardID := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardID] != kv.gid || kv.shardsPersist[shardID].StateMachine == nil {
		fmt.Println("[ERROR] somes errors in shaed or shard is not initialize")
		reply.Err = ShardError
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{OpType: args.Op, ClientID: args.ClientID, ReqID: args.RequestID, Key: args.Key, Value: args.Value}
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
}

func (kv *ShardKV) AddShard(args *SendShardArg, reply *Reply) {
	command := Op{OpType: AddShardType, ClientID: args.ClientID, ReqID: args.RequestID, ShardID: args.ShardID,
		Shard: args.Shard, ReqSet: args.LastAppliedRequestID}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// leader服务器处理相关的Req
func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if !kv.killed() {
			msg := <-kv.applyCh
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{ClientID: op.ClientID, SeqID: op.ReqID, Err: OK}
				// 在持久化存储中存储相关数据
				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {
					shardID := key2shard(op.Key) // 根据key获得需要存储的shard
					if kv.Config.Shards[shardID] != kv.gid || kv.shardsPersist[shardID].StateMachine == nil {
						fmt.Println("[ERROR] somes errors in shaed or shard is not initialize")
						reply.Err = ShardError
					} else {
						lastReqID, ok := kv.ReqSet[op.ClientID]
						if !ok || lastReqID < op.ReqID {
							kv.ReqSet[op.ClientID] = op.ReqID
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardID].StateMachine[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardID].StateMachine[op.Key] += op.Value
							default:
							}
						}
					}
				} else {
					// request from server of other group
					switch op.OpType {
					case UpConfigType:
						kv.upConfigHandler(op)
					case AddShardType:
						if kv.Config.Num < op.ReqID {
							reply.Err = ConfigOutdated
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						kv.removeShardHandler(op)
					default:
					}
				}

				// 快照变更
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}
		}
	}
}

func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		// leader所有数据最新，所以当冲突时，
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		if !kv.allSent() {
			ReqSet := make(map[int64]int)
			for k, v := range kv.ReqSet {
				ReqSet[k] = v
			}
			// 处理由于配置变更导致的shard不同导致的变更，需要在其他服务器上实现
			for shard, gid := range kv.LastConfig.Shards {
				// 对于当前组（kv.gid）的分片，如果新配置中的对应分片不属于当前组，
				// 并且当前分片的配置号小于新配置的配置号，说明该分片需要被发送给其他组。
				if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
					// 拷贝分片的数据
					sendDate := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shard].StateMachine)
					args := SendShardArg{
						LastAppliedRequestID: ReqSet,
						ShardID:              shard,
						Shard:                sendDate,
						ClientID:             int64(gid),
						RequestID:            kv.Config.Num,
					}
					// shardId -> gid -> server names 找出分片对于的复制组的服务器集合
					serversList := kv.Config.Groups[kv.Config.Shards[shard]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}
					// 使用 goroutine 将切片发送给对应的复制组的所有服务器
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply Reply
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)
							if ok && reply.Err == OK || time.Since(start) >= 2*time.Second {
								// 如果成功将切片发送给对应的服务器，GC 掉不属于自己的切片
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientID: int64(kv.gid),
									ReqID:    kv.Config.Num,
									ShardID:  args.ShardID,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							// 如果已经发送了一轮
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		if !kv.allReceived() { // 检测发送的是否由其他集群接收处理
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval) // 延时等待处理完
			continue
		}

		curConfig = kv.Config
		sck := kv.mck
		kv.mu.Unlock()
		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientID: int64(kv.gid),
			ReqID:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.ReqSet = make(map[int64]int)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.chMap = make(map[int]chan OpReply)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.rubbishCh = make(chan int, 1<<16)

	go kv.applyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()
	go kv.deletemap()
	return kv
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	ch, exist := kv.chMap[index]
	if !exist {
		kv.chMap[index] = make(chan OpReply, 1)
		ch = kv.chMap[index]
	}
	return ch
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()
	timer := time.NewTimer(timeoutPeriod)
	defer timer.Stop()
	select {
	case re := <-ch:
		kv.rubbishCh <- index
		if re.SeqID != command.ReqID || re.ClientID != command.ClientID {
			return ErrInconsistentData
		}
		return re.Err
	case <-timer.C:
		return ErrOverTime
	}
}

func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shardsPersist) != nil || e.Encode(kv.ReqSet) != nil ||
		e.Encode(kv.maxraftstate) != nil || e.Encode(kv.Config) != nil ||
		e.Encode(kv.LastConfig) != nil {
		fmt.Println("[ERROR] some errors happened in the dataEncoding")
	}
	return w.Bytes()
}

func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
	} else {
		kv.shardsPersist = shardsPersist
		kv.ReqSet = SeqMap
		kv.maxraftstate = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

// 上传一个新的config
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardsPersist[shard].StateMachine = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig
}

// 添加一个Shard
func (kv *ShardKV) addShardHandler(op Op) {
	if kv.shardsPersist[op.ShardID].StateMachine != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardID] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.StateMachine)
	for clientID, seqID := range op.ReqSet {
		if r, ok := kv.ReqSet[clientID]; !ok || r < seqID {
			kv.ReqSet[clientID] = seqID
		}
	}
}

// 移除一个Shard
func (kv *ShardKV) removeShardHandler(op Op) {
	if op.ReqID < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardID].StateMachine = nil
	kv.shardsPersist[op.ShardID].ConfigNum = op.ReqID
}

func (kv *ShardKV) allSent() bool {
	// 如果这个群组有新的配置
	for shard, gid := range kv.LastConfig.Shards {
		// 由于配置变更，如果属于当前集群shard对应的gid不属于这个集群，且配置号小，表示需要发送给其他组
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// gid != kv.gid                                     -> 在旧的配置中不属于当前复制组,也就是在现有的配置中可能需要被当前服务器接收
		// kv.Config.Shards[shard] == kv.gid                 -> 切片属于当前复制组
		// kv.shardsPersist[shard].ConfigNum < kv.Config.Num -> 当前分片的配置号小于新配置的配置号，说明该分片需要被接收
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {
	// 将shard进行深拷贝
	migrateShard := Shard{
		StateMachine: make(map[string]string),
		ConfigNum:    ConfigNum,
	}
	for k, v := range KvMap {
		migrateShard.StateMachine[k] = v
	}
	return migrateShard
}

func (kv *ShardKV) deletemap() {
	for {
		time.Sleep(400 * time.Millisecond)
		kv.mu.Lock()
		flag := true
		for flag {
			select {
			case id := <-kv.rubbishCh:
				delete(kv.chMap, id)
			default:
				flag = false
			}
		}
		kv.mu.Unlock()
	}
}
