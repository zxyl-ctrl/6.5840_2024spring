package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqID    int
	Key      string
	Value    string
	ClientID int64
	OpType   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap        map[string]string // key/value map
	reqUniqueMap map[int64]int     // ClientID/ReqID // 用于记录最新访问的ReqID
	waitChMap    map[int]chan Op   // index / Chan Op

	lastIncludeIndex int

	recycleCh chan int // 定时回收内存
}

func (kv *KVServer) initState(r *Reply) bool {
	if kv.killed() {
		r.Err = ErrWrongLeader
		return false
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		r.Err = ErrWrongLeader
		return false
	}

	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	// Your code here.
	if !kv.initState(reply) {
		return
	}

	op := Op{args.ReqID, args.Key, "", args.ClientID, GET}
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	timer := time.NewTimer(timerDelay * time.Millisecond)

	select {
	case replyOp := <-ch:
		if op.ClientID != replyOp.ClientID || op.ReqID != replyOp.ReqID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	timer.Stop()
	kv.recycleCh <- lastIndex
}

func (kv *KVServer) PA_Process(args *PutAppendArgs, reply *Reply) {
	// Your code here.
	if !kv.initState(reply) {
		return
	}

	op := Op{args.ReqID, args.Key, args.Value, args.ClientID, args.OpType}
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	timer := time.NewTimer(timerDelay * time.Millisecond)

	select {
	case replyOp := <-ch:
		if op.ClientID != replyOp.ClientID || op.ReqID != replyOp.ReqID {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	timer.Stop()
	kv.recycleCh <- lastIndex
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = map[string]string{}
	kv.reqUniqueMap = map[int64]int{}
	kv.waitChMap = map[int]chan Op{}

	kv.recycleCh = make(chan int, 1<<16)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandlerLoop()

	go kv.deletemap()

	return kv
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if !kv.killed() {
			msg := <-kv.applyCh
			if msg.CommandValid && msg.CommandIndex > kv.lastIncludeIndex {
				index := msg.CommandIndex
				op := msg.Command.(Op)

				kv.mu.Lock()
				lastReqID, ok := kv.reqUniqueMap[op.ClientID]
				if !ok || op.ReqID > lastReqID {
					switch op.OpType {
					case PUT:
						kv.kvMap[op.Key] = op.Value
					case APPEND:
						kv.kvMap[op.Key] += op.Value
					}
					kv.reqUniqueMap[op.ClientID] = op.ReqID
				}
				kv.mu.Unlock()

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				kv.getWaitCh(index) <- op
			}

			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.lastIncludeIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var reqUniqueMap map[int64]int

	if d.Decode(&kvMap) == nil && d.Decode(&reqUniqueMap) == nil {
		kv.kvMap = kvMap
		kv.reqUniqueMap = reqUniqueMap
	} else {
		log.Fatal("[ERROR] in DECODE snapshot!!!")
	}
}

func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.reqUniqueMap)
	data := w.Bytes()
	return data
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChMap[index]
	if !ok {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) deletemap() {
	for {
		time.Sleep(400 * time.Millisecond)
		kv.mu.Lock()
		flag := true
		for flag {
			select {
			case id := <-kv.recycleCh:
				delete(kv.waitChMap, id)
			default:
				flag = false
			}
		}
		kv.mu.Unlock()
	}
}
