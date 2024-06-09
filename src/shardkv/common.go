package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Operation string
type Err string

const (
	OK         = "OK"
	ShardError = "ShardError"
	SERR       = "some_errors"

	// ErrNoKey            = "ErrNoKey"
	// ErrWrongGroup = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	// ShardNotReady       = "ShardNotReady"
	ErrInconsistentData = "ErrInconsistentData"
	ErrOverTime         = "ErrOverTime"
	ConfigOutdated      = "ConfigOutdated"
)

const (
	PutType         Operation = "Put"
	AppendType      Operation = "Append"
	GetType         Operation = "Get"
	UpConfigType    Operation = "UpConfig"
	AddShardType    Operation = "AddShard"
	RemoveShardType Operation = "RemoveShard"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    Operation // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64
	RequestID int
}

type SendShardArg struct {
	LastAppliedRequestID map[int64]int // for receiver to update its state
	ShardID              int
	Shard                Shard // Shard to be sent
	ClientID             int64
	RequestID            int
}

type Reply struct {
	Err   Err
	Value string
}
