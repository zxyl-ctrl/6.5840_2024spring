package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqID    int
	ClientID int64
	OpType   int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ReqID    int
	ClientID int64
}

type Reply struct {
	Err   Err
	Value string
}

const (
	GET int = iota
	PUT
	APPEND
)

const (
	timerDelay = 100
)
