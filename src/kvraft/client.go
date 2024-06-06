package kvraft

import (
	"crypto/rand"
	"math/big"

	mathrand "math/rand"

	"6.5840/labrpc"
)

// 为了避免一个命令被执行两次，可以额外添加一个序列用来标记该命令是否被执行过
// LEADER可能会在响应客户端宕机，客户端需要找下一个领导者处理请求，这可能会导致命令被执行两次

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	reqID    int
	leaderID int
	clientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	// 初始设置不同的leader服务器，防止客户端都访问同一台服务器导致流量过大
	ck.leaderID = mathrand.Intn(len(ck.servers))
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.reqID++
	// You will have to modify this function.
	args := &GetArgs{key, ck.reqID, ck.clientID}
	serverID := ck.leaderID
	for {
		reply := &Reply{}
		ok := ck.servers[serverID].Call("KVServer.Get", args, reply)
		if ok {
			if reply.Err == ErrNoKey || reply.Err == OK {
				ck.leaderID = serverID
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				continue
			}
		}
		serverID = (serverID + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.reqID++
	args := &PutAppendArgs{key, value, ck.reqID, ck.clientID, PUT}
	if op == "Append" {
		args.OpType = APPEND
	}
	serverID := ck.leaderID
	for {
		reply := &Reply{}
		ok := ck.servers[serverID].Call("KVServer.PA_Process", args, reply)
		if ok {
			if reply.Err == OK {
				ck.leaderID = serverID
				return
			} else if reply.Err == ErrWrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				continue
			}
		}
		// 轮询，向其他服务器请求消息，保证leader一开始宕机就进行切换
		serverID = (serverID + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
