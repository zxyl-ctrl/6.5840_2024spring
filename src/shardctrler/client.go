package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	mathrand "math/rand"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clientID = nrand()
	ck.leaderID = mathrand.Intn(len(ck.servers))
	return ck
}

// 查询配置信息
func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.reqID++
	args := &QueryArgs{num, ck.reqID, ck.clientID}
	serverID := ck.leaderID
	for {
		// try each known server.
		reply := Reply{}
		ok := ck.servers[serverID].Call("ShardCtrler.Query", args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderID = serverID
				return reply.Config
			} else if reply.WrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				continue
			}
		}
		serverID = (serverID + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.reqID++
	args := &JoinArgs{servers, ck.reqID, ck.clientID}
	serverID := ck.leaderID
	for {
		// try each known server.
		reply := Reply{}
		ok := ck.servers[serverID].Call("ShardCtrler.Join", args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderID = serverID
				return
			} else if reply.WrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				continue
			}
		}
		serverID = (serverID + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.reqID++
	args := &LeaveArgs{gids, ck.reqID, ck.clientID}
	serverID := ck.leaderID
	for {
		// try each known server.
		reply := Reply{}
		ok := ck.servers[serverID].Call("ShardCtrler.Leave", args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderID = serverID
				return
			} else if reply.WrongLeader {
				serverID = (serverID + 1) % len(ck.servers)
				continue
			}
		}
		serverID = (serverID + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.reqID++
	args := &MoveArgs{shard, gid, ck.reqID, ck.clientID}
	serverId := ck.leaderID
	for {
		// try each known server.
		reply := Reply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderID = serverId
				return
			} else if reply.WrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
