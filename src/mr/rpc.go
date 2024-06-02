package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskReq struct{}

type TaskResp struct {
	ID        int
	Type      TaskType // 处理任务类型
	FileSlice []string // 存储不同阶段的任务路径
	ReduceNum int
	State     TaskState // 当前任务的状态working, waiting, done
	StartTime time.Time
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

type TaskState int

const (
	Working TaskState = iota
	Waiting
	Done
)

type CoordinatorState int

const (
	MapPhase CoordinatorState = iota
	ReducePhase
	Finished
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
