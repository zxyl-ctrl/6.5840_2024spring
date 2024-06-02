package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MapReduce是将Map和Reduce阶段分开处理的，而不是一边处理MAP一边处理REDUCE

type Coordinator struct {
	// Your definitions here.
	AllocatedID     int // 用于分配任务ID
	ReduceNum       int
	FileNames       []string
	MapTaskQueue    chan *TaskResp // channel先分配类型然后再分配缓存
	ReduceTaskQueue chan *TaskResp
	TaskSave        map[int]*TaskResp
	Phase           CoordinatorState
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(tReq *TaskReq, tResp *TaskResp) error {
	// fmt.Println("c.Phase:", c.Phase)
	switch c.Phase {
	case MapPhase:
		if len(c.MapTaskQueue) > 0 {
			(*tResp) = (*<-c.MapTaskQueue)
			c.adjustTask(tResp.ID)
		} else {
			c.scanTasks()
			tResp.Type = WaitingTask
		}
	case ReducePhase:
		if len(c.ReduceTaskQueue) > 0 {
			(*tResp) = (*<-c.ReduceTaskQueue)
			c.adjustTask(tResp.ID)
		} else {
			c.scanTasks()
			tResp.Type = WaitingTask
		}
	case Finished:
		tResp.Type = ExitTask
	default:
		fmt.Println("[ERROR] Undefined Phase!")
	}
	return nil
}

func (c *Coordinator) adjustTask(taskID int) {
	// 调整存储在coordinator中的Task状态和发送回worker的TaskResp，并根据情况进行调整
	task, ok := c.TaskSave[taskID]
	if !ok || task.State != Waiting {
		fmt.Println("[ERROR] Task Error", taskID)
		return
	}
	task.StartTime = time.Now()
	task.State = Working
}

func (c *Coordinator) scanTasks() {
	// 检查当前是否执行完成，如果执行完成，则切换状态
	c.mu.Lock() // test 3 error 忘记加锁了
	defer c.mu.Unlock()
	switch c.Phase {
	case MapPhase:
		numDone := 0
		for _, task := range c.TaskSave {
			if task.State == Done {
				numDone++
			}
		}
		fmt.Println("Scan:", numDone)
		if numDone == len(c.FileNames) { // Map阶段的任务已经全部完成
			c.loadReduceTasks() // 装载所有Reduce任务
			c.Phase = ReducePhase
		}
	case ReducePhase:
		numDone := 0
		for _, task := range c.TaskSave {
			if task.State == Done {
				numDone++
			}
		}
		if numDone == c.ReduceNum+len(c.FileNames) { // Reduce阶段的任务已经全部完成
			c.Phase = Finished
		}
	case Finished:

	}
}

func (c *Coordinator) loadReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		tTask := TaskResp{
			ID:        c.AllocatedID,
			Type:      ReduceTask,
			FileSlice: selectReduceName(i), // 前面第一个位置存储map读取的元素，后面的位置存储map生成的文件路径
			ReduceNum: c.ReduceNum,
			State:     Waiting,
			StartTime: time.Now(),
		}
		c.TaskSave[tTask.ID] = &tTask
		c.ReduceTaskQueue <- &tTask // 将Map任务发送给通道
		c.AllocatedID++
	}
}

func selectReduceName(reduceNum int) []string {
	// 获得工作路径下的所有"mr-tmp"+...+"strconv.Itoa(reduceNum)"
	var s []string
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) SetTaskDone(tReq *TaskResp, tResp *TaskResp) error {
	// 任务完成，更改任务的状态
	switch tReq.Type {
	case MapTask:
		tID := tReq.ID
		task, ok := c.TaskSave[tID]
		if !ok {
			return fmt.Errorf("[ERROR] No task like", tID)
		}
		if task.State == Working {
			task.State = Done
		}
	case ReduceTask:
		tID := tReq.ID
		task, ok := c.TaskSave[tID]
		if !ok {
			return fmt.Errorf("[ERROR] No task like", tID)
		}
		if task.State == Working {
			task.State = Done
		}
	default:
		return fmt.Errorf("[ERROR] receive unExpected task type")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	if c.Phase == Finished {
		time.Sleep(1 * time.Second) // 安全退出
		fmt.Println("[INFO] All tasks are finished")
		return true
	}
	// fmt.Println("Done:", c.Phase)
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 创建Coordinator，初始化相关任务，启动服务器，启动监视器
	c := Coordinator{
		AllocatedID:     1,
		ReduceNum:       nReduce,
		FileNames:       files,
		MapTaskQueue:    make(chan *TaskResp, len(files)),
		ReduceTaskQueue: make(chan *TaskResp, nReduce),
		TaskSave:        make(map[int]*TaskResp),
		Phase:           MapPhase,
	}

	// Your code here.
	c.makeTasks()
	c.server()
	go c.monitorWorker()
	return &c
}

func (c *Coordinator) makeTasks() {
	for _, filename := range c.FileNames {
		tTask := TaskResp{
			ID:        c.AllocatedID,
			Type:      MapTask,
			FileSlice: []string{filename}, // 前面第一个位置存储map读取的元素，后面的位置存储map生成的文件路径
			ReduceNum: c.ReduceNum,
			State:     Waiting,
			StartTime: time.Now(),
		}
		tTask.FileSlice[0] = filename
		c.TaskSave[tTask.ID] = &tTask
		c.MapTaskQueue <- c.TaskSave[tTask.ID] // 将Map任务发送给通道
		c.AllocatedID++
	}
}

func (c *Coordinator) monitorWorker() {
	// 检测函数是否超时，如果有，则回滚Task
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		if c.Phase == Finished {
			c.mu.Unlock()
			break
		}
		for key, value := range c.TaskSave {
			if value.State == Working && time.Since(value.StartTime) > 10*time.Second {
				fmt.Println("[INFO] the task", key, "is crash at", time.Since(value.StartTime).Seconds(), "s")
				switch value.Type {
				case MapTask:
					value.State = Waiting
					c.MapTaskQueue <- value
				case ReduceTask:
					value.State = Waiting
					c.ReduceTaskQueue <- value
				}
			}
		}
		c.mu.Unlock()
	}
}
