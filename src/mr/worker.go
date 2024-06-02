package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 向Coordinator请求任务，并根据任务类型分别处理
	for {
		task := getTask()
		switch task.Type {
		case MapTask:
			Map(mapf, &task)
			callDone(&task)
		case ReduceTask:
			Reduce(reducef, &task)
			callDone(&task)
		case WaitingTask:
			fmt.Println("[INFO] waiting carrying on tasks")
			time.Sleep(1 * time.Second)
		case ExitTask:
			fmt.Println("[INFO] exit")
			goto exit
		}
	}
exit:
	fmt.Println("[INFO] The worker finished all Tasks")
}

func getTask() TaskResp {
	req, resp := TaskReq{}, TaskResp{}
	ok := call("Coordinator.AllocateTask", &req, &resp)
	if !ok {
		fmt.Printf("[ERROR] call failed\n")
	}
	return resp
}

func Map(dealMap func(string, string) []KeyValue, task *TaskResp) {
	// 读取文件中的全部内容；调用dealMap函数，返回[]keyValue；然后使用ihash将其分组写入
	fmt.Println(task, task.FileSlice)
	filename := task.FileSlice[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("[ERROR] A deally error happed in reading files")
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("[ERROR] An error happned in reading Errors")
	}
	file.Close()
	var mapResKV []KeyValue = dealMap(filename, string(content))
	rn := task.ReduceNum
	docKV := make([][]KeyValue, rn)
	for _, kv := range mapResKV {
		id := ihash(kv.Key) % rn
		docKV[id] = append(docKV[id], kv)
	}
	for i, data := range docKV {
		wFileName := "mr-tmp-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(i)
		file, err := os.Create(wFileName)
		if err != nil {
			log.Fatal("[ERROR] creating file in error: 102", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range data {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
	}
}

func Reduce(dealReduce func(string, []string) string, task *TaskResp) {
	// 读取文件，并进行排序后返回文件；合并文件后交由dealReduce处理；之后将处理完成的文件写入
	reduceFileNum := task.ID
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("[ERROR] Failed to create temp file", err)
	}

	kvSet := loadAndSort(task)
	lenKVSet := len(kvSet)
	for i := 0; i < lenKVSet; {
		j, mainKey, input := i, kvSet[i].Key, []string{}
		for j < lenKVSet {
			if kvSet[j].Key == mainKey {
				input = append(input, kvSet[j].Value)
				j++
			} else {
				break
			}
		}
		resStr := dealReduce(mainKey, input)
		fmt.Fprintf(tempFile, "%v %v\n", kvSet[i].Key, resStr)
		// 调整i，为下一次操作做准备
		i = j
	}
	tempFile.Close()

	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func loadAndSort(task *TaskResp) []KeyValue {
	// 读取文件，进行排序和合并
	var kvSlice []KeyValue
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < len(task.FileSlice); i++ {
		filename := task.FileSlice[i]
		wg.Add(1)
		go func(filename string) {
			file, _ := os.Open(filename)
			dec := json.NewDecoder(file)
			mu.Lock()
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvSlice = append(kvSlice, kv)
			}
			mu.Unlock()
			wg.Done()
		}(filename)
	}
	wg.Wait()
	sort.Slice(kvSlice, func(i, j int) bool { return kvSlice[i].Key < kvSlice[j].Key })
	return kvSlice
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, req interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, req, reply)
	if err == nil {
		return true
	}

	fmt.Println("[ERROR] An error happened during the process", err, time.Now())
	return false
}

func callDone(task *TaskResp) TaskResp {
	reply := TaskResp{}
	ok := call("Coordinator.SetTaskDone", task, &reply)
	if ok {
		fmt.Println("[INFO] close task: ", task.ID, task.Type)
	} else {
		fmt.Println("[ERROR] An UnKnown Error!")
	}
	// call("Coordinator.SetTaskDone", task, &reply)
	return reply
}
