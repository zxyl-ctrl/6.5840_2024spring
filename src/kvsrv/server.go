package kvsrv

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Debug = false

// 实现线性一致性
// 1. 同一个线程并不能同时获取互斥锁，如果A调用B，不使用goroutine，此时A先调用了锁，此时B调用锁会导致卡死
// 2. 如果A是结构体，使用map[type]A，type是内置的类型，需要此时json.Encode无法对A进行编码

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu        sync.Mutex
	kvMap     map[string]string  // key, value
	reqSet    map[int64]struct{} // token, 返回值，不需要保存Get
	appendMap map[int64]string

	filenameID int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reply.Value = kv.kvMap[args.Key]
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// PUT方法不需要返回任何值
	kv.mu.Lock()
	_, ok := kv.reqSet[args.Token]
	kv.reqSet[args.Token] = struct{}{}
	if !ok {
		kv.kvMap[args.Key] = args.Value
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldVal := kv.kvMap[args.Key]
	_, ok := kv.reqSet[args.Token]
	kv.reqSet[args.Token] = struct{}{}
	if ok {
		// reply.Value = kv.appendMap[args.Token].res
		val, ok := kv.appendMap[args.Token]
		if ok {
			reply.Value = val
		} else {
			reply.Value = kv.Readmap(args.Token, kv.filenameID)
		}
	} else {
		kv.appendMap[args.Token] = oldVal
		reply.Value = oldVal
		kv.kvMap[args.Key] = oldVal + args.Value
	}
}

func (kv *KVServer) Readmap(token int64, maxID int) string {
	for i := 0; i < maxID; i++ {
		temp := map[int64]string{}
		filename := "temp_" + strconv.Itoa(i) + ".json"
		data, err := os.ReadFile(filename)
		if err != nil {
			log.Fatal("[ERROR] An error in load Files", err)
		}
		if err := json.Unmarshal(data, &temp); err != nil {
			log.Fatal("[ERROR] An error in json_Unmarshal", err)
		}
		// fmt.Println("data load:", data)
		val, ok := temp[token]
		if ok {
			return val
		}
	}
	return ""
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = map[string]string{}
	// You may need initialization code here.
	kv.reqSet = make(map[int64]struct{})
	kv.appendMap = make(map[int64]string)
	kv.filenameID = 0

	files, _ := os.ReadDir("./")
	for _, file := range files {
		filename := file.Name()
		ext := filepath.Ext(filename)
		if strings.EqualFold(ext, ".json") {
			os.Remove(filename)
		}
	}

	go kv.monitor()
	return kv
}

// 最后一个测试没过，解决方法应当是将内容写入硬盘存储，并强制回收内存
func (kv *KVServer) monitor() {
	for {
		time.Sleep(500 * time.Millisecond)
		kv.mu.Lock()
		filename := "temp_" + strconv.Itoa(kv.filenameID) + ".json"
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal("Create file in Error!", err)
		}
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(kv.appendMap); err != nil {
			log.Fatal("Error encoding and writing JSON data:", err)
		}

		// fmt.Println("ORIGIN:", kv.appendMap)
		kv.appendMap = map[int64]string{} // 清空存储
		kv.filenameID++
		runtime.GC() // 回收内存
		kv.mu.Unlock()
		file.Close()
	}
}
