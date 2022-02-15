package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// 定义RPC请求时用到的常量，master.go修改方法名，则这里也要修改
const (
	callGetTask       = "Master.AssignTask"
	callTaskCompleted = "Master.TaskCompleted"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
// 启动Worker
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// 通过RPC获取空闲任务
		task := getTask()
		// 根据任务当前的执行状态进行相应处理
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

// 执行Reduce任务
func reducer(task *Task, reducef func(string, []string) string) {
	// 从磁盘中读取中间文件
	intermediate := *readFromLocalFile(task.Intermediates)
	// 根据key进行字典序排序
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	// 遍历每一个key
	for i < len(intermediate) {
		j := i + 1
		// 相同的key分组合并
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		// 保存该key的最终计数, 即对相同key的计数进行合并统计
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 结果交给reducef进行统计
		output := reducef(intermediate[i].Key, values)
		// 最终结果的字符串内容保存到临时文件里
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	// 定义输出文件的文件名
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

// 根据文件路径，从本地磁盘中读取文件内容
func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file"+filepath, err)
		}
		// json解码
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 解码并将内容放到kv中
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// 执行Map任务
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// 获取任务对应的文件路径
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	// 执行wc.go中的mapf方法，进行MapReduce的map阶段，得到nReduce个中间文件路径的字符串数组
	intermediates := mapf(task.Input, string(content))
	// 将map阶段生成的中间文件路径保存到列数为NReducer的二维数组中
	buffer := make([][]KeyValue, task.NReducer)
	// 保存结果到内存buffer中
	for _, intermediate := range intermediates {
		// 根据key进行hash，将结果切分成NReducer份
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	// 周期性地从内存保存到磁盘中
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		// 将中间结果写入到NReducer个中间临时文件中
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}
	// NReducer个文件的路径保存到内存，Master就可以获取到
	task.Intermediates = mapOutput
	// 设置该任务状态为已完成
	TaskCompleted(task)
}

func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	// 新建临时目录，存放临时文件
	dir, _ := os.Getwd()
	// 创建临时文件用来保存mapf函数的中间结果
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 将数据序列化为json格式并保存到临时文件tempFile中
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	// lab1要求的命名规则：mr-x-y
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	// 重命名
	os.Rename(tempFile.Name(), outputName)
	// 临时文件放入临时目录
	return filepath.Join(dir, outputName)
}

// 通过RPC获取空闲任务
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	// RPC请求调用Master的服务来获取Task
	call("Master.AssignTask", &args, &reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
