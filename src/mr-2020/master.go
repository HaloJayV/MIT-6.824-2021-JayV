package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterTaskStatus int

// 枚举，表示任务执行阶段，根据论文，分为空闲、执行中、已完成
const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

type State int

// 枚举，表示Master和Task的状态
const (
	Map State = iota
	Reduce
	Exit
	Wait
)

// 任务对象
type Task struct {
	Input         string   // 任务负责处理的输入文件名
	TaskState     State    // 任务状态
	NReducer      int      // R个Reducer
	TaskNumber    int      // TaskId
	Intermediates []string // 保存Map任务产生的R个中间文件的磁盘路径
	Output        string   // 输出文件名
}

// Master记录的任务信息，包含任务执行阶段、任务开始时间，Task对象的指针
type MasterTask struct {
	TaskStatus    MasterTaskStatus // 任务执行阶段
	StartTime     time.Time        // 任务开始执行时间
	TaskReference *Task            // 表示当前执行的是哪个任务
}

// Master节点对象
type Master struct {
	TaskQueue     chan *Task          // 保存Task的队列，通过channel通道实现队列
	TaskMeta      map[int]*MasterTask // 当前系统所有task的信息，key为taskId
	MasterPhase   State               // Master阶段
	NReduce       int                 // R个Reduce工作线程
	InputFiles    []string            // 输入文件名
	Intermediates [][]string          // M行R列的二维数组，保存Map任务产生的M*R个中间文件
}

// 互斥锁
var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// 注册服务并监听socket
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	// 注册Master对象的所有方法，表明该Master就是服务器
	rpc.Register(m)
	// 采用HTTP协议
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	//监听该socket
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 创建http服务器并监听socket
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := m.MasterPhase == Exit
	return ret
}

// 创建Map任务
func (m *Master) createMapTask() {
	// 遍历所有的输入文件，每个文件用一个Map任务处理
	for idx, fileName := range m.InputFiles {
		// 创建Map Task对象
		taskMeta := Task{
			Input:      fileName,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		// Task对象放入队列
		m.TaskQueue <- &taskMeta
		// 填充Master对当前队列中所有Task的信息, taskId为key，value保存task信息
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// crash，启动一个协程来不断检查超时的任务
func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		// 锁住其他线程可能会使用的m.MasterPhase
		mu.Lock()
		// Master节点的执行状态是退出状态，则退出检查
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		// 检查所有任务
		for _, masterTask := range m.TaskMeta {
			// 任务执行中并且执行时间大于10秒，则重新放入队列等待被其他worker执行
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// 创建Master节点，负责分发任务，作为服务注册中心、服务调度中心
func MakeMaster(files []string, nReduce int) *Master {
	// 创建Master节点
	m := Master{
		// 保存task的队列，通过chan通道实现先进先出
		TaskQueue: make(chan *Task, max(nReduce, len(files))),
		// 主要作用是通过taskId这个key获取到对应的Task信息
		TaskMeta: make(map[int]*MasterTask),
		// 一开始Master和Task都处于Map阶段
		MasterPhase: Map,
		NReduce:     nReduce,
		InputFiles:  files,
		// 创建二维数组保存Map阶段生成的中间文件路径，设置列数为nReduce
		Intermediates: make([][]string, nReduce),
	}
	// TODO 将files中的文件切分成16MB-64MB的文件

	// 创建Map任务
	m.createMapTask()
	// 启动Master节点，将Master的方法都注册到注册中心，worker就可以通过RPC访问Master的方法
	m.server()
	// crash，启动一个协程来不断检查超时的任务
	go m.catchTimeOut()
	return &m
}

// 等待worker通过rpc请求Master的服务
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// 锁住Master节点
	mu.Lock()
	defer mu.Unlock()
	// 队列里还有空闲任务
	if len(m.TaskQueue) > 0 {
		// taskQueue还有空闲的task就发出一个Task指针给一个worker
		*reply = *<-m.TaskQueue
		// 设置Task状态
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		// 队列里还有任务但是Master状态为Exit
		// 返回一个带着Exit状态的Task，表示Master已经终止服务了
		*reply = Task{
			TaskState: Exit,
		}
	} else {
		// 队列里没有任务，则让请求的worker等待
		*reply = Task{
			TaskState: Wait,
		}
	}
	return nil
}

// 更新Task状态为已完成并检查
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	// 容错、检查节点状态、检查重复任务
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 重复任务要丢弃
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

// master通过协程获取任务执行的结果
func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		// Map阶段则收集中间结果到Master内存中
		// key为taskId，value为文件路径的字符串数组，一个task有NReducer个filePath
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		// 所有任务已完成则进入reduce阶段
		if m.allTaskDone() {
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		// Reduce则设置状态为Exit
		if m.allTaskDone() {
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

// 创建Reduce 任务
func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	// 根据所有中间文件来创建NReducer个任务
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		// 将任务放入队列，并重新设置Master的Task信息
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}
