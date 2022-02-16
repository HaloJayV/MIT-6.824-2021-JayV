package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const CONSENSUS_TIMEOUT = 500 // ms

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Operation = "Put" or "Append"
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu sync.Mutex
	me int
	// 每个KVServer对应一个Raft
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// 快照日志中，最后日志条目的State
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	// 保存put的数据，key : value
	kvDB map[string]string
	// index(Raft pper) -> chan
	waitApplyCh map[int]chan Op
	// clientId : requestId
	lastRequestId map[int64]int

	// last Snapshot point & raftIndex
	lastSSPointRaftLogIndex int
}

// RPC方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	// RaftServer必须是Leader
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: "get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// 向Raft server 发送命令
	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[GET StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// waitForCh
	kv.mu.Lock()
	// chForRaftIndex为保存Op的chan，raftIndex为Raft Server的LastLogIndex+1
	// 用于实现RPC调用Raft.Start时，保存RPC返回的Op，通过 Raft Server 的 lastLogIndex获取
	// 通过raft的lastLogIndex,就能得到该日志条目保存的value，并保存到KVDB中
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	// Loop Apply ，技术上要求线性化
	// 不存在该记录，表明调用还未返回结果，则继续等待调用返回
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	// RPC调用完成
	kv.mu.Unlock()

	// Timeout
	select {
	// 超过一致性要求的时间，则需要通过lastRequestId，从KVDB中获取结果
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)
		_, ifLeader := kv.rf.GetState()

		// 该client的最新RequestId是否是newRequestId，不是，则返回最新RequestId
		// 该步骤保证了client并发调用KVServer时，根据最新的RequestId，得到最新的结果
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader {
			// 根据命令获取该client最新RequestId得到并保存在KVDB的value
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	// 在一致性的有效时间内：
	case raftCommitOp := <-chForRaftIndex:
		DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		// 该已提交到Raft的RPC请求，是本次的Op命令
		if raftCommitOp.ClientId == op.ClientId &&
			raftCommitOp.RequestId == op.RequestId {
			// 则从KVServer的Map直接获取value
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	// Get结束后，删除chan map中raftIndex对应的Op
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

// RPC方法
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	// RaftServer必须是Leader
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// 向Raft server 发送命令
	raftIndex, _, _ := kv.rf.Start(op)
	DPrintf("[PUTAPPEND StartToRaft]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId, args.RequestId, kv.me, op.Key, raftIndex)

	// waitForCh
	kv.mu.Lock()
	// chForRaftIndex为保存Op的chan，raftIndex为Raft Server的LastLogIndex+1
	// 用于实现RPC调用Raft.Start时，保存RPC返回的Op，通过 Raft Server 的 lastLogIndex获取
	// 通过raft的lastLogIndex,就能得到该日志条目保存的value，并保存到KVDB中
	chForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	// Loop Apply ，技术上要求线性化
	// 不存在该记录，表明调用还未返回结果，则继续等待调用返回
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	// RPC调用完成
	kv.mu.Unlock()

	// Timeout
	select {
	// 超过一致性要求的时间，则需要通过lastRequestId，从KVDB中获取结果
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		DPrintf("[TIMEOUT PUTAPPEND !!!!]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)

		// 该client的最新RequestId是否是newRequestId，不是，则返回最新RequestId
		// 该步骤保证了client并发调用KVServer时，根据最新的RequestId，得到最新的结果
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	// 在一致性的有效时间内：
	case raftCommitOp := <-chForRaftIndex:
		DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)

		// 该已提交到Raft的RPC请求，是本次的Op命令
		if raftCommitOp.ClientId == op.ClientId &&
			raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 循环打印Map保存的所有数据
func (kv *KVServer) DprintfKVDB() {
	if !Debug {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, value := range kv.kvDB {
		DPrintf("[DBInfo ----]Key : %v, Value : %v", key, value)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
// 启动KVServer
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("[InitKVServer---]Server %d", me)
	// 注册rpc服务器
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// kv初始化
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	// 快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		// 读取快照日志
		kv.ReadSnapshotToInstall(snapshot)
	}
	// 循环读取Raft已经应用的日志条目命令
	go kv.ReadRaftApplyCommandLoop()
	return kv
}
