package kvraft

import "6.824/raft"

// 该client的最新RequestId是否是newRequestId，不是，则返回最新RequestId
func (kv *KVServer) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	// 不存在newClientId记录
	if !ifClientInRecord {
		return false
	}
	// 该Client的最后一次RequestId
	return newRequestId <= lastRequestId
}

// 执行Op并返回从KVDB这个Map中对用key的value
func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	if exist {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId, op.RequestId, op.Key, value)
	} else {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId, op.RequestId, op.Key)
	}

	kv.DprintfKVDB()
	return value, exist
}

// 循环读取Raft已经应用的日志条目命令
func (kv *KVServer) ReadRaftApplyCommandLoop() {
	// 执行日志条目中的命令
	for message := range kv.applyCh {
		if message.CommandValid {
			kv.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapshotFromRaft(message)
		}
	}
}

// 从Raft获取命令，并发送命令到KVServer执行
func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)
	DPrintf("[RaftApplyCommand]Server %d , Got Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, message.CommandIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)

	// 该命令的Index比KVServer的最后的LogIndex还小，则不需要补充快照日志
	if message.CommandIndex <= kv.lastSSPointRaftLogIndex {
		return
	}

	// 不存在clientId对应的Request， 则直接在KVServer执行命令
	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == "put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	// 发送快照命令
	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	// 向Raft Server发送命令，并等待调用的返回
	kv.SendMessageToWaitChan(op, message.CommandIndex)
}

// 向Raft Server发送命令, Raft存在则保存Op到waitApplyCh数组的chan中
func (kv *KVServer) SendMessageToWaitChan(op Op, raftIndex int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 调用Raft Server
	ch, exist := kv.waitApplyCh[raftIndex]
	if exist {
		DPrintf("[RaftApplyMessageSendToWaitChan-->]Server %d , Send Command --> Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key, op.Value)
		ch <- op
	}
	return exist
}

// KVServer中执行Put命令
func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	kv.kvDB[op.Key] = op.Value
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}

// Append
func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId, op.Key, op.Value)
	kv.DprintfKVDB()
}
