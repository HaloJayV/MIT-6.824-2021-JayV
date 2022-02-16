package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

// 读取快照日志的命令
func (kv *KVServer) ReadSnapshotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvdb map[string]string
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_kvdb) != nil ||
		d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
	} else {
		// 解码正常
		kv.kvDB = persist_kvdb
		kv.lastRequestId = persist_lastRequestId
	}
}

func (kv *KVServer) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		// 发送快照命令
		snapshot := kv.MakeSnapshot()
		// 快照日志更新补充
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

// 创建快照数据
func (kv *KVServer) MakeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

// 从Raft中获取快照日志
func (kv *KVServer) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		// 追加快照日志
		kv.ReadSnapshotToInstall(message.Snapshot)
		kv.lastSSPointRaftLogIndex = message.SnapshotIndex
	}
}
