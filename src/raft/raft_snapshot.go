package raft

type InstallSnapshotArgs struct {
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 供KVServer调用, 实现快照压缩日志
// 当前Raft服务调用Snapshot()将其服务自身快照的状态传达给 Raft
// index是指从哪个index的日志条目开始补充快照日志
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 当前Raft服务的last snapshot log index 比 index还高
	// 或者要追加的快照日志索引大于当前raft服务的提交日志最高索引，不符合Raft规定
	// 补充快照日志的index需要满足：lastSSPointIndex < index <= commitIndex
	if rf.lastSSPointIndex >= index || index > rf.commitIndex {
		return
	}

	// 开始补充快照日志
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})
	// 从 index+1开始从Leader的快照中更新日志到当前Raft，直到lastLogIndex
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		// 根据logIndex获取Raft的日志条目
		tempLog = append(tempLog, rf.getLogWithIndex(i))
	}

	// 补充快照日志后，更新最新快照日志最高term
	if index == rf.getLastIndex()+1 {
		rf.lastSSPointTerm = rf.getLastTerm()
	} else {
		rf.lastSSPointTerm = rf.getLogTermWithIndex(index)
	}

	rf.lastSSPointIndex = index

	// 补充快照日志
	rf.log.Entries = tempLog
	// 更新commitIndex和lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d", rf.me, index, rf.lastSSPointTerm, len(rf.log.Entries)-1)
	// 保存快照日志信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
