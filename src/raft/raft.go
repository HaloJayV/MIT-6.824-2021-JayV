package raft

//
// this is an outline of the API that raft-2021 must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"
import "6.824/labgob"

// import "bytes"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
// 为每个新提交的日志条目 发送一个ApplyMsg到Make()的applyCh通道参数。
type ApplyMsg struct {
	CommandValid bool        // 命令是否生效
	Command      interface{} // 要append的命令
	CommandIndex int         // 该命令的索引

	// 2D 日志压缩
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 标识是Follower、Candidate、Leader
type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         RaftState     // 节点的角色
	appendentryCh chan *Entry   // 需要复制的日志条目
	heartBeat     time.Duration // 当前Leader的心跳时间
	electionTime  time.Time     // 当前Candidate的选举时间

	// Persistent on all server，所有服务器都需要持久化的信息
	currentTerm int // 当前任期
	votedFor    int // 当前正在投票给的那个节点id
	log         Log // 包好当前服务器所有日志条目

	// Volatile on all servers, 即所有服务器不需要持久化的信息
	commitIndex int // 已提交的最后一条日志条目的索引
	lastApplied int // 已应用到服务器本地的最后一条日志条目的索引

	// Volatile on Leader, 在leader中不需要持久化的状态信息
	nextIndex  []int // 发送到对应服务器的下一条日志条目的索引
	matchIndex []int // 已经复制到对应服务器的最高日志条目的索引

	// 为每个新提交的日志条目 发送一个ApplyMsg到Make()的applyCh通道参数。
	applyCh chan ApplyMsg
	// 条件变量，用于等待一个或一组goroutines满足条件后唤醒的场景,实现线程同步
	applyCond *sync.Cond

	// Lab2D Snapshot
	// last snapshot point index
	lastSSPointIndex int
	lastSSPointTerm  int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 实现持久化，编码成byte数组并保存到磁盘
func (rf *Raft) persist() {
	DPrintVerbose("[%v]: STATE: %v", rf.me, rf.log.String())
	// Your code here (2C).
	w := new(bytes.Buffer)
	// e中编码后的数据保存到w这个Buffer缓冲区中
	e := labgob.NewEncoder(w)
	// 持久化任期号、投票对象、当前所有日志条目
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// 保存为字节数组
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSSPointIndex)
	e.Encode(rf.lastSSPointTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
// 读取磁盘中的持久化数据
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// 解码
	var currentTerm int
	var votedFor int
	var logs Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("failed to Persist\n")
	} else {
		// 三个都解码成功，都不为nil才算成功
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 每个服务器开始处理并将命令附加到复制的日志中
// 分别返回：要复制的日志条目中最后一条日志条目的索引；当前Raft的任期；是否成功
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Leader才能执行Start函数
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.log.lastLog().Index + 1
	term := rf.currentTerm
	log := Entry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log.append(log)
	rf.persist()
	// debug
	DPrintf("[%v]: term %v Start %v", rf.me, term, log)
	// 向Follower发送追加日志
	rf.appendEntries(false)
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 应用日志到本地
func (rf *Raft) apply() {
	// 放行线程
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

// 心跳&追加日志、选举事件定时器
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		// 发送心跳
		if rf.state == Leader {
			rf.appendEntries(true)
		}
		// 选举超时, 则进行新的一轮选举
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

// 应用日志到状态机
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// raft实例还在
	for !rf.killed() {
		// 开始应用日志
		if rf.commitIndex > rf.lastApplied &&
			rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			// 保存每一条日志条目的命令、索引
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			// 等到执行apply方法才放行线程
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}

// 获取状态机内的命令，返回字符串拼接后的形式
func (rf *Raft) commits() string {
	commands := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		commands = append(commands, fmt.Sprintf("%4d", rf.log.at(i).Command))
	}
	return fmt.Sprintf(strings.Join(commands, "|"))
}

// 根据logIndex获取Raft的日志条目
func (rf *Raft) getLogWithIndex(globalIndex int) Entry {
	return rf.log.Entries[globalIndex-rf.lastSSPointIndex]
}

func (rf *Raft) getLastTerm() int {
	// 刚经过日志压缩
	if len(rf.log.Entries) == 1 {
		return rf.lastSSPointTerm
	} else {
		return rf.log.Entries[len(rf.log.Entries)-1].Term
	}
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	if globalIndex == rf.lastSSPointIndex {
		return rf.lastSSPointTerm
	}
	return rf.log.Entries[globalIndex-rf.lastSSPointIndex].Term
}

// 快照的状态信息字节大小
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 创建Raft实例
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = 50 * time.Millisecond
	rf.resetElectionTimer()

	// 初始化日志
	rf.log = makeEmptyLog()
	rf.log.append(Entry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	// 锁放到条件变量condition里
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 心跳&追加日志、选举事件定时器
	go rf.ticker()
	// 已提交的日志逐渐应用到状态机
	go rf.applier()
	return rf
}
