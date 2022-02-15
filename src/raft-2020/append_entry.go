package raft

// 追加日志时，Leader需要发送给Follower的参数
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// 发送追加日志命令后，Follower响应给Leader的参数
type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

// 向Follower发送追加日志
func (rf *Raft) appendEntries(heartBeat bool) {
	lastLog := rf.log.lastLog()
	for peer, _ := range rf.peers {
		if peer == rf.me {
			// 重启选举计时器
			rf.resetElectionTimer()
			continue
		}

		// Leaders role3
		//  如果Leader的lastLogIndex大于跟随者的最后一个日志索引 ≥ nextIndex
		// 则发送AppendEntries RPC包含从nextIndex开始的日志条目
		if lastLog.Index > rf.nextIndex[peer] || heartBeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			// Follower的日志需要与Leader同步
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			// 则发送AppendEntries RPC包含从nextIndex开始的日志条目
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			// 拷贝要追加的日志条目数组到参数
			copy(args.Entries, rf.log.slice(nextIndex))
			// Leader并行地向peers发送追加日志的命令
			go rf.leaderSendEntries(peer, &args)
		}
	}
}

// Leader向peers发送追加日志的命令
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	// 发送追加日志命令异常
	if !ok {
		return
	}
	// 发送追加日志命令成功
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 比当前Leader的term还大，term异常
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	// term正常
	if args.Term == rf.currentTerm {
		// rules for leader 3.1, 更新matchIndex和nextIndex
		// 复制日志成功
		if reply.Success {
			// matchIndex: 已知复制到该服务器的最高日志条目索引
			match := args.PrevLogIndex + len(args.Entries)
			// nextIndex
			next := match + 1
			// 更新Follower的nextIndex和matchIndex
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			// Follower.lastLogIndex < PrevLogIndex
			if reply.XTerm == -1 {
				// 日志缺失，nextIndex设置为Follower的日志条目数量
				rf.nextIndex[serverId] = reply.XLen
			} else {
				// Follower.log.at(args.PrevLogIndex).Term != Leader.PrevLogTerm
				// 即Follower的日志条目中某条日志的prevLogIndex对应的prevLogTerm不一样
				// reply.XTerm为Follower.log[PrevLogIndex].Term
				// Leader找到自己这个Term对应的最后一条日志条目索引
				lastIndexOfXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastIndexOfXTerm)
				if lastIndexOfXTerm > 0 {
					// 找得到，则直接复制为nextIndex
					rf.nextIndex[serverId] = lastIndexOfXTerm
				} else {
					// Leader日志中不存在这个term，则设置为Follower这个term的第一个日志条目索引
					rf.nextIndex[serverId] = reply.XIndex
				}
			}
			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			// 如果AppendEntries因为日志不一致而失败：递减NextIndex并重试
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}
}

// Leader复制日志到Follower超过一半后，就可以提交日志
func (rf *Raft) leaderCommitRule() {

	if rf.state != Leader {
		return
	}

	/**
	leader rule 4：
	如果存在一个N，使得N>commitIndex，大多数的matchIndex[i]≥N，
	并且log[N].term == currentTerm：设置commitIndex = N
	*/
	// 每次以一个日志条目为粒度，更新commitIndex
	for i := rf.commitIndex + 1; i <= rf.log.lastLog().Index; i++ {
		if rf.log.at(i).Term != rf.currentTerm {
			continue
		}
		counter := 1
		// 根据matchIndex判断是否复制成功
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			// 通过计算所有Follower中，matchIndex > Leader.commitIndex，则计数
			if serverId != rf.me && rf.matchIndex[serverId] >= i {
				counter++
			}
			// 超过一半，标识复制成功，更新commitIndex
			if counter > len(rf.peers)/2 {
				rf.commitIndex = i
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

// Leader找到Term对应的最后一条日志条目索引
func (rf *Raft) findLastLogInTerm(xTerm int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == xTerm {
			return i
		} else if term < xTerm {
			// 没有找到
			break
		}
	}
	return -1
}

func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// RPC
	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RPC方法
// Follower执行接受Leader追加日志的命令。并返回Follower自己的信息
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: (term %d) follower received [%v] AppendEntries %v, prevIndex %v, prevLogTerm %v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// all servers rule 2:
	// 如果RPC请求或响应包含任期 T > currentTerm：设置currentTerm = T，转换为follower
	reply.Success = false
	reply.Term = rf.currentTerm
	// 更新最新的term并返回
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// AppendEntries rpc rule 1:
	// 如果 term < currentTerm，则返回 false
	// Leader的term比Follower的term还小，则直接返回
	if args.Term < rf.currentTerm {
		return
	}
	// setNewTerm成功
	rf.resetElectionTimer()

	// candidate rule 3
	if rf.state == Candidate {
		rf.state = Follower
	}

	// AppendEntries rpc rule 2:
	// 如果日志在prevLogIndex处不包含term与prevLogTerm匹配的条目，则返回false
	// Follower的lastLogIndex小于Leader的prevLogIndex, 不符合追加日志的情况, 即缺失日志
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// prevLogIndex对应的prevLogTerm不一样，有冲突，返回false
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		// prevLogIndex对应的term，preLogIndex之前的日志条目与Leader的相同
		xTerm := rf.log.at(args.PrevLogIndex).Term
		// 借助xTerm，从preLogIndex开始，从右到左寻找第一个term不等于xTerm的日志条目索引xIndex+1
		// 此时reply的XIndex为：任期为xTerm的日志条目中第一个logIndex
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		// 此时reply的XIerm为：prevLogIndex对应的term
		reply.XTerm = xTerm
		// 状态机内的日志条目数量
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// Follower日志中存在idx和term分别为prevLogIndexh和prevLogTerm的日志条目
	for idx, entry := range args.Entries {
		// append entries rpc 3
		// 删除和新日志条目发生冲突（索引相同，任期不同）的日志条目
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			// 相当于删除entry.Index开始的后续日志条目
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		// 开始追加新日志
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	// Follower追加日志后，更新commitIndex，并且开始应用日志到状态机
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}
