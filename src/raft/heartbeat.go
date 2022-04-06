package raft

import (
	"sort"
	"time"
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.electionTime = newElectionTime()
	rf.serverType = Follower
	rf.currentTerm = args.Term

	DPrintf("%v\t收到心跳", rf.me)

	// 延迟收到的日志处理
	if rf.lastLogIndex+rf.snapshotIndex > args.PrevLogIndex+len(args.Entries) {
		if len(args.Entries) > 0 {
			argsLastIndex := args.PrevLogIndex + len(args.Entries)
			argsLastTerm := args.Entries[len(args.Entries)-1].Term
			if rf.validationLogL(argsLastIndex, argsLastTerm) {
				reply.Success = true
				return
			}
		} else if rf.validationLogL(args.PrevLogIndex, args.PrevLogTerm) {
			reply.Success = true
			return
		}
	}

	// 去除与快照重复的日志
	// 存储到快照的log一定是正确的
	if args.PrevLogIndex < rf.snapshotIndex {
		argsLen := rf.snapshotIndex - args.PrevLogIndex
		args.Entries = args.Entries[argsLen:]
		args.PrevLogIndex = rf.snapshotIndex
	}
	// 去除超过长度的日志
	if rf.lastLogIndex+rf.snapshotIndex > args.PrevLogIndex {
		rf.logs = rf.logs[0 : args.PrevLogIndex-rf.snapshotIndex+1]
		rf.lastLogIndex = len(rf.logs) - 1
	}

	// 不冲突，但缺少部分日志
	if rf.lastLogIndex+rf.snapshotIndex < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastLogIndex + rf.snapshotIndex
		return
	}

	// 日志冲突
	// if !rf.validationLogL(args.PrevLogIndex, args.PrevLogTerm) {
	if rf.lastLogIndex > 0 && rf.logs[rf.lastLogIndex].Term != args.PrevLogTerm {
		reply.XLen = rf.lastLogIndex + rf.snapshotIndex
		reply.XTerm = rf.logs[rf.lastLogIndex].Term

		// 计算对应任期的第一条槽位号
		reply.XIndex = rf.lastLogIndex
		for reply.XIndex >= 2 && rf.logs[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex -= 1
		}
		reply.XIndex += rf.snapshotIndex

		return
	}

	reply.Success = true

	// 添加到logs
	if args.Entries != nil {
		DPrintf("%v\t收到Log\t[%v:%v]", rf.me, rf.lastLogIndex+1, rf.lastLogIndex+len(args.Entries))
		for entIdx := 0; entIdx < len(args.Entries); entIdx++ {
			rf.logs = append(rf.logs, args.Entries[entIdx])
		}
		rf.lastLogIndex = len(rf.logs) - 1
		rf.persist()
	}

	rf.lastLogIndex = len(rf.logs) - 1
	if rf.lastLogIndex+rf.snapshotIndex < args.LeaderCommit {
		rf.commitIndex = rf.lastLogIndex
	} else {
		rf.commitIndex = args.LeaderCommit - rf.snapshotIndex
	}

	DPrintf("%v\t收到心跳\tlastIndex:%v\tcommIdx:%v", rf.me, rf.lastLogIndex+rf.snapshotIndex, rf.commitIndex+rf.snapshotIndex)

	// 应用状态机
	if rf.commitIndex > 0 && rf.logs[rf.commitIndex].Term == rf.currentTerm && rf.lastApplied < rf.commitIndex {
		rf.condApply.Broadcast()
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverType != Leader {
		return
	}

	rf.electionTime = newElectionTime()
	rf.heartbeatTime = time.Now().UnixNano() / 1e6

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.snapshotIndex,
			PrevLogTerm:  rf.snapshotTerm,
			Entries:      nil,
			LeaderCommit: rf.commitIndex + rf.snapshotIndex,
		}

		start := rf.nextIndex[server] - rf.snapshotIndex
		end := rf.lastLogIndex

		endLogIndex := rf.lastLogIndex + rf.snapshotIndex
		isSnapshot := false

		DPrintf("%v\t发送至%v\t[%v,%v]", rf.me, server, start, end)

		if start <= 0 {
			isSnapshot = true
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm:  rf.snapshotTerm,
				Offset:            0,
				Data:              rf.snapshotData,
				Done:              false,
			}
			go rf.sendInstallSnapshot(server, &args)
		} else {
			if rf.lastLogIndex > 0 {
				if start <= rf.lastLogIndex {
					for i := start; i <= end; i++ {
						args.Entries = append(args.Entries, rf.logs[i])
					}
				}
				if start > 1 {
					args.PrevLogIndex = start + rf.snapshotIndex - 1
					args.PrevLogTerm = rf.logs[start-1].Term
				}
			}
		}

		go rf.sendHeartbeat(server, &args, endLogIndex, isSnapshot)
	}
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, endLogIndex int, isSnapshot bool) {
	reply := AppendEntriesReply{}

	start := time.Now().UnixNano() / 1e6
	//DPrintf("%v\t发送心跳至%v", rf.me, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok || time.Now().UnixNano()/1e6-start >= 100 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm || rf.serverType != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.serverType = Follower
		rf.currentTerm = reply.Term
		return
	}

	if reply.Success {
		//DPrintf("%v\t确认%v Log\t[1:%v]", rf.me, server, endLogIndex)
		rf.nextIndex[server] = max(endLogIndex+1, rf.nextIndex[server])
		rf.matchIndex[server] = max(endLogIndex, rf.matchIndex[server])
	} else if !isSnapshot {
		if reply.XTerm == -1 {
			// 不冲突，但是缺少部分日志
			rf.nextIndex[server] = reply.XLen + 1
		} else {
			// 冲突：以Term为单位回退
			// 搜索Logs内是否包含对应 Term
			idx := rf.lastLogIndex
			for idx > 0 && rf.logs[idx].Term > reply.XTerm {
				idx--
			}

			if idx == 0 {
				// 需要重新开始发送
				rf.nextIndex[server] = 1
			} else if rf.logs[idx].Term == reply.XTerm {
				// Logs包含日志对应的Term
				rf.nextIndex[server] = idx + rf.snapshotIndex
			} else {
				// Logs不包含日志对应的Term
				rf.nextIndex[server] = min(reply.XIndex, rf.snapshotIndex+rf.lastLogIndex+1)
			}
		}
		//DPrintf("%v\t失败%v Log\t%v\t参数\tXTerm:%v,XIndex:%v,XLen:%v", rf.me, server, rf.nextIndex[server], reply.XTerm, reply.XIndex, reply.XLen)
	}
	rf.commitEntryL()
}

func (rf *Raft) commitEntryL() {
	var arr []int
	for _, val := range rf.matchIndex {
		arr = append(arr, val)
	}
	sort.Ints(arr)
	idx := arr[len(arr)/2] - rf.snapshotIndex

	if idx > rf.commitIndex && rf.logs[idx].Term == rf.currentTerm {
		rf.commitIndex = idx
		rf.condApply.Broadcast()
		DPrintf("%v\tLeader commitIndex:%v", rf.me, rf.commitIndex)
	}
}

// 判断log[idx]是否与tarLog一致
func (rf *Raft) validationLogL(idx int, tarTerm int) bool {
	if idx == 0 {
		return true
	}

	if rf.snapshotIndex == idx {
		return rf.snapshotTerm == tarTerm
	}

	if rf.snapshotIndex > idx {
		return true
	}

	if rf.snapshotIndex+rf.lastLogIndex < idx {
		return false
	}

	return rf.logs[idx-rf.snapshotIndex].Term == tarTerm
}
