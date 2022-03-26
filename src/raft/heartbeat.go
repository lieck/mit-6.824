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
	rf.serverType = 3
	rf.currentTerm = args.Term

	// 接收日志
	if rf.lastLogIndex+rf.snapshotIndex > args.PrevLogIndex {
		rf.logs = rf.logs[0 : args.PrevLogIndex-rf.snapshotIndex+1]
		rf.lastLogIndex = len(rf.logs) - 1
	}

	// 日志错误处理
	if rf.lastLogIndex+rf.snapshotIndex < args.PrevLogIndex ||
		rf.lastLogIndex > 0 && rf.logs[rf.lastLogIndex].Term != args.PrevLogTerm {

		reply.XLen = rf.lastLogIndex + rf.snapshotIndex
		if rf.lastLogIndex > 0 {
			reply.XTerm = rf.logs[rf.lastLogIndex].Term
			reply.XIndex = rf.lastLogIndex
			for reply.XIndex >= 2 && rf.logs[reply.XIndex-1].Term == reply.XTerm {
				reply.XIndex -= 1
			}
			reply.XIndex += rf.snapshotIndex
		} else if rf.snapshotIndex > 0 {
			reply.XTerm = rf.snapshotTerm
			reply.XIndex = rf.snapshotIndex
		} else {
			reply.XTerm = -1
			reply.XIndex = -1
		}
		return
	}

	// 添加到logs
	reply.Success = true
	if args.Entries != nil {
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

	// 应用状态机
	if rf.logs[rf.commitIndex].Term == rf.currentTerm && rf.lastApplied < rf.commitIndex {
		tLogs := make([]Entry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			tLogs = append(tLogs, rf.logs[i])
		}
		index := rf.lastApplied + rf.snapshotIndex + 1
		go rf.apply(index, tLogs)

		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		for {
			rf.mu.Lock()
			status := rf.serverType
			rf.mu.Unlock()

			if status != 1 {
				rf.muHeartbeat.Lock()
				rf.condHeartbeat.Wait()
				rf.muHeartbeat.Unlock()
			} else {
				break
			}
		}

		rf.mu.Lock()
		if rf.isPersist {
			rf.persist()
		}
		rf.mu.Unlock()
		//dPrint(3, rf.me, "\t发送心跳请求")

		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go rf.rpcHeartbeat(idx)
		}

		time.Sleep(time.Millisecond * 105)

		rf.mu.Lock()
		if rf.serverType == 1 {
			var arr []int
			for _, val := range rf.matchIndex {
				arr = append(arr, val)
			}
			sort.Ints(arr)
			idx := arr[len(arr)/2] - rf.snapshotIndex

			if idx > rf.commitIndex {
				rf.commitIndex = idx
				if rf.lastApplied < rf.commitIndex && rf.logs[rf.commitIndex].Term == rf.currentTerm {

					tLogs := make([]Entry, 0)
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						tLogs = append(tLogs, rf.logs[i])
					}
					index := rf.lastApplied + rf.snapshotIndex + 1
					go rf.apply(index, tLogs)

					rf.lastApplied = rf.commitIndex

					if rf.isPersist {
						rf.persist()
					}
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) rpcHeartbeat(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	if rf.serverType != 1 {
		rf.mu.Unlock()
		return
	}

	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.Entries = nil
	args.LeaderCommit = rf.commitIndex + rf.snapshotIndex

	start := rf.nextIndex[server] - rf.snapshotIndex
	end := rf.lastLogIndex

	endLogIndex := rf.lastLogIndex + rf.snapshotIndex

	args.PrevLogIndex = rf.snapshotIndex
	args.PrevLogTerm = rf.snapshotTerm
	isSnapshot := false

	if start <= 0 {
		isSnapshot = true
		//dPrint(3, rf.me, "\t日志缺失:", server, "\t", rf.nextIndex[server], "\t", rf.snapshotIndex)
		// 日志缺失，发送快照
		rf.mu.Unlock()
		go rf.rpcInstallSnapshot(server)
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
		rf.mu.Unlock()
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.serverType = 3
			rf.currentTerm = reply.Term
		} else if args.Term == rf.currentTerm {
			if reply.Success {
				rf.nextIndex[server] = endLogIndex + 1
				rf.matchIndex[server] = endLogIndex
				//dPrint(3, rf.me, "\t日志确定成功：", server, ",", rf.nextIndex[server])
			} else if !isSnapshot {
				if rf.lastLogIndex > 0 {
					idx := rf.lastLogIndex
					for idx > 1 && rf.logs[idx].Term > reply.XTerm {
						idx -= 1
					}
					if rf.logs[idx].Term == reply.XTerm {
						rf.nextIndex[server] = idx + rf.snapshotIndex
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
					if rf.nextIndex[server] >= reply.XLen {
						rf.nextIndex[server] = reply.XLen + 1
					}
				} else {
					rf.nextIndex[server] = 1
				}

				if rf.nextIndex[server] <= 0 {
					rf.nextIndex[server] = 1
				}
				rf.matchIndex[server] = 0
				//dPrint(3, rf.me, "\t日志返回:", server, "\t", rf.nextIndex[server], "\t", reply.XIndex)
			}
		}
		rf.mu.Unlock()
	}
}
