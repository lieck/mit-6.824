package raft

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapshotIndex >= index {
		return
	}

	// 丢弃日志
	size := index - rf.snapshotIndex

	rf.snapshotTerm = rf.logs[index-rf.snapshotIndex].Term
	rf.snapshotIndex = index
	rf.snapshotData = snapshot

	rf.commitIndex -= size
	rf.lastApplied -= size
	rf.logs = append(rf.logs[:1], rf.logs[size+1:]...)
	rf.lastLogIndex = len(rf.logs) - 1

	DPrintf("%v\t生成快照\tlast:%v", rf.me, index)

	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	DPrintf("%v\t安装快照", rf.me)

	rf.currentTerm = args.Term
	rf.serverType = Follower
	// rf.electionTime = newElectionTime()

	// 是否需要快照
	if rf.snapshotIndex+rf.commitIndex >= args.LastIncludedIndex {
		return
	}

	rf.logs = make([]Entry, 1)
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.snapshotData = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshotIs = true
	rf.persist()

	rf.condApply.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}

	DPrintf("%v\t发送快照", rf.me)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.serverType = Follower
			rf.currentTerm = reply.Term
		} else {
			rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		}
	}
}
