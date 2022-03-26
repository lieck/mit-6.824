package raft

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 丢弃日志
	size := index - rf.snapshotIndex

	rf.snapshotTerm = rf.logs[index-rf.snapshotIndex].Term
	rf.snapshotIndex = index
	rf.snapshotData = snapshot

	rf.commitIndex -= size
	rf.lastApplied -= size
	rf.logs = append(rf.logs[:1], rf.logs[size+1:]...)
	rf.lastLogIndex = len(rf.logs) - 1

	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.currentTerm = args.Term
	rf.serverType = 3
	rf.votedFor = args.LeaderId

	rf.snapshotData = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	rf.logs = make([]Entry, 1)
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.Term,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
}

func (rf *Raft) rpcInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Offset:            0,
		Data:              rf.snapshotData,
		Done:              false,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.serverType = 3
			rf.currentTerm = reply.Term
		} else {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
			//dPrint(3, rf.me, "\t快照确定成功:", server, "\t", rf.matchIndex[server])
		}
		rf.mu.Unlock()
	}
}
