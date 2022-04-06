package raft

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (rf.votedFor != -1 && args.Term == rf.currentTerm) {
		return
	}
	rf.currentTerm = args.Term
	reply.Term = args.Term
	rf.serverType = Follower

	// 比较index / term
	lastTerm := rf.snapshotTerm
	lastIndex := rf.snapshotIndex
	if rf.lastLogIndex >= 1 {
		lastTerm = rf.logs[rf.lastLogIndex].Term
		lastIndex += rf.lastLogIndex
	}

	if lastTerm > args.LastLogTerm {
		return
	} else if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
		return
	}

	DPrintf("%v\t投票给%v\tTerm:%v\taTerm:%v,aIdx:%v\tTerm:%v,idx:%v", rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.logs[rf.lastLogIndex].Term, rf.lastLogIndex)

	// 投票
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.electionTime = newElectionTime()
	rf.persist()
}

func (rf *Raft) election() {
	DPrintf("%v\t开始选举", rf.me)

	// 开始选举
	rf.mu.Lock()
	rf.currentTerm++
	rf.serverType = Candidate
	rf.votesNum = 1
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTime = newElectionTime()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex + rf.snapshotIndex,
	}
	if rf.lastLogIndex > 0 {
		args.LastLogTerm = rf.logs[rf.lastLogIndex].Term
	} else {
		args.LastLogTerm = rf.snapshotTerm
	}
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx != rf.me {
			go rf.sendElection(idx, args)
		}
	}
}

func (rf *Raft) sendElection(serverId int, args RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[serverId].Call("Raft.RequestVote", &args, &reply)

	if !ok || !reply.VoteGranted {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > args.Term {
		rf.serverType = Follower
		rf.currentTerm = reply.Term
		return
	}

	// 过期的请求
	if args.Term < rf.currentTerm || rf.serverType != Candidate {
		return
	}

	rf.votesNum += 1
	DPrintf("%v\t收到%v的选举信息", rf.me, serverId)
	if rf.votesNum > rf.serverNum/2 {
		// 选举成功
		DPrintf("%v\t获取Leader\tlastIndex:%v", rf.me, rf.lastLogIndex+rf.snapshotIndex+1)
		rf.serverType = Leader
		for i := 0; i < rf.serverNum; i++ {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = rf.lastLogIndex + rf.snapshotIndex + 1
		}
		// 发送心跳请求
		go rf.heartbeat()
	}
}
