package raft

import "time"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && args.Term == rf.currentTerm) {
		return
	}
	rf.currentTerm = args.Term
	rf.serverType = 3

	// 快照比较
	if rf.snapshotTerm > args.LastLogTerm {
		return
	} else if rf.snapshotTerm == args.LastLogTerm && rf.snapshotIndex > args.LastLogIndex {
		return
	}

	// 日志比较
	if rf.lastLogIndex > 0 {
		if rf.logs[rf.lastLogIndex].Term > args.LastLogTerm {
			return
		}
		if rf.logs[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex+rf.snapshotIndex > args.LastLogIndex {
			return
		}
	}

	// 投票
	reply.VoteGranted = true
	rf.serverType = 3
	rf.votedFor = args.CandidateId
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// 判断是否开始选举
		rf.mu.Lock()
		if rf.serverType != 3 {
			rf.electionTime = newElectionTime()
		}

		nextTime := rf.electionTime - time.Now().UnixNano()/1e6
		rf.mu.Unlock()

		if nextTime > 0 {
			time.Sleep(time.Duration(nextTime) * time.Millisecond)
			continue
		}

		// 开始选举
		rf.mu.Lock()
		rf.currentTerm += 1
		rf.serverType = Candidate
		rf.votesNum = 1
		rf.votedFor = rf.me

		rf.requestNum = rf.serverNum
		rf.electionTime = newElectionTime()

		args := RequestVoteArgs{}
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		args.LastLogIndex = rf.lastLogIndex + rf.snapshotIndex
		if rf.lastLogIndex > 0 {
			args.LastLogTerm = rf.logs[rf.lastLogIndex].Term
		} else {
			args.LastLogTerm = rf.snapshotTerm
		}
		rf.mu.Unlock()

		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go rf.requestTicker(idx, args)
		}

		rf.muTicker.Lock()
		rf.condTicker.Wait()
		rf.muTicker.Unlock()

		rf.mu.Lock()
		rf.electionTime = newElectionTime()

		if rf.serverType == 2 && rf.votesNum >= rf.serverNum/2 {
			// 选举成功
			rf.serverType = 1
			rf.votedFor = rf.me
			for i := 0; i < rf.serverNum; i++ {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = rf.lastLogIndex + rf.snapshotIndex + 1
			}
			rf.persist()
			rf.condHeartbeat.Broadcast()
		} else {
			rf.serverType = 3
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) requestTicker(serverId int, args RequestVoteArgs) {
	is := false
	go func() {
		time.Sleep(200 * time.Millisecond)
		is = true

		rf.mu.Lock()
		rf.requestNum -= 1
		if rf.requestNum <= 0 || rf.votesNum > rf.serverNum/2 {
			rf.condTicker.Broadcast()
		}
		rf.mu.Unlock()
	}()

	reply := RequestVoteReply{}
	ok := rf.peers[serverId].Call("Raft.RequestVote", &args, &reply)

	if is {
		return
	}

	rf.mu.Lock()
	if ok {
		if reply.VoteGranted && args.Term == rf.currentTerm {
			rf.votesNum += 1
		} else if reply.Term > rf.currentTerm {
			rf.serverType = 3
			rf.currentTerm = reply.Term
		}
	}

	rf.requestNum -= 1
	if rf.requestNum <= 0 || rf.votesNum > rf.serverNum/2 {
		rf.condTicker.Broadcast()
	}
	rf.mu.Unlock()
}
