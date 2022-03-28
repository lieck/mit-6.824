package raft

//
// this is an outline of the API that raft must expose to
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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	//"labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

// Raft
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

	muApply   sync.Mutex
	condApply *sync.Cond

	applyCh chan ApplyMsg

	serverType   int   // 服务器类型
	electionTime int64 // 选举超时时间
	votesNum     int   // 投票数量
	serverNum    int   // 集群服务器数量

	heartbeatTime int64

	currentTerm int
	votedFor    int
	logs        []Entry

	lastLogIndex int
	commitIndex  int
	lastApplied  int

	nextIndex  []int
	matchIndex []int

	isPersist bool // 是否存在未持久化数据

	// 快照
	snapshotIndex int
	snapshotTerm  int
	snapshotData  []byte
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.serverType == Leader
	rf.mu.Unlock()

	return term, isleader
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) apply(index int, logs []Entry) {
	DPrintf("%v\t应用状态机\t[%v:%v]", rf.me, index, index+len(logs)-1)
	for i, val := range logs {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      val.Command,
			CommandIndex: i + index,
		}
	}
}

func (rf *Raft) applyEd() {
	for rf.killed() == false {
		rf.mu.Lock()

		start := rf.lastApplied + 1
		logs := make([]Entry, 0)

		if rf.lastApplied < rf.commitIndex && rf.logs[rf.commitIndex].Term == rf.currentTerm {
			for i := start; i <= rf.commitIndex; i++ {
				logs = append(logs, rf.logs[i])
				rf.lastApplied++
			}
		}

		rf.mu.Unlock()

		if len(logs) >= 1 {
			rf.apply(start, logs)
		}

		rf.muApply.Lock()
		rf.condApply.Wait()
		rf.muApply.Unlock()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.serverType == Leader

	if isLeader {
		rf.logs = append(rf.logs, Entry{
			Term:    term,
			Command: command,
		})
		rf.lastLogIndex = len(rf.logs) - 1
		rf.matchIndex[rf.me] = len(rf.logs) + rf.snapshotIndex - 1
		index = rf.lastLogIndex + rf.snapshotIndex
		rf.isPersist = true

		DPrintf("%v\tStart Entry\t%v", rf.me, index)
		DPrintf("%v\tlastLogIndex:%v", rf.me, rf.lastLogIndex)

		// 发送Log
		go rf.heartbeat()
	}

	return index, term, isLeader
}

// Kill
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
	rf.condApply.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		var sleepTime int64 = 100

		rf.mu.Lock()
		if rf.serverType == Leader {
			// 发送心跳请求
			go rf.heartbeat()
		} else {
			// 发起选举
			sleepTime = rf.electionTime - time.Now().UnixNano()/1e6
			if sleepTime <= 0 {
				rf.electionTime = newElectionTime()
				sleepTime = rf.electionTime - time.Now().UnixNano()/1e6
				go rf.election()
			}
		}
		rf.mu.Unlock()
		if sleepTime > 100 {
			sleepTime = 100
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)

	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.condApply = sync.NewCond(&rf.muApply)
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.serverType = 3
	rf.electionTime = newElectionTime()
	rf.serverNum = len(peers)

	rf.logs = make([]Entry, 1)

	rf.nextIndex = make([]int, rf.serverNum)
	rf.matchIndex = make([]int, rf.serverNum)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyEd()

	return rf
}

func newElectionTime() int64 {
	currTime := time.Now().UnixNano() / 1e6
	return currTime + rand.Int63n(200) + 250
}
