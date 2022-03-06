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
	"bytes"
	"sort"

	//"labgob"
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

	muTicker      sync.Mutex
	muHeartbeat   sync.Mutex
	condTicker    *sync.Cond
	condHeartbeat *sync.Cond

	applyCh chan ApplyMsg

	serverType   int   // 1为leader，2为candidate，3为follower
	electionTime int64 // 选举超时时间
	votesNum     int   // 投票数量
	rpcNum       int   // rpc响应的server数量
	serverNum    int   // 集群服务器数量

	lastLogIndex int

	currentTerm int
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	appliedStep int  // 日志收敛
	isPersist   bool // 是否存在日志未持久化日志
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.serverType == 1
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	for i := 1; i <= rf.lastLogIndex; i++ {
		e.Encode(rf.logs[i])
	}
	dPrint(3, rf.me, "\t持久化", rf.lastLogIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.isPersist = false
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)

	var val = Entry{}
	for d.Decode(&val) == nil {
		rf.logs = append(rf.logs, val)
	}
	rf.lastLogIndex = len(rf.logs) - 1

	dPrint(3, rf.me, "\t读取磁盘信息：", rf.currentTerm, "\t", rf.votedFor, "\t", rf.lastLogIndex)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool // 0拒绝，1同意
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && args.Term == rf.currentTerm) {
		return
	}
	rf.currentTerm = args.Term

	if rf.lastLogIndex > 0 {
		if rf.logs[rf.lastLogIndex].Term > args.LastLogTerm {
			return
		}
		if rf.logs[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
			return
		}
	}

	//dPrint(2, rf.me, " 投票给", args.CandidateId)

	reply.VoteGranted = true
	rf.serverType = 3
	rf.votedFor = args.CandidateId
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dPrint(3, rf.me, " 收到 ", args.LeaderId, " 的心跳请求\t", rf.currentTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	rf.electionTime = newElectionTime()
	rf.serverType = 3

	// 接收日志
	if rf.lastLogIndex > args.PrevLogIndex {
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
		rf.lastLogIndex = len(rf.logs) - 1
	}

	reply.Success = false
	if rf.lastLogIndex < args.PrevLogIndex ||
		rf.logs[rf.lastLogIndex].Term != args.PrevLogTerm {

		reply.XLen = rf.lastLogIndex
		if rf.lastLogIndex > 0 {
			reply.XTerm = rf.logs[rf.lastLogIndex].Term
			reply.XIndex = rf.lastLogIndex
			for reply.XIndex >= 2 && rf.logs[reply.XIndex-1].Term == reply.XTerm {
				reply.XIndex -= 1
			}
		} else {
			reply.XTerm = -1
			reply.XIndex = -1
		}
		return
	}

	reply.Success = true
	if args.Entries != nil {
		for entIdx := 0; entIdx < len(args.Entries); entIdx++ {
			rf.logs = append(rf.logs, args.Entries[entIdx])
		}
		dPrint(3, rf.me, "\t添加日志:[", args.PrevLogIndex+1, ",", len(rf.logs)-1, "]")
		rf.lastLogIndex = len(rf.logs) - 1
		rf.persist()
	}
	rf.lastLogIndex = len(rf.logs) - 1

	if rf.lastLogIndex < args.LeaderCommit {
		rf.commitIndex = rf.lastLogIndex
	} else {
		rf.commitIndex = args.LeaderCommit
	}

	// 应用状态机
	if rf.logs[rf.commitIndex].Term == rf.currentTerm {
		for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
			dPrint(3, rf.me, "\t应用日志", idx, "\t", rf.logs[idx].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[idx].Command,
				CommandIndex: idx,
			}
			rf.lastApplied += 1
		}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.serverType == 1
	term = rf.currentTerm
	if isLeader {
		rf.logs = append(rf.logs, Entry{
			Term:    term,
			Command: command,
		})
		rf.lastLogIndex = len(rf.logs) - 1
		index = rf.lastLogIndex
		rf.isPersist = true
		dPrint(3, rf.me, " client entry:", rf.lastLogIndex, "\t", command, "\t", term)
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	//dPrint(3, rf.me, "\ttype:", rf.serverType, "\tterm:", rf.currentTerm)
	//dPrint(3, rf.me, "\tlastLogIndex:", rf.lastLogIndex)
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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
		rf.votesNum = 0
		rf.serverType = 2
		rf.rpcNum = 0
		rf.votedFor = rf.me
		rf.mu.Unlock()

		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go rf.rpcTicker(idx)
		}

		go func() {
			time.Sleep(time.Millisecond * 30)
			rf.condTicker.Broadcast()
		}()

		rf.muTicker.Lock()
		rf.condTicker.Wait()
		rf.muTicker.Unlock()

		rf.mu.Lock()
		rf.electionTime = newElectionTime()

		dPrint(3, rf.me, " 选举", rf.votesNum)
		if rf.serverType == 2 {
			//dPrint(3, rf.me, " 选举完成\t", rf.votesNum >= rf.serverNum/2)
			if rf.votesNum >= rf.serverNum/2 {
				// 选举成功
				dPrint(3, rf.me, " 获取leader")
				rf.serverType = 1
				rf.votedFor = rf.me
				for i := 0; i < rf.serverNum; i++ {
					rf.matchIndex[i] = 0
					rf.nextIndex[i] = rf.lastLogIndex + 1
				}

				rf.condHeartbeat.Broadcast()
			} else {
				rf.serverType = 3
				rf.votedFor = -1
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) rpcTicker(idx int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastLogIndex
	if rf.lastLogIndex != 0 {
		args.LastLogTerm = rf.logs[rf.lastLogIndex].Term
	} else {
		args.LastLogTerm = 0
	}
	rf.mu.Unlock()

	ok := rf.peers[idx].Call("Raft.RequestVote", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			rf.votesNum += 1
		} else if reply.Term > rf.currentTerm {
			rf.serverType = 3
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	rf.electionTime = newElectionTime()
	rf.rpcNum -= 1
	if rf.rpcNum <= 0 {
		// rf.votesNum >= rf.serverNum/2
		rf.condTicker.Broadcast()
	}
	rf.mu.Unlock()
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

		//dPrint(3, rf.me, " 心跳")
		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go rf.rpcHeartbeat(idx)
		}

		time.Sleep(time.Millisecond * 105)

		rf.mu.Lock()
		if rf.serverType == 1 {
			arr := rf.matchIndex
			sort.Ints(arr)
			idx := arr[len(arr)/2+1]

			if idx > rf.commitIndex {
				rf.commitIndex = idx

				if rf.commitIndex > rf.lastApplied && rf.logs[rf.commitIndex].Term == rf.currentTerm {
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						dPrint(3, rf.me, "\t应用日志", i, "\t", rf.logs[i].Command)
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i].Command,
							CommandIndex: i,
						}
						rf.lastApplied += 1
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
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.Entries = nil
	args.LeaderCommit = rf.commitIndex

	start := rf.nextIndex[server]
	end := rf.lastLogIndex

	if rf.lastLogIndex > 0 {
		if start <= rf.lastLogIndex {
			//dPrint(3, rf.me, " 发送log至", server, " [", start, ",", end, "]")
			args.Entries = rf.logs[start : end+1]
		}
		args.PrevLogIndex = start - 1
		args.PrevLogTerm = rf.logs[start-1].Term
	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		rf.electionTime = newElectionTime()
		if reply.Term > rf.currentTerm {
			rf.serverType = 3
			rf.currentTerm = reply.Term
		} else if reply.Success {
			rf.nextIndex[server] = end + 1
			rf.matchIndex[server] = end
		} else {
			idx := rf.lastLogIndex
			for idx > 1 && rf.logs[idx].Term > reply.XTerm {
				idx -= 1
			}
			if rf.logs[idx].Term == reply.XTerm {
				rf.nextIndex[server] = idx
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
			if rf.nextIndex[server] >= reply.XLen {
				rf.nextIndex[server] = reply.XLen + 1
			}
			if rf.nextIndex[server] <= 0 {
				rf.nextIndex[server] = 1
			}

			rf.matchIndex[server] = 0
		}
		rf.mu.Unlock()
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

	rf.condTicker = sync.NewCond(&rf.muTicker)
	rf.condHeartbeat = sync.NewCond(&rf.muHeartbeat)
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

	dPrint(1, rf.me, " server num:", rf.serverNum)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	//print("\n")

	return rf
}

func newElectionTime() int64 {
	currTime := time.Now().UnixNano() / 1e6
	return currTime + rand.Int63n(400) + 300
}

func dPrint(t int, a ...interface{}) {
	debugType := 31
	if t == debugType {
		//currTime := time.Now()
		//print(currTime.Hour(), ":", currTime.Minute(), ":", currTime.Second(), "-")
		//print(currTime.Nanosecond() / 1e9, ' ')
		//log.Print(time.Now().UnixNano() / 1e6)
		log.Print(a...)
	}
	return
}
