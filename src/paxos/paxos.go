package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq 可以忘记所有<= seq的实例
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten 在此序列之前的实例已被遗忘
//

import (
	"log"
	"mit6.824/src/config"
	"net"
	"time"
)
import "net/rpc"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// Fate px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

func DPrintf(format string, a ...interface{}) {
	if config.PaxosDebugLog > 0 {
		log.Printf(format, a...)
	}
	return
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	quorum     int
	proposeMap map[int]*ProposeInfo
	peerNum    int

	forgottenSeq  int   // 遗忘的 Seq
	maxSeq        int   // 已知最大的 Seq
	maxDecidedSeq int   // 当前完成最大的 Seq
	peerDoneSeq   []int // 完成的 Seq
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Start the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	// 提议已经完成
	if seq <= px.peerDoneSeq[px.me] {
		return
	}

	info, ok := px.proposeMap[seq]
	if ok {
		// 存在协程提议
		if info.start {
			return
		}

		// 提议已完成
		if info.status != Pending {
			return
		}

		info.start = true
	} else {
		info = &ProposeInfo{
			status: Pending,
			accept: nil,
			start:  true,
		}
		px.proposeMap[seq] = info
	}

	// 更新状态机
	px.maxSeq = max(px.maxSeq, seq)

	go px.propose(seq, v)
}

// Proposer 发起提议
// 返回其他节点的最大 rnd 及其对应的 value（如果有的话），当前提议状态，该阶段是否成功
func (px *Paxos) proposePhase(args *ProposeArgs) (Round, *Data, Fate, bool) {
	var highestRnd Round
	var accept *Data

	ch := make(chan *ProposeReply, px.peerNum)

	px.mu.Lock()
	args.Commit = &DecidedArgs{
		ServerId: px.me,
		MaxSeq:   px.maxSeq,
		DoneSeq:  px.peerDoneSeq[px.me],
	}

	for id, peer := range px.peers {
		reply := &ProposeReply{}
		if id == px.me {
			_ = px.proposeHandlerL(args, reply)
			ch <- reply
		} else {
			go func(id int, peer string) {
				ok := call(peer, "Paxos.ProposeHandler", args, reply)

				if !ok {
					reply = nil
				} else {
					// 更新状态机
					px.mu.Lock()

					//if reply.Status == Decided {
					//	// TODO 直接应用状态机
					//	if info, ok := px.proposeMap[args.Seq]; ok {
					//		if info.status == Pending {
					//			info.status = Decided
					//			info.accept = reply.Accept
					//		}
					//	}
					//}

					px.maxSeq = max(px.maxSeq, reply.Commit.MaxSeq)
					px.peerDoneSeq[id] = max(reply.Commit.DoneSeq, px.peerDoneSeq[id])
					px.mu.Unlock()
				}

				ch <- reply
			}(id, peer)
		}
	}
	px.mu.Unlock()

	failingCount := 0
	succeedCount := 0
	for {
		var reply *ProposeReply
		select {
		case reply = <-ch:
		case <-time.After(500 * time.Millisecond):
			reply = nil
		}

		if reply == nil {
			failingCount++
		} else {
			// Seq 已经被确认
			//if reply.Status != Pending {
			//	return highestRnd, reply.Accept.Value, reply.Status, false
			//}

			// 选择轮次最高的值
			if reply.Accept != nil {
				if accept == nil || reply.Accept.Rnd.Ge(&accept.Rnd) {
					accept = reply.Accept
				}
			}

			// 选择当前最高的轮次
			if reply.Rnd.Ge(&highestRnd) {
				highestRnd = reply.Rnd
			}

			if reply.Reject {
				failingCount++
			} else {
				succeedCount++
			}
		}

		if failingCount >= px.quorum || succeedCount >= px.quorum {
			return highestRnd, accept, Pending, succeedCount >= px.quorum
		}
	}
}

// accept phase 返回当前选择最高的轮次及其是否成功
func (px *Paxos) acceptPhase(args *AcceptArgs) (Round, bool) {
	rnd := args.Rnd

	ch := make(chan *AcceptReply, px.peerNum)

	px.mu.Lock()
	args.Commit = &DecidedArgs{
		ServerId: px.me,
		MaxSeq:   px.maxSeq,
		DoneSeq:  px.peerDoneSeq[px.me],
	}

	for i, peer := range px.peers {
		reply := &AcceptReply{
			Commit: &DecidedReply{},
		}
		if i == px.me {
			_ = px.acceptHandlerL(args, reply)
			ch <- reply
		} else {
			go func(id int, peer string) {
				ok := call(peer, "Paxos.AcceptHandler", args, reply)
				if ok {
					px.mu.Lock()
					px.maxSeq = max(px.maxSeq, reply.Commit.MaxSeq)
					px.peerDoneSeq[id] = max(reply.Commit.DoneSeq, px.peerDoneSeq[id])
					px.mu.Unlock()
				} else {
					reply = nil
				}

				ch <- reply
			}(i, peer)
		}
	}
	px.mu.Unlock()

	failingCount := 0
	succeedCount := 0
	for {
		var reply *AcceptReply
		select {
		case reply = <-ch:
		case <-time.After(500 * time.Millisecond):
		}

		if reply == nil {
			failingCount++
		} else {

			// 选择当前最高的轮次
			if reply.Rnd.Ge(&rnd) {
				rnd = reply.Rnd
			}

			if reply.Reject {
				failingCount++
			} else {
				succeedCount++
			}
		}

		if failingCount >= px.quorum || succeedCount >= px.quorum {
			return rnd, succeedCount >= px.quorum
		}
	}
}

func (px *Paxos) commitPhase(args *DecidedArgs) {
	// Commit
	px.mu.Lock()
	defer px.mu.Unlock()

	args.ServerId = px.me
	args.DoneSeq = px.peerDoneSeq[px.me]
	args.MaxSeq = px.maxSeq

	// 发送 Commit Msg
	for id, peer := range px.peers {
		reply := &DecidedReply{}
		if id == px.me {
			_ = px.decidedHandlerL(args, reply)
			continue
		}
		go func(id int, peer string) {
			ok := call(peer, "Paxos.DecidedHandler", args, reply)
			if ok {
				px.mu.Lock()
				defer px.mu.Unlock()
				px.maxSeq = max(px.maxSeq, reply.MaxSeq)
				px.peerDoneSeq[id] = max(reply.DoneSeq, px.peerDoneSeq[id])
			}
		}(id, peer)
	}
}

// 发起提议
func (px *Paxos) propose(seq int, v interface{}) {
	rnd := Round{
		N:        0,
		ServerId: px.me,
	}
	var accept *Data

	for !px.isdead() {
		rnd.N++
		DPrintf("[%v]proposal seq[%v]", px.me, seq)

		// Phase-1
		proposeArgs := &ProposeArgs{
			ServerId: px.me,
			Seq:      seq,
			Rnd:      rnd,
		}
		replyRnd, replyAccept, _, ok := px.proposePhase(proposeArgs)

		// TODO 提议已经完成
		//if status != Pending {
		//	v = val
		//	break
		//}

		// 提议失败, 选择当前已知最大的 rnd 重新提议
		if !ok {
			if replyRnd.Ge(&rnd) {
				rnd = replyRnd
			}
			time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
			continue
		}

		if replyAccept != nil {
			// 若存在值则选择已经存在的值
			accept = replyAccept
			accept.Rnd = rnd
		} else {
			// 否则选择自己的值
			accept = &Data{
				Seq:   seq,
				Rnd:   rnd,
				Value: v,
			}
		}

		DPrintf("[%v]accept seq[%v]\trnd:%v\taccept:%v\n", px.me, seq, rnd, accept)

		// Phase-2
		acceptArgs := &AcceptArgs{
			Seq:    seq,
			Rnd:    rnd,
			Accept: accept,
		}

		// 提议失败, 选择当前已知最大的 rnd 重新提议
		replyRnd, ok = px.acceptPhase(acceptArgs)
		if !ok {
			if replyRnd.Ge(&rnd) {
				rnd = replyRnd
			}
			time.Sleep(time.Duration(rand.Int31n(50)) * time.Millisecond)
			continue
		}

		DPrintf("[%v]decided seq[%v]\trnd:%v\taccept:%v\n", px.me, seq, rnd, accept)

		// Phase-3
		px.commitPhase(&DecidedArgs{
			Rnd:      rnd,
			ServerId: px.me,
			Data: []*Data{
				accept,
			},
		})
		break
	}
}

// Done the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	px.peerDoneSeq[px.me] = seq
}

// 遗忘 Seq 及其之前的 Paxos 实例
func (px *Paxos) forgotten(seq int) {
	if seq <= px.forgottenSeq {
		return
	}

	for s := px.forgottenSeq; s <= seq; s++ {
		delete(px.proposeMap, s)
	}
	px.forgottenSeq = seq
}

// Max the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

// Min Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	// 获取已完成的最小实例
	minSeq := px.peerDoneSeq[0]
	for _, s := range px.peerDoneSeq {
		minSeq = min(minSeq, s)
	}

	if minSeq != px.forgottenSeq {
		px.forgotten(minSeq)
	}

	return minSeq + 1
}

// Status the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq <= px.peerDoneSeq[px.me] {
		return Forgotten, nil
	}

	info, ok := px.proposeMap[seq]
	if !ok {
		return Pending, nil
	}

	if info.status == Decided {
		return Decided, info.accept.Value
	}

	return Pending, nil
}

// Kill tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// Make the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.peerNum = len(peers)
	px.quorum = (px.peerNum + 1) >> 1
	px.proposeMap = make(map[int]*ProposeInfo)

	px.maxSeq = -1
	px.maxDecidedSeq = -1
	px.forgottenSeq = -1

	px.peerDoneSeq = make([]int, px.peerNum)
	for i := 0; i < px.peerNum; i++ {
		px.peerDoneSeq[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					// fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
