package kvpaxos

import (
	"mit6.824/src/config"
	"mit6.824/src/paxos"
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"

import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

func DPrintf(format string, a ...interface{}) {
	if config.KVPaxosDebugLog > 0 {
		log.Printf(format, a...)
	}
	return
}

type KvOpType int

const (
	Get KvOpType = iota + 1
	Put
	Append
)

func toOpType(op string) KvOpType {
	if op == "Put" {
		return Put
	}
	return Append
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Op         KvOpType
	ClientId   int64
	RequestSeq int
}

func (a *Op) Eq(b *Op) bool {
	return a.Op == b.Op && a.Key == b.Key && a.Value == b.Value && a.ClientId == b.ClientId && a.RequestSeq == b.RequestSeq
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kv            map[string]string
	logMap        map[int]*Op
	requestFilter map[int64]int // 过滤器, 表示 Client 已经处理完成的请求 Seq
	lastApplySeq  atomic.Value  // 最后应用的 Paxos 实例
	lastWaitSeq   int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	// 请求过滤
	kv.mu.Lock()
	if clientSeq, ok := kv.requestFilter[args.ClientId]; ok && clientSeq >= args.RequestSeq {
		DPrintf("[%v]serverGetFilter\tkey=%v\tvalue=%v\tclient=[%v:%v]", kv.me, args.Key, reply.Value, args.ClientId, args.RequestSeq)
		if val, ok := kv.kv[args.Key]; ok {
			reply.Value = val
		}
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	op := &Op{
		Key:        args.Key,
		Op:         Get,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}
	kv.startPropose(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Paxos 完成, 获取值
	if val, ok := kv.kv[args.Key]; ok {
		reply.Value = val
	}
	DPrintf("[%v]serverGet\tkey=%v\tvalue=%v\tclient=[%v:%v]", kv.me, args.Key, reply.Value, args.ClientId, args.RequestSeq)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, _ *PutAppendReply) error {
	// Your code here.
	// 请求过滤
	kv.mu.Lock()
	if clientSeq, ok := kv.requestFilter[args.ClientId]; ok && clientSeq >= args.RequestSeq {
		DPrintf("[%v]serverFilter%v\tkey=%v\tvalue=%v\tclient=[%v:%v]", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.RequestSeq)
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// 发起提议
	op := &Op{
		Key:        args.Key,
		Value:      args.Value,
		Op:         toOpType(args.Op),
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
	}

	kv.startPropose(op)
	DPrintf("[%v]server%v\tkey=%v\tvalue=%v\tclient=[%v:%v]", kv.me, args.Op, args.Key, args.Value, args.ClientId, args.RequestSeq)
	return nil
}

// 判断请求是否应用过
// return true 表示应用，否则没有
func (kv *KVPaxos) opFilterL(op *Op) bool {
	if s, ok := kv.requestFilter[op.ClientId]; ok {
		return s >= op.RequestSeq
	}
	return false
}

// 开始提议并等待日志应用
// 1. 获取 px.max + 1 作为 seq 提议
// 2. 若小于 seq 未启动协程监听并应用，则启用新的协程获取并应用
// 3. 等待 seq 提议完成并应用
// 4. 若应用的 seq 与提议的不一致，重试
func (kv *KVPaxos) startPropose(op *Op) {
	for !kv.isdead() {
		kv.mu.Lock()
		// 1. 获取 px.max + 1 作为 seq 提议
		seq := kv.px.Max() + 1
		kv.px.Start(seq, *op)
		DPrintf("[%v]startPropose\tseq[%v]\top=%v\tkey=%v\tval=%v\n", kv.me, seq, op.Op, op.Key, op.Value)

		// 2. 若小于 seq 未启动协程监听并应用，则启用新的协程获取并应用
		if kv.lastWaitSeq+1 < seq {
			lastWaitSeq := kv.lastWaitSeq + 1
			go kv.getProposeApply(lastWaitSeq, seq)
		}
		// 更新最后监听的 seq
		kv.lastWaitSeq = seq
		kv.mu.Unlock()

		// 3. 等待 seq 提议完成并应用
		replyOp := kv.waitPropose(seq)
		applyOk := kv.applyLog(seq, replyOp)

		// 4. 若应用的 seq 与提议的不一致，重试
		if replyOp == nil || !replyOp.Eq(op) {
			continue
		}

		if !applyOk {
			// 日志应用未成功，等待完成应用
			kv.waitApply(seq)
		}

		break
	}
}

// 等待并获取 paxos 中获取提议成功的日志
func (kv *KVPaxos) waitPropose(seq int) *Op {
	// 等待 paxos 提议
	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		if status == paxos.Decided {
			value, ok := (value).(Op)
			if ok {
				return &value
			}
			return nil
		}
		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
}

// 等待 seq 应用成功, 即循环判断 lastApplySeq >= seq
func (kv *KVPaxos) waitApply(seq int) {
	to := 10 * time.Millisecond
	for {
		if kv.lastApplySeq.Load().(int) >= seq {
			return
		}
		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
}

// 应用日志操作
func (kv *KVPaxos) applyLog(seq int, op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seq != kv.lastApplySeq.Load().(int)+1 {
		// 不可以应用
		kv.logMap[seq] = op
		return false
	}

	// 可以直接应用
	if op != nil && !kv.opFilterL(op) {
		if op.Op == Put {
			kv.kv[op.Key] = op.Value
			DPrintf("[%v]applyLog seq[%v] put %v=%v", kv.me, seq, op.Key, op.Value)
		} else if op.Op == Append {
			kv.kv[op.Key] += op.Value
			DPrintf("[%v]applyLog seq[%v] append %v=%v", kv.me, seq, op.Key, op.Value)
		} else {
			DPrintf("[%v]applyLog seq[%v] get %v", kv.me, seq, op.Key)
		}
		kv.requestFilter[op.ClientId] = max(op.RequestSeq, kv.requestFilter[op.ClientId])
	}

	// 判断后续的 log 是否可以应用
	for {
		if op, ok := kv.logMap[seq+1]; ok {
			seq++
			if op != nil && !kv.opFilterL(op) {
				if op.Op == Put {
					kv.kv[op.Key] = op.Value
					DPrintf("[%v]applyLog seq[%v] put %v=%v", kv.me, seq, op.Key, op.Value)
				} else if op.Op == Append {
					kv.kv[op.Key] += op.Value
					DPrintf("[%v]applyLog seq[%v] append %v=%v", kv.me, seq, op.Key, op.Value)
				} else {
					DPrintf("[%v]applyLog seq[%v] get %v", kv.me, seq, op.Key)
				}
				kv.requestFilter[op.ClientId] = max(op.RequestSeq, kv.requestFilter[op.ClientId])
			}
		} else {
			break
		}
	}

	kv.lastApplySeq.Store(seq)
	kv.px.Done(seq)
	kv.px.Min()

	return true
}

func (kv *KVPaxos) getProposeApply(startSeq int, endSeq int) {
	DPrintf("[%v]getProposeApply\tstart[%v]\tend[%v]\n", kv.me, startSeq, endSeq)
	for seq := startSeq; seq < endSeq; seq++ {
		var status paxos.Fate
		var value interface{}
		for i := 0; i < 3; i++ {
			status, value = kv.px.Status(seq)
			if status == paxos.Decided {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if status != paxos.Pending {
			// 直接应用
			value, _ := (value).(Op)
			kv.applyLog(seq, &value)
			continue
		}

		// paxos 中不存在信息，需要重新启动实例
		kv.px.Start(seq, nil)
		op := kv.waitPropose(seq)
		kv.applyLog(seq, op)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kv = make(map[string]string)
	kv.logMap = make(map[int]*Op)
	kv.requestFilter = make(map[int64]int)
	kv.lastWaitSeq = -1

	kv.lastApplySeq.Store(-1)
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
