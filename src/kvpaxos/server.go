package kvpaxos

import (
	"mit6.824/src/paxos"
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"

//import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
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
	return a.Op == b.Op && a.Key == b.Key && a.Value == b.Value
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
	logMap        map[int64]*Op
	requestFilter map[int64]int // 过滤器, 表示 Client 已经处理完成的请求 Seq
	lastApplySeq  atomic.Int64  // 最后应用的 Paxos 实例
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	kv.mu.Lock()
	// 请求过滤
	if clientSeq, ok := kv.requestFilter[args.ClientId]; ok && clientSeq >= args.RequestSeq {
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

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	// 请求过滤
	kv.mu.Lock()
	if clientSeq, ok := kv.requestFilter[args.ClientId]; ok && clientSeq >= args.RequestSeq {
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

func (kv *KVPaxos) startPropose(op *Op) {
	for !kv.isdead() {
		seq := kv.px.Max() + 1
		kv.px.Start(seq, op)

		// 等待 paxos 提议
		getOp := kv.waitPropose(seq)
		kv.applyLog(int64(seq), getOp)

		// 表示需要重新提议
		if getOp == nil || !getOp.Eq(op) {
			continue
		}

		// 等待应用
		kv.waitApplySeq(int64(seq))
		break
	}
}

// 从 paxos 中获取提议成功的日志
func (kv *KVPaxos) waitPropose(seq int) *Op {
	// 等待 paxos 提议
	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		if status == paxos.Decided {
			value, ok := (value).(*Op)
			if ok {
				return value
			}
			return nil
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) waitApplySeq(seq int64) {
	// 等待应用
	to := 10 * time.Millisecond
	isRetry := false

	for {
		currDoneSeq := kv.lastApplySeq.Load()

		// 应用成功
		if currDoneSeq >= seq {
			return
		}

		// 超过等待时间，重启之前的提议，并且监听之前请求的完成
		if !isRetry && to >= 300*time.Millisecond {
			isRetry = true
			kv.retryPaxos(seq)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// 应用日志操作
func (kv *KVPaxos) applyLog(seq int64, op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seq != kv.lastApplySeq.Load()+1 {
		// 不可以应用
		kv.logMap[seq] = op
		return
	}

	// 可以直接应用
	if op != nil && !kv.opFilterL(op) {
		if op.Op == Put {
			kv.kv[op.Key] = op.Value
		} else if op.Op == Append {
			kv.kv[op.Key] += op.Value
		}
		kv.requestFilter[op.ClientId] = op.RequestSeq
	}

	// 判断后续的 log 是否可以应用
	for {
		if op, ok := kv.logMap[seq+1]; ok {
			seq++
			if op != nil && !kv.opFilterL(op) {
				if op.Op == Put {
					kv.kv[op.Key] = op.Value
				} else if op.Op == Append {
					kv.kv[op.Key] += op.Value
				}
				kv.requestFilter[op.ClientId] = op.RequestSeq
			}
		} else {
			break
		}
	}

	kv.lastApplySeq.Store(seq)
	kv.px.Done(int(seq))
}

// 重试 seq 之前未应用日志，并等待之前的日志应用完成
func (kv *KVPaxos) retryPaxos(seq int64) {
	for s := int(kv.lastApplySeq.Load()) + 1; s < int(seq); s++ {
		kv.px.Start(s, nil)
	}

	currSeq := kv.lastApplySeq.Load() + 1
	for currSeq < seq {
		op := kv.waitPropose(int(currSeq))
		kv.applyLog(currSeq, op)

		currSeq = kv.lastApplySeq.Load() + 1
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
	kv.logMap = make(map[int64]*Op)
	kv.requestFilter = make(map[int64]int)

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
