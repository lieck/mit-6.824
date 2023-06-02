package shardkv

import (
	"mit6.824/src/config"
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "mit6.824/src/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "mit6.824/src/shardmaster"

func DPrintf(format string, a ...interface{}) {
	if config.ShardKVServerDebugLog > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	Empty OpType = iota
	Get
	Put
	Append
	ChangConfig
)

// ChangeConfig 配置变更
type ChangeConfig struct {
	ConfigNum     int
	ShardId       int
	RequestFilter map[int64]int
	Kv            map[string]string
	Apply         bool
}

func (c *ChangeConfig) Eq(a *ChangeConfig) bool {
	if c.ConfigNum != a.ConfigNum || c.ShardId != a.ShardId {
		return false
	}
	return true
}

type Op struct {
	// Your definitions here.
	Type       OpType
	ShardId    int
	Key        string
	Value      string
	ClientId   int64
	RequestSeq int

	Config *ChangeConfig

	Tag      Err
	GetValue string
}

func (op *Op) Eq(a *Op) bool {
	if op.Type != a.Type || op.ShardId != a.ShardId || op.Key != a.Key || op.Value != a.Value {
		return false
	}
	if op.ClientId != a.ClientId || op.RequestSeq != a.RequestSeq {
		return false
	}
	if op.Config != nil && a.Config != nil {
		return op.Config.Eq(a.Config)
	}
	if op.Config != nil || a.Config != nil {
		return false
	}
	return true
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	kv            []map[string]string
	requestFilter []map[int64]int

	startApplyConfig bool
	applyConfig      *shardmaster.Config // 当前要应用的配置信息, 若当前没有应用为 -1
	currConfig       *shardmaster.Config // 当前的配置信息

	shardData map[int][]*ChangeConfig // 当前配置暂存的更新信息

	logMap       map[int]*Op
	waitCh       map[int]chan *Op
	lastApplySeq int // 最后应用的 Paxos 实例
	lastWaitSeq  int
}

func (kv *ShardKV) getPaxosSeq(seq int) (*Op, bool) {
	status, value := kv.px.Status(seq)
	if status == paxos.Decided {
		value, ok := (value).(Op)
		if ok {
			return &value, true
		} else {
			panic(fmt.Sprintf("[%v:%v]paxos status error\tval=%v\n", kv.gid, kv.me, value))
		}
	}

	if status == paxos.Forgotten {
		return nil, true
	}

	return nil, false
}

// 等待 Seq 完成提议，propose 表示是否允许重新提议获取
func (kv *ShardKV) waitDecided(seq int, propose bool) *Op {
	to := 10 * time.Millisecond
	for !kv.isdead() {
		val, ok := kv.getPaxosSeq(seq)
		if ok {
			return val
		}

		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
	return nil
}

// 获取实例应用
func (kv *ShardKV) getSeqApply(startSeq int, endSeq int) {
	propose := true
	for seq := startSeq; seq < endSeq; seq++ {
		if op, ok := kv.getPaxosSeq(seq); ok {
			kv.applyLog(seq, op)
			continue
		}

		if propose {
			for i := seq; i < endSeq; i++ {
				kv.px.Start(i, Op{})
			}
			propose = false
		}

		op := kv.waitDecided(seq, false)
		kv.applyLog(seq, op)
	}
}

// 提议并等待完成
func (kv *ShardKV) proposeLog(op *Op) *Op {
	for !kv.isdead() {

		kv.mu.Lock()
		ch := make(chan *Op, 100)
		seq := kv.px.Max() + 1
		kv.px.Start(seq, *op)
		kv.waitCh[seq] = ch

		// 2. 若小于 seq 未启动协程监听并应用，则启用新的协程获取并应用
		if kv.lastWaitSeq+1 < seq {
			lastWaitSeq := kv.lastWaitSeq + 1
			go kv.getSeqApply(lastWaitSeq, seq)
		}
		kv.lastWaitSeq = seq
		kv.mu.Unlock()

		if op.Config == nil {
			DPrintf("[%v:%v]proposeLog seq[%v] op=%v\n", kv.gid, kv.me, seq, op)
		} else {
			DPrintf("[%v:%v]proposeConfig seq[%v] op=%v\n", kv.gid, kv.me, seq, op.Config)
		}

		go func() {
			op := kv.waitDecided(seq, false)
			kv.applyLog(seq, op)
		}()

		replyOp := <-ch
		if replyOp != nil && replyOp.Eq(op) {
			return replyOp
		}
	}
	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.updateKVConfig(-1)
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// Setunreliable please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// StartServer Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.shardData = make(map[int][]*ChangeConfig)

	kv.logMap = make(map[int]*Op)
	kv.waitCh = make(map[int]chan *Op)
	kv.lastApplySeq = -1
	kv.lastWaitSeq = -1

	c := kv.sm.Query(0)
	kv.currConfig = &c

	kv.kv = make([]map[string]string, len(c.Shards))
	kv.requestFilter = make([]map[int64]int, len(c.Shards))

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		cnt := 0
		for kv.isdead() == false {
			cnt++
			// 尝试更新
			seq := kv.px.Max()
			kv.mu.Lock()
			if seq != kv.lastWaitSeq {
				go kv.getSeqApply(kv.lastWaitSeq+1, seq+1)
				kv.lastWaitSeq = seq
			}
			kv.mu.Unlock()

			if cnt == 20 {
				cnt = 0
				go kv.proposeLog(&Op{})
			}

			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
