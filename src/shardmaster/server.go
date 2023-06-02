package shardmaster

import (
	"mit6.824/src/config"
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"

import "mit6.824/src/paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

func DPrintf(format string, a ...interface{}) {
	if config.ShardMasterDebugLog > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []*Config // indexed by config num

	lastApplySeq atomic.Value // 最后应用的 Paxos 实例
	lastWaitSeq  atomic.Value
}

type OpType int

const (
	Get OpType = iota + 1
	Put
)

type Op struct {
	// Your data here.
	Op       OpType
	ServerID int
	Configs  *Config
	Debug    string
}

func (o *Op) Eq(a *Op) bool {
	if o.Op != a.Op {
		return false
	}
	if o.Op == Get {
		return o.ServerID == a.ServerID
	}
	return o.Configs.Eq(a.Configs)
}

// Join RPC的参数是一个唯一的非零副本组标识符（GID）和一个服务器端口数组。
// Shardmaster 应该通过创建一个新的包含新副本组的配置来做出反应。
// 新配置应该尽可能均匀地将分片分配给各个组，并且应该尽可能少地移动分片以实现该目标。
func (sm *ShardMaster) Join(args *JoinArgs, _ *JoinReply) error {
	// Your code here.
	fun := func(sm *ShardMaster, argsInter interface{}) *Op {
		args := argsInter.(*JoinArgs)
		sm.mu.Lock()
		defer sm.mu.Unlock()

		lastIndex := len(sm.configs) - 1

		// 是否已经在当前组
		if _, ok := sm.configs[lastIndex].Groups[args.GID]; ok {
			return nil
		}

		newConfig := sm.configs[lastIndex].Copy()
		newConfig.Num++
		newConfig.Groups[args.GID] = args.Servers

		// 当前 newConfig.Groups <= 10 需要平衡
		if len(newConfig.Groups) <= 10 {
			newConfig.Balance()
		}

		return &Op{Configs: &newConfig, Debug: fmt.Sprint("Join_", args.GID)}
	}

	sm.propose(fun, args)

	DPrintf("[%v]ConfigJoin %v", sm.me, args.GID)
	return nil
}

// Leave RPC 的参数是先前加入的组的GID。
// Shardmaster 应该创建一个不包括该组并将组的分片分配给剩余组的新配置。
// 新配置应该尽可能均匀地将分片分配给各个组，并且应该尽可能少地移动分片以实现该目标。
func (sm *ShardMaster) Leave(args *LeaveArgs, _ *LeaveReply) error {
	// Your code here.
	fun := func(sm *ShardMaster, argsInter interface{}) *Op {
		args := argsInter.(*LeaveArgs)
		lastIndex := len(sm.configs) - 1

		if _, ok := sm.configs[lastIndex].Groups[args.GID]; !ok {
			return nil
		}

		newConfig := sm.configs[lastIndex].Copy()
		newConfig.Num++

		// 移除成员
		delete(newConfig.Groups, args.GID)
		for i := range newConfig.Shards {
			if newConfig.Shards[i] == args.GID {
				newConfig.Shards[i] = 0
			}
		}

		// 重新平衡
		newConfig.Balance()

		return &Op{Configs: &newConfig, Debug: fmt.Sprint("Leave_", args.GID)}
	}

	sm.propose(fun, args)

	DPrintf("[%v]ConfigLeave %v", sm.me, args.GID)
	return nil
}

// Move RPC 的参数是一个分片号和一个GID。
// Shardmaster 应该创建一个新的配置，其中该分片被分配给该组。
// Move 的主要目的是允许我们测试您的软件，但如果某些分片比其他分片更受欢迎或某些副本组比其他副本组更慢，则可能还有用于微调负载平衡。
// Move之后的Join或Leave可能会撤销Move，因为Join和Leave会重新平衡。
func (sm *ShardMaster) Move(args *MoveArgs, _ *MoveReply) error {
	// Your code here.
	fun := func(sm *ShardMaster, argsInter interface{}) *Op {
		args := argsInter.(*MoveArgs)

		sm.mu.Lock()
		defer sm.mu.Unlock()

		lastIndex := len(sm.configs) - 1
		if sm.configs[lastIndex].Shards[args.Shard] == args.GID {
			return nil
		}

		newConfig := sm.configs[lastIndex].Copy()
		newConfig.Num++
		newConfig.Shards[args.Shard] = args.GID
		return &Op{Configs: &newConfig, Debug: fmt.Sprint("Move_", args.GID)}
	}
	sm.propose(fun, args)
	return nil
}

// Query RPC 用于查询配置
// Shardmaster 回复具有该编号的配置，若数字为 -1 或 大于已知的最大配置号，则返回最新配置
// Query(-1)的结果应反映在发送Query(-1) RPC之前已完成的所有Join、Leave或Move。
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	// 存在配置, 直接获取
	if args.Num != -1 {
		sm.mu.Lock()
		if args.Num < len(sm.configs) {
			reply.Config = *sm.configs[args.Num]
			sm.mu.Unlock()
			return nil
		}
		sm.mu.Unlock()
	}

	// 发送日志获取最新配置
	sm.getNewestConfig()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	idx := args.Num
	if idx == -1 || idx >= len(sm.configs) {
		idx = len(sm.configs) - 1
	}
	reply.Config = *sm.configs[idx]
	return nil
}

// 等待 Seq 完成提议，propose 表示是否允许重新提议获取
func (sm *ShardMaster) waitDecided(seq int, propose bool) *Op {
	// 等待 paxos 提议
	to := 10 * time.Millisecond
	for {
		status, value := sm.px.Status(seq)
		if status == paxos.Decided {
			value, ok := (value).(Op)
			if ok {
				return &value
			} else {
				DPrintf("[%v]PaxosStatusErr seq[%v]\n", sm.me, seq)
				return nil
			}
		}

		if status == paxos.Forgotten {
			return nil
		}

		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
		// 超过 10 + 20 + 40  时重新提议
		if propose && to >= 50*time.Millisecond {
			propose = false
			sm.px.Start(seq, Op{
				Op:       0,
				ServerID: 0,
				Configs:  nil,
				Debug:    "Empty",
			})
		}
	}
}

// 应用当前日志的配置, 返回值表示是否应用成功, 如果之前的 seq 未被应用，则会进行等待
// 当 c.num 已经被占用时，返回失败
func (sm *ShardMaster) applyConfig(seq int, op *Op) bool {
	if op == nil {
		return false
	}

	sm.waitApply(seq - 1)
	sm.px.Done(seq)
	sm.px.Min()

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if seq <= sm.lastApplySeq.Load().(int) {
		return false
	}

	sm.lastApplySeq.Store(seq)

	if op.Configs == nil {
		DPrintf("[%v]applyConfig\tseq[%v]\top=%v\n", sm.me, seq, op)
		return false
	}

	if op.Configs.Num == len(sm.configs) {
		DPrintf("[%v]applyConfigOk\tseq[%v]\top=%v\tconfig=%v\tconfigLen=%v\n", sm.me, seq, op.Debug, op.Configs, len(op.Configs.Groups))
		sm.configs = append(sm.configs, op.Configs)
		return true
	}

	DPrintf("[%v]applyConfigErr\tseq[%v]\top=%v\tconfig=%v\n", sm.me, seq, op.Debug, op.Configs)
	return false
}

// 等待 seq 应用成功, 即循环判断 lastApplySeq >= seq
func (sm *ShardMaster) waitApply(seq int) {
	to := 10 * time.Millisecond
	for {
		if sm.lastApplySeq.Load().(int) >= seq {
			return
		}
		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
}

// 获取指定的服务器配置
func (sm *ShardMaster) getConfig(start int, end int) {
	propose := true
	for seq := start; seq < end; seq++ {
		var status paxos.Fate
		var value interface{}
		for i := 0; i < 3; i++ {
			status, value = sm.px.Status(seq)
			if status != paxos.Pending {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if status == paxos.Decided {
			// 直接应用
			value, ok := (value).(Op)
			if ok {
				sm.applyConfig(seq, &value)
			} else {
				DPrintf("[%v]PaxosStatusErr seq[%v]\n", sm.me, seq)
			}
			continue
		}

		if status == paxos.Forgotten {
			continue
		}

		// paxos 中不存在信息，需要重新启动实例
		if propose {
			propose = false
			for i := seq; i < end; i++ {
				sm.px.Start(i, Op{
					Op:       0,
					ServerID: 0,
					Configs:  nil,
					Debug:    "Empty",
				})
			}
		}

		op := sm.waitDecided(seq, true)
		sm.applyConfig(seq, op)
	}

}

// 发送 Get 日志请求获取最新的服务器配置
func (sm *ShardMaster) getNewestConfig() {
	op := Op{
		Op:       Get,
		ServerID: sm.me,
	}

	for !sm.isdead() {
		seq := sm.px.Max() + 1
		sm.px.Start(seq, op)

		// 获取之前未提交的数据
		lastWaitSeq := sm.lastWaitSeq.Load().(int)
		if lastWaitSeq+1 < seq {
			go sm.getConfig(lastWaitSeq+1, seq)
		}
		sm.lastWaitSeq.Store(seq)

		replyOp := sm.waitDecided(seq, false)
		sm.applyConfig(seq, replyOp)

		if replyOp != nil && replyOp.Eq(&op) {
			break
		}
	}
}

// 提议配置
// 1. 获取当前的最新配置
// 2. 应用
// 3. 提议新配置
func (sm *ShardMaster) propose(fun func(*ShardMaster, interface{}) *Op, argsInter interface{}) {
	for !sm.isdead() {
		seq := sm.px.Max() + 1

		lastWaitSeq := sm.lastWaitSeq.Load().(int)
		sm.lastWaitSeq.Store(seq - 1)
		if lastWaitSeq+1 < seq {
			sm.getConfig(lastWaitSeq+1, seq)
		}

		sm.waitApply(seq - 1)
		op := fun(sm, argsInter)
		if op == nil {
			return
		}

		DPrintf("[%v]proposeConfig seq[%v]\tconfig=%v\tInfo=%v\n", sm.me, seq, op.Configs, op.Debug)
		sm.lastWaitSeq.Store(seq)
		sm.px.Start(seq, *op)
		replyOp := sm.waitDecided(seq, false)
		if replyOp == nil {
			continue
		}
		if !sm.applyConfig(seq, replyOp) {
			continue
		}

		if replyOp != nil && replyOp.Configs.Eq(op.Configs) {
			return
		}
	}
}

// Kill please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.lastWaitSeq.Store(-1)
	sm.lastApplySeq.Store(-1)

	sm.configs = make([]*Config, 1)
	sm.configs[0] = &Config{}
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
