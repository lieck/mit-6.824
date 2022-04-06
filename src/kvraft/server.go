package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	//
	//"labgob"
	//"labrpc"
	//"raft"
)

const Debug = true

var isInit = 2

func DPrintf(format string, a ...interface{}) {
	if isInit == 1 {
		logFile, err := os.OpenFile("./log.log", os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
		isInit = 2
	}

	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// RPC
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string

	ClientId  int64 // 服务器ID
	RequestId int64 // 请求ID
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data      map[string]string // kv数据库
	lastIndex int               // 表示已经应用的请求
	rs        requestStart      // 请求过滤

	applyMu   sync.Mutex
	applyCond *sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//DPrintf("Server%v\tGet %v", kv.me, args.Key)

	if kv.lastIndex >= args.LastIndex {
		reply.Err = OK
		reply.Value = kv.data[args.Key]
		reply.LastIndex = kv.lastIndex
		reply.ServerId = kv.me
	} else {
		reply.Err = ErrWrongIndex
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("Server%d\t%v %v:%v", kv.me, args.Op, args.Key, args.Value)

	kv.mu.Lock()
	// 判断是否执行过
	reStart := kv.rs.Get(args.ClientId, args.RequestId)
	if reStart == 2 {
		reply.Err = OK
		reply.ServerId = kv.me
		kv.mu.Unlock()
		return
	} else if reStart == 0 {
		if !kv.start(args) {
			kv.mu.Unlock()
			reply.Err = ErrWrongLeader
			return
		} else {
			kv.rs.Set(args.ClientId, args.RequestId, 1)
		}
	}
	kv.mu.Unlock()

	startTime := time.Now().UnixNano() / 1e6

	// 轮询判断
	kv.applyMu.Lock()
	for {
		kv.applyCond.Wait()

		if kv.rs.Get(args.ClientId, args.RequestId) == 2 {
			reply.Err = OK
			reply.ServerId = kv.me
			reply.LastIndex = kv.lastIndex
			kv.applyMu.Unlock()
			return
		}
		if time.Now().UnixNano()/1e6-startTime >= 250 {
			kv.applyMu.Unlock()
			return
		}
	}
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.applyCond = sync.NewCond(&kv.applyMu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyData()
	kv.rs = *MakeRequestStart()

	return kv
}

func (kv *KVServer) start(args *PutAppendArgs) bool {
	startMsg := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	_, _, isLeader := kv.rf.Start(startMsg)
	DPrintf("发送")
	return isLeader
}

func (kv *KVServer) applyData() {
	for kv.killed() == false {

		// 从Raft中获取成功应用的Entry
		msg := <-kv.applyCh
		command := (msg.Command).(Op)

		DPrintf("Server%v\tApply %v", kv.me, msg.CommandIndex)

		kv.mu.Lock()
		kv.lastIndex = msg.CommandIndex

		// 应用状态机
		if command.Op == "Append" {
			kv.data[command.Key] += command.Value
		} else {
			kv.data[command.Key] = command.Value
		}

		// 更新请求状态
		kv.rs.Set(command.ClientId, command.RequestId, 2)

		// 唤醒阻塞线程
		kv.applyCond.Broadcast()

		kv.mu.Unlock()
	}
}
