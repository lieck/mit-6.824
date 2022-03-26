package kvraft

import (
	"crypto/rand"
	"time"

	//"labrpc"
	"6.824/labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex

	meId      int64
	requestId int64

	leaderId    int
	getServerId int

	leaderErrCnt int
	lastIndex    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.meId = nrand()
	ck.requestId = 1
	ck.leaderId = -1
	ck.getServerId = int(nrand()) % len(servers)
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.requestId += 1
	args := GetArgs{
		Key:       key,
		RequestId: ck.requestId,
		ClientId:  ck.meId,
		LastIndex: ck.lastIndex,
	}
	reply := GetReply{}
	serverId := ck.getServerId
	ck.mu.Unlock()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for {
		//DPrintf("Client\tto Server%v\tGet %v", serverId, args.Key)
		go ck.requestGet(cond, serverId, &args, &reply)

		mu.Lock()
		cond.Wait()
		mu.Unlock()

		ck.mu.Lock()
		if reply.Err == OK {
			ck.getServerId = reply.ServerId
			ck.lastIndex = reply.LastIndex
			ck.mu.Unlock()
			return reply.Value
		} else {
			ck.getServerId = (ck.getServerId + 1) % len(ck.servers)
			for ck.getServerId == ck.leaderId {
				ck.getServerId = (ck.getServerId + 1) % len(ck.servers)
			}
			serverId = ck.getServerId
		}
		ck.mu.Unlock()
	}

}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.requestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: ck.requestId,
		ClientId:  ck.meId,
	}
	reply := PutAppendReply{}
	serverId := ck.leaderId
	ck.mu.Unlock()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	num := 0

	//DPrintf("Client\tRID:%v\t%v %v:%v", args.RequestId, args.Op, args.Key, args.Value)

	for {
		cnt := 1
		if serverId != -1 {
			//DPrintf("Client\tto Server%v\t%v %v:%v", serverId, args.Op, args.Key, args.Value)
			args.ServerId = serverId
			go ck.requestPut(cond, serverId, &args, &reply)
		} else {
			for i := 0; i < len(ck.servers); i++ {
				//DPrintf("Client\tto Server%v\t%v %v:%v", i, args.Op, args.Key, args.Value)
				args.ServerId = i
				go ck.requestPut(cond, i, &args, &reply)
				cnt++
			}
		}

		mu.Lock()
		for {
			cond.Wait()
			cnt--
			if reply.Err == OK || cnt == 0 {
				break
			}
		}
		mu.Unlock()

		ck.mu.Lock()
		if reply.Err == OK {
			ck.leaderId = reply.ServerId
			ck.lastIndex = reply.LastIndex
			ck.mu.Unlock()
			//DPrintf("Client\tRID ok:%v\t%v %v:%v", args.RequestId, args.Op, args.Key, args.Value)
			return
		} else {
			if num >= 1 || reply.Err == ErrWrongLeader {
				ck.leaderId = -1
				serverId = -1
			}
			num++
		}
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) requestGet(cond *sync.Cond, serverId int, args *GetArgs, reply *GetReply) {
	is := true
	go func() {
		time.Sleep(50 * time.Millisecond)
		is = false
		cond.Broadcast()
	}()

	re := GetReply{}
	ok := ck.servers[serverId].Call("KVServer.Get", args, &re)
	if ok && re.Err == OK {
		*reply = re
		cond.Broadcast()
	} else if is {
		cond.Broadcast()
	}
}

func (ck *Clerk) requestPut(cond *sync.Cond, serverId int, args *PutAppendArgs, reply *PutAppendReply) {
	is := true
	go func() {
		time.Sleep(300 * time.Millisecond)
		is = false
		cond.Broadcast()
	}()

	re := PutAppendReply{}
	ok := ck.servers[serverId].Call("KVServer.PutAppend", args, &re)
	if ok && re.Err == OK {
		*reply = re
		cond.Broadcast()
	} else if is {
		cond.Broadcast()
	}
}
