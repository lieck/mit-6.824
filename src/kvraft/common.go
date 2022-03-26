package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrWrongIndex   = "ErrWrongIndex"
	ErrWrongNetwork = "ErrWrongNetwork"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	RequestId int64 // 请求ID
	ClientId  int64 // 客户端ID
	ServerId  int
}

type PutAppendReply struct {
	Err       Err
	LastIndex int // 最后应用的请求状态
	ServerId  int // leaderID
}

type GetArgs struct {
	Key       string
	RequestId int64 // 请求ID
	ClientId  int64 // 客户端ID
	LastIndex int   // 最后应用的请求状态
}

type GetReply struct {
	Err       Err
	Value     string
	LastIndex int // 最后应用的请求状态
	ServerId  int
}
