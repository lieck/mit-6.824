package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId   int64
	RequestSeq int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId   int64
	RequestSeq int
}

type GetReply struct {
	Err   Err
	Value string
}