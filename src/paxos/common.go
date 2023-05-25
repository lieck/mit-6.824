package paxos

// Round 投票轮次
type Round struct {
	N        int
	ServerId int
}

// Ge a 是否大于等于 b
func (a *Round) Ge(b *Round) bool {
	if a.N > b.N {
		return true
	} else if a.N < b.N {
		return false
	}
	return a.ServerId >= b.ServerId
}

// Gt a 是否大于 b
func (a *Round) Gt(b *Round) bool {
	if a.N > b.N {
		return true
	} else if a.N < b.N {
		return false
	}
	return a.ServerId > b.ServerId
}

// Eq a 是否等于 b
func (a *Round) Eq(b *Round) bool {
	return a.N == b.N && a.ServerId == b.ServerId
}

// Data 提议数据
type Data struct {
	Seq   int
	Rnd   Round
	Value interface{}
}

// ProposeInfo 提议状态
type ProposeInfo struct {
	status Fate
	rnd    Round // highest prepare seen
	accept Data  // highest accept seen
	start  bool  // 表示是否协程启动了提议
}

type ProposeArgs struct {
	ServerId int
	Seq      int // Paxos 实例
	Rnd      Round

	Commit *CommitArgs
}

type ProposeReply struct {
	Reject bool
	Rnd    Round
	accept Data
	Status Fate // Value 状态

	Commit *CommitReply
}

type AcceptArgs struct {
	Seq    int
	Rnd    Round
	Accept Data

	Commit *CommitArgs
}

type AcceptReply struct {
	Reject bool
	Rnd    Round
	Commit *CommitReply
}

type CommitArgs struct {
	ServerId int
	Data     []*Data // 数据
	MaxSeq   int     // 已知的最大 Seq
	DoneSeq  int     // 当前最小未完成提议的 Seq
}

type CommitReply struct {
	ServerId int
	MaxSeq   int // 已知的最大 Seq
	DoneSeq  int // 当前最小未完成提议的 Seq
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxRnd(a, b Round) Round {
	if a.Ge(&b) {
		return a
	}
	return b
}
