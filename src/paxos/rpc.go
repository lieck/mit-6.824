package paxos

import log "github.com/sirupsen/logrus"

// ProposeHandler Acceptor
func (px *Paxos) ProposeHandler(args *ProposeArgs, reply *ProposeReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Commit = &CommitReply{}
	_ = px.decidedHandlerL(args.Commit, reply.Commit)
	_ = px.proposeHandlerL(args, reply)
	return nil
}

// proposeHandlerL 的处理函数。
func (px *Paxos) proposeHandlerL(args *ProposeArgs, reply *ProposeReply) error {
	// seq 为 Done
	if args.Seq <= px.peerDoneSeq[px.me] {
		reply.Status = Forgotten
		reply.Reject = true
		return nil
	}

	if info, ok := px.proposeMap[args.Seq]; ok {
		reply.Rnd = info.rnd

		// 存在值
		if info.accept.Value != nil {
			reply.accept = info.accept
		}

		// 优化：值已经选定，直接返回，通知 proposer 不进行后续的处理
		if info.status == Decided {
			reply.Status = Decided
			return nil
		}
		reply.Status = Pending

		// args.rnd >= highest_rnd 时更新 rnd，否则拒绝
		if args.Rnd.Ge(&info.rnd) {
			info.rnd = args.Rnd
		} else {
			reply.Reject = true
		}

	} else {
		// 不存在 seq 创建信息
		px.proposeMap[args.Seq] = &ProposeInfo{
			status: Pending,
			accept: Data{
				Seq:   args.Seq,
				Rnd:   args.Rnd,
				Value: nil,
			},
			start: false,
		}
		reply.Status = Pending
	}

	log.Debugf("[%v]proposeHandler send:%v\tSeq: %v\trnd:%v\treject:%v", px.me, args.ServerId, args.Seq, reply.Rnd, reply.Reject)
	return nil
}

// AcceptHandler Acceptor
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Commit = &CommitReply{}
	_ = px.decidedHandlerL(args.Commit, reply.Commit)
	_ = px.acceptHandlerL(args, reply)
	return nil
}

// Acceptor 请求处理函数.
func (px *Paxos) acceptHandlerL(args *AcceptArgs, reply *AcceptReply) error {
	if args.Seq <= px.peerDoneSeq[px.me] {
		reply.Reject = true
		return nil
	}

	if info, ok := px.proposeMap[args.Seq]; ok {
		reply.Rnd = info.rnd
		if args.Rnd.Ge(&info.rnd) {
			info.rnd = args.Rnd
			info.accept = args.Accept
		} else {
			reply.Reject = true
		}
	} else {
		px.proposeMap[args.Seq] = &ProposeInfo{
			status: Pending,
			rnd:    args.Rnd,
			accept: args.Accept,
			start:  false,
		}
	}

	return nil
}

// DecidedHandler Acceptor
func (px *Paxos) DecidedHandler(args *CommitArgs, reply *CommitReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.decidedHandlerL(args, reply)
}

func (px *Paxos) decidedHandlerL(args *CommitArgs, reply *CommitReply) error {
	for _, data := range args.Data {
		if data.Seq <= px.peerDoneSeq[px.me] {
			continue
		}

		info, ok := px.proposeMap[data.Seq]
		if ok {
			if info.status == Pending {
				info.accept = *data
				info.status = Decided
				log.Debugf("[%v]Decided Seq: %v\n", px.me, data.Seq)
			}
		} else {
			px.proposeMap[data.Seq] = &ProposeInfo{
				status: Decided,
				accept: *data,
				start:  false,
			}
			log.Debugf("[%v]Decided Seq: %v\n", px.me, data.Seq)
		}
	}

	// 更新状态机
	px.maxSeq = max(args.MaxSeq, px.maxSeq)
	px.peerDoneSeq[args.ServerId] = max(args.DoneSeq, px.peerDoneSeq[args.ServerId])

	reply.MaxSeq = px.maxSeq
	reply.DoneSeq = px.peerDoneSeq[px.me]
	reply.ServerId = px.me
	return nil
}
