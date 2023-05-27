package paxos

// ProposeHandler Acceptor
func (px *Paxos) ProposeHandler(args *ProposeArgs, reply *ProposeReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Commit = &DecidedReply{}
	_ = px.decidedHandlerL(args.Commit, reply.Commit)
	_ = px.proposeHandlerL(args, reply)
	return nil
}

// proposeHandlerL 的处理函数。
func (px *Paxos) proposeHandlerL(args *ProposeArgs, reply *ProposeReply) error {

	if info, ok := px.proposeMap[args.Seq]; ok {
		reply.Rnd = info.rnd

		// 存在值
		if info.accept != nil {
			reply.Accept = info.accept
		}

		// TODO 优化：值已经选定，直接返回，通知 proposer 不进行后续的处理
		//if info.status == Decided {
		//	reply.Status = Decided
		//	return nil
		//}

		// args.rnd > highest_rnd 时更新 rnd，否则拒绝
		if args.Rnd.Gt(&info.rnd) {
			info.rnd = args.Rnd
		} else {
			reply.Reject = true
		}

	} else {
		// 不存在 seq 创建信息
		px.proposeMap[args.Seq] = &ProposeInfo{
			status: Pending,
			accept: nil,
			start:  false,
			rnd:    args.Rnd,
		}
	}

	reply.Status = Pending
	DPrintf("[%v]proposeHandler\tseq[%v]\treply_rnd=%v\treject=%v\tvalue:%v\n", px.me, args.Seq, reply.Rnd, reply.Reject, reply.Accept)
	return nil
}

// AcceptHandler Acceptor
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.Commit = &DecidedReply{}
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
		// 接受大于等于 rnd 的信息
		if args.Rnd.Ge(&info.rnd) {
			info.rnd = args.Rnd
			info.accept = args.Accept
		} else {
			reply.Reject = true
		}
	} else {
		// 不存在创建对应信息
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
func (px *Paxos) DecidedHandler(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.decidedHandlerL(args, reply)
}

func (px *Paxos) decidedHandlerL(args *DecidedArgs, reply *DecidedReply) error {
	for _, data := range args.Data {
		if data.Seq <= px.peerDoneSeq[px.me] {
			continue
		}

		info, ok := px.proposeMap[data.Seq]
		if ok {
			if info.status == Pending {
				info.accept = data
				info.status = Decided
			}
		} else {
			px.proposeMap[data.Seq] = &ProposeInfo{
				rnd:    args.Rnd,
				status: Decided,
				accept: data,
				start:  false,
			}
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
