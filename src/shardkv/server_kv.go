package shardkv

// 重复请求过滤
func (kv *ShardKV) requestRepetitionValidL(shardId int, client int64, requestSeq int) bool {
	if seq, ok := kv.requestFilter[shardId][client]; ok {
		return seq >= requestSeq
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()

	// 分片过滤
	if kv.applyConfig != nil && kv.applyConfig.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		reply.ConfigNum = kv.applyConfig.Num
		kv.mu.Unlock()
		return nil
	}

	if kv.currConfig.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		reply.ConfigNum = kv.currConfig.Num
		kv.mu.Unlock()
		return nil
	}

	// 请求过滤
	if kv.requestRepetitionValidL(args.ShardId, args.ClientId, args.RequestSeq) {
		if val, ok := kv.kv[args.ShardId][args.Key]; ok {
			reply.Value = val
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	op := &Op{
		Type:       Get,
		ShardId:    args.ShardId,
		Key:        args.Key,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
		Tag:        "",
	}

	op = kv.proposeLog(op)

	if op.Tag != "" {
		reply.Err = op.Tag
	} else {
		reply.Err = OK
		reply.Value = op.GetValue
	}

	return nil
}

// PutAppend RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	kv.mu.Lock()

	// 分片过滤
	if kv.applyConfig != nil && kv.applyConfig.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		reply.ConfigNum = kv.applyConfig.Num
		kv.mu.Unlock()
		return nil
	}

	if kv.currConfig.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		reply.ConfigNum = kv.currConfig.Num
		kv.mu.Unlock()
		return nil
	}

	// 重复请求
	if kv.requestRepetitionValidL(args.ShardId, args.ClientId, args.RequestSeq) {
		kv.mu.Unlock()
		reply.Err = OK
		return nil
	}

	kv.mu.Unlock()

	opType := Put
	if args.Op == "Append" {
		opType = Append
	}

	op := &Op{
		Type:       opType,
		ShardId:    args.ShardId,
		Key:        args.Key,
		Value:      args.Value,
		ClientId:   args.ClientId,
		RequestSeq: args.RequestSeq,
		Tag:        "",
	}

	op = kv.proposeLog(op)

	if op.Tag != "" {
		reply.Err = op.Tag
	} else {
		reply.Err = OK
	}

	return nil
}

func (kv *ShardKV) applyLogL(seq int, op *Op) {
	defer func() {
		ch, chOk := kv.waitCh[seq]
		if chOk {
			ch <- op
			delete(kv.waitCh, seq)
		}
	}()

	if op == nil {
		return
	}

	if op.Type != ChangConfig && op.Type != Empty {
		// 切片不存在
		if kv.startApplyConfig && kv.applyConfig.Shards[op.ShardId] != kv.gid {
			op.Tag = ErrWrongGroup
			return
		}

		if kv.currConfig.Shards[op.ShardId] != kv.gid {
			op.Tag = ErrWrongGroup
			return
		}

		// 请求过滤
		if rf, ok := kv.requestFilter[op.ShardId][op.ClientId]; ok {
			if rf >= op.RequestSeq {
				if op.Type == Get {
					if val, ok := kv.kv[op.ShardId][op.Key]; ok {
						op.GetValue = val
					} else {
						op.Tag = ErrNoKey
					}
				}
				return
			}
		}

		// 更新过滤请求
		kv.requestFilter[op.ShardId][op.ClientId] = op.RequestSeq
	}

	switch op.Type {
	case Get:
		if val, ok := kv.kv[op.ShardId][op.Key]; ok {
			op.GetValue = val
		} else {
			op.Tag = ErrNoKey
		}
	case Put:
		DPrintf("[%v:%v]applyLog\tseq[%v]\tput[%v : %v]\tclient[%v : %v]\n", kv.gid, kv.me, seq, op.Key, op.Value, op.ClientId, op.RequestSeq)
		kv.kv[op.ShardId][op.Key] = op.Value
	case Append:
		DPrintf("[%v:%v]applyLog\tseq[%v]\tappend[%v : %v]\tclient[%v : %v]\n", kv.gid, kv.me, seq, op.Key, op.Value, op.ClientId, op.RequestSeq)
		kv.kv[op.ShardId][op.Key] += op.Value
	case ChangConfig:
		kv.applyConfigLogL(seq, op.Config)
	}
}

// 应用日志
func (kv *ShardKV) applyLog(seq int, op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 当前无法应用, 存放到 log map 中等待
	if seq != kv.lastApplySeq+1 {
		kv.logMap[seq] = op
		return
	}

	ok := true
	for {
		kv.applyLogL(seq, op)

		op, ok = kv.logMap[seq+1]
		if !ok {
			break
		}
		seq++
		delete(kv.logMap, seq)
	}

	kv.lastApplySeq = seq

	go func() {
		kv.px.Done(seq)
		kv.px.Min()
	}()
}
