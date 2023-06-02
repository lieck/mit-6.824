package shardkv

import (
	"mit6.824/src/shardmaster"
	"time"
)

// TransferData RPC Server 之间迁移数据, 应用配置信息
func (kv *ShardKV) TransferData(args *TransferDataArgs, reply *TransferDataReply) error {
	kv.mu.Lock()

	// 为已经应用的配置信息, 返回
	if args.ConfigNum <= kv.currConfig.Num {
		kv.mu.Unlock()
		return nil
	}

	// 不为当前需要变更的配置，通知发送方稍后再来
	if kv.currConfig.Num+1 != args.ConfigNum {
		kv.mu.Unlock()
		reply.Err = "WaitUpdateConfig"
		return nil
	}

	// 是否已经被接收
	args.Data.Apply = false
	if !kv.updateConfigDataL(args.Data, false) {
		kv.mu.Unlock()
		return nil
	}

	kv.mu.Unlock()

	// 发起提议
	op := &Op{
		Type:   ChangConfig,
		Config: args.Data,
	}

	DPrintf("[%v:%v]TransferData\top=%v", kv.gid, kv.me, op)

	workDone := make(chan bool)
	go func() {
		replyOp := kv.proposeLog(op)
		DPrintf("[%v:%v]%v", kv.gid, kv.me, replyOp)
		workDone <- true
	}()

	select {
	case <-workDone:
	case <-time.After(1000 * time.Millisecond):
		reply.Err = "Timeout"
	}
	DPrintf("[%v:%v]TransferDataOk\terr=%v", kv.gid, kv.me, reply.Err)
	return nil
}

// 应用 ChangeConfig 到 config data 中
func (kv *ShardKV) updateConfigDataL(cc *ChangeConfig, apply bool) bool {
	if cc.ShardId == -1 {
		return false
	}

	if cc.Kv == nil || cc.RequestFilter == nil {
		panic("c.Kv == nil || cc.RequestFilter == nil")
	}

	if data, ok := kv.shardData[cc.ConfigNum]; ok {
		// 是否已经存在
		for i := range data {
			if data[i].ShardId == cc.ShardId {
				data[i].Apply = apply || data[i].Apply
				return false
			}
		}
		cc.Apply = apply
		kv.shardData[cc.ConfigNum] = append(kv.shardData[cc.ConfigNum], cc)
	} else {
		data := make([]*ChangeConfig, 1)
		data[0] = cc
		cc.Apply = apply
		kv.shardData[cc.ConfigNum] = data
	}
	DPrintf("[%v:%v]updateConfigDataL\tcc=%v\n", kv.gid, kv.me, cc)
	return true
}

// 调用 RPC 发起数据迁移
func (kv *ShardKV) startTransferData(gid int64, args *TransferDataArgs, config *shardmaster.Config) {
	if args.Data.Kv == nil || args.Data.RequestFilter == nil {
		panic("args.Data.Kv == nil || args.Data.RequestFilter == nil")
	}

	DPrintf("[%v:%v]startingTransferData\ttoGid=%v\tconfigId=%v\tdata=%v\n", kv.gid, kv.me, gid, config.Num, args.Data)
	currIdx := 0
	for !kv.isdead() {
		reply := &TransferDataReply{}
		ok := call(config.Groups[gid][currIdx], "ShardKV.TransferData", args, reply)
		if ok {
			if reply.Err == "" {
				return
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		currIdx = (currIdx + 1) % len(config.Groups[gid])
	}

	//for i := range config.Groups[gid] {
	//	for {
	//		reply := &TransferDataReply{}
	//		ok := call(config.Groups[gid][i], "ShardKV.TransferData", args, reply)
	//		if ok {
	//			if reply.Err == "" {
	//				break
	//			} else {
	//				time.Sleep(100 * time.Millisecond)
	//			}
	//		}
	//	}
	//}

	DPrintf("[%v:%v]startingTransferData\ttoGid=%v\tconfigId=%v\tdata=%v\n", kv.gid, kv.me, gid, config.Num, args.Data)
}

// 更新当前配置状态为 configId
func (kv *ShardKV) updateKVConfig(configId int) {
	// 当前存在需要被应用的日志
	kv.mu.Lock()
	if kv.applyConfig != nil {
		kv.mu.Unlock()
		return
	}
	currConfigId := kv.currConfig.Num
	kv.mu.Unlock()

	// 设置为指定的 config
	ok := kv.setConfigInfo(configId)
	if !ok && !kv.setConfigInfo(currConfigId+1) {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.applyConfig == nil {
		return
	}

	// 开始提议配置更新
	op := &Op{
		Type: ChangConfig,
		Config: &ChangeConfig{
			ConfigNum: kv.applyConfig.Num,
			ShardId:   -1,
		},
	}
	go kv.proposeLog(op)
}

// 将 applyConfig 设置为指定 config
func (kv *ShardKV) setConfigInfo(configId int) bool {
	c := kv.sm.Query(configId)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.applyConfig != nil {
		return kv.applyConfig.Num == c.Num
	}
	if kv.currConfig == nil {
		return c.Num == 1
	}
	if kv.currConfig.Num+1 != c.Num {
		return false
	}
	kv.applyConfig = &c
	DPrintf("[%v:%v]setApplyConfig\tconfig=%v\n", kv.gid, kv.me, kv.applyConfig)
	return true
}

// 应用配置更新, 假设需要的分片已经足够，则进入下一个配置
func (kv *ShardKV) applyConfigLogL(seq int, cc *ChangeConfig) {
	if cc.ConfigNum <= kv.currConfig.Num {
		return
	}

	if cc.ConfigNum != kv.currConfig.Num+1 {
		panic("applyConfigLog Err cc.ConfigNum != kv.currConfig.Num+1")
	}

	// 未从 sm 中获取信息，等待
	if kv.applyConfig == nil {
		kv.mu.Unlock()
		kv.setConfigInfo(cc.ConfigNum)
		kv.mu.Lock()
	}
	newConfig := kv.applyConfig

	// 应用更新配置后，要迁移的数据将不会被更新，可以将其发送给其他节点
	for sid := range newConfig.Shards {
		if kv.currConfig.Shards[sid] == kv.gid && newConfig.Shards[sid] != kv.gid {
			if kv.kv[sid] != nil {
				args := &TransferDataArgs{
					ConfigNum: newConfig.Num,
					Data: &ChangeConfig{
						ConfigNum:     newConfig.Num,
						ShardId:       sid,
						RequestFilter: kv.requestFilter[sid],
						Kv:            kv.kv[sid],
					},
				}

				kv.kv[sid] = nil
				kv.requestFilter[sid] = nil
				go kv.startTransferData(newConfig.Shards[sid], args, newConfig)
			}
		}
	}

	// 更新 config data
	cc.Apply = true
	kv.updateConfigDataL(cc, true)
	kv.startApplyConfig = true

	DPrintf("[%v:%v]applyConfig\tseq[%v]\tconfig=%v", kv.gid, kv.me, seq, cc)

	// 判断是否存在数据未被迁移
	dataCnt := 0
	for i := range kv.applyConfig.Shards {
		if kv.currConfig.Shards[i] != kv.gid && kv.currConfig.Shards[i] != 0 && kv.applyConfig.Shards[i] == kv.gid {
			dataCnt++
		}
	}

	if dataCnt != 0 {
		if data, ok := kv.shardData[cc.ConfigNum]; ok {
			for i := range data {
				if data[i].Apply {
					dataCnt--
				}
			}

			if dataCnt != 0 {
				return
			}
		} else {
			return
		}
	}

	// 允许更新配置信息

	if data, ok := kv.shardData[cc.ConfigNum]; ok {
		for i := range data {
			sid := data[i].ShardId
			kv.kv[sid] = data[i].Kv
			kv.requestFilter[sid] = data[i].RequestFilter
		}
	}

	for i := range kv.applyConfig.Shards {
		if kv.kv[i] == nil && kv.applyConfig.Shards[i] == kv.gid {
			if kv.currConfig.Shards[i] == 0 {
				kv.kv[i] = make(map[string]string)
				kv.requestFilter[i] = make(map[int64]int)
			} else {
				panic("kv.kv[i] == nil")
			}
		}
	}

	delete(kv.shardData, cc.ConfigNum)

	kv.currConfig = kv.applyConfig
	kv.applyConfig = nil
	kv.startApplyConfig = false

	DPrintf("[%v:%v]applyConfigOk\tseq[%v]\tconfig=%v", kv.gid, kv.me, seq, kv.currConfig)

	// 是否存在下一个可用配置？
	go kv.updateKVConfig(cc.ConfigNum + 1)
}
