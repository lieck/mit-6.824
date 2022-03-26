package kvraft

import "sync"

type requestStart struct {
	lastIndex map[int64]int64
	mp        map[int64]map[int64]int
	mu        sync.Mutex
}

func (rs *requestStart) Get(serverId int64, key int64) int {
	if val, ok := rs.lastIndex[serverId]; ok && val >= key {
		return 2
	}

	if _, ok := rs.mp[serverId]; !ok {
		return 0
	}

	if val, ok := rs.mp[serverId][key]; ok {
		return val
	}
	return 0
}

func (rs *requestStart) Set(serverId int64, key int64, val int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if _, ok := rs.mp[serverId]; !ok {
		rs.mp[serverId] = make(map[int64]int)
		rs.lastIndex[serverId] = 0
	}

	rs.mp[serverId][key] = val

	idx := rs.lastIndex[serverId]
	for _, ok := rs.mp[serverId][idx+1]; ok; {
		idx += 1
		delete(rs.mp[serverId], idx)
		_, ok = rs.mp[serverId][idx+1]
	}
	rs.lastIndex[serverId] = idx
}

func MakeRequestStart() *requestStart {
	re := requestStart{}
	re.mp = make(map[int64]map[int64]int)
	re.lastIndex = make(map[int64]int64)
	return &re
}
