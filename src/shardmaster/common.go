package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- replica group gid is joining, give it some shards.
// Leave(gid) -- replica group gid is retiring, hand off all its shards.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// Please don't change this file.
//

const NShards = 10

type Config struct {
	Num    int                // config number
	Shards [NShards]int64     // shard -> gid
	Groups map[int64][]string // gid -> servers[]
}

func (c *Config) Copy() Config {
	newConfig := Config{}
	newConfig.Num = c.Num
	newConfig.Shards = c.Shards
	newConfig.Groups = make(map[int64][]string)
	for gid, servers := range c.Groups {
		s := make([]string, len(servers))
		copy(s, servers)
		newConfig.Groups[gid] = s
	}
	return newConfig
}

// Move 将 gid 管理的 cnt 个分片移动给 to
func (c *Config) Move(gid int64, to int64, cnt int) {
	if cnt == 0 {
		return
	}

	for i := range c.Shards {
		if c.Shards[i] == gid {
			c.Shards[i] = to
			cnt--

			if cnt == 0 {
				return
			}
		}
	}
}

func (c *Config) Eq(nc *Config) bool {
	if c.Num != nc.Num {
		return false
	}

	for i := range nc.Shards {
		if nc.Shards[i] != c.Shards[i] {
			return false
		}
	}

	if len(c.Groups) != len(nc.Groups) {
		return false
	}

	for i := range c.Groups {
		cServer := c.Groups[i]
		ncServer := nc.Groups[i]
		if len(cServer) != len(ncServer) {
			return false
		}
		for j := range cServer {
			if cServer[j] != ncServer[j] {
				return false
			}
		}
	}
	return true
}

func (c *Config) Balance() {
	groupCnt := len(c.Groups)

	if groupCnt == 0 {
		for i := range c.Shards {
			c.Shards[i] = 0
		}
		return
	}

	minCnt := len(c.Shards) / groupCnt
	maxCnt := (len(c.Shards) + groupCnt - 1) / groupCnt

	shardCnt := make(map[int64]int)
	for i := range c.Groups {
		shardCnt[i] = 0
	}
	for _, i := range c.Shards {
		shardCnt[i]++
	}

	for {
		// 找 0 或组分片最大的成员 maxGid
		var srcGid int64 = -1
		srcGidCnt := 0
		for g, cnt := range shardCnt {
			if g == 0 && cnt != 0 {
				srcGid = 0
				srcGidCnt = cnt
				break
			}
			if cnt > srcGidCnt {
				srcGidCnt = cnt
				srcGid = g
			}
		}

		// 找到组分片最小的成员 minGid, 除了 0
		var dstGid int64 = -1
		dstGidCnt := 1000
		for g, cnt := range shardCnt {
			if g == 0 {
				continue
			}
			if cnt < dstGidCnt {
				dstGidCnt = cnt
				dstGid = g
			}
		}

		// 已经平衡, 退出程序
		if srcGid != 0 && dstGidCnt >= minCnt && srcGidCnt <= maxCnt {
			return
		}

		// 将 srcGid 的分片移动给 dstGid
		moveCnt := 0
		if srcGid == 0 {
			moveCnt = maxCnt - dstGidCnt
			if moveCnt > srcGidCnt {
				moveCnt = srcGidCnt
			}
		} else {
			moveCnt = srcGidCnt - minCnt
		}

		shardCnt[srcGid] -= moveCnt
		shardCnt[dstGid] += moveCnt
		c.Move(srcGid, dstGid, moveCnt)
	}
}

type JoinArgs struct {
	GID     int64    // unique replica group ID
	Servers []string // group server ports
}

type JoinReply struct {
}

type LeaveArgs struct {
	GID int64
}

type LeaveReply struct {
}

type MoveArgs struct {
	Shard int
	GID   int64
}

type MoveReply struct {
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	Config Config
}
