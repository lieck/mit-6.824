package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap    int

	//map任务
	mapId     int
	statusMap []int
	timeMap   []int64
	mapFiles  []string

	//reduce任务
	statusReduce []int
	timeReduce   []int64
	reduceFiles  [][]string

	status int

	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) monitorFunc() {

	print("===========================================================\n\n")
	print("map运行状态\n")
	for i := 0; i < c.nMap; i++ {
		print(c.statusMap[i], "\t")
	}
	print("\n\n")

	print("===========================================================\n\n")
	time.Sleep(time.Second * 15)
}

func (c *Coordinator) Worker(args *WorkArgs, reply *WorkReply) error {

	c.lock.Lock()

	// 任务完成
	if args.AType == 1 {
		c.statusMap[args.WorkId] = 2
		for i := 0; i < c.nReduce; i++ {
			c.reduceFiles[i] = append(c.reduceFiles[i], args.MapFiles[i])
		}
	} else if args.AType == 2 {
		c.statusReduce[args.WorkId] = 2
	}

	// 申请任务
	currTime := time.Now().Unix()
	if c.status == 0 {
		workId := -1

		for i := 0; i < c.nMap; i++ {
			if c.statusMap[i] == 0 {
				workId = i
				break
			}
		}

		nextStatus := true
		for i := 0; i < c.nMap && workId == -1; i++ {
			if c.statusMap[i] == 1 {
				nextStatus = false
				if c.timeMap[i]-currTime > 30 {
					print("超时重试:", workId, "\n\n")
					workId = i
					break
				}
			}
		}

		if workId != -1 {
			reply.WorkId = workId
			reply.AType = 1
			reply.MapFile = c.mapFiles[workId]
			reply.MapId = c.mapId
			reply.NReduce = c.nReduce

			c.mapId += 1

			c.statusMap[workId] = 1
			c.timeMap[workId] = currTime
		} else {
			if nextStatus {
				c.status = 1
			}
			reply.AType = 10
		}
	}

	if c.status == 1 {
		workId := -1
		for i := 0; i < c.nReduce; i++ {
			if c.statusReduce[i] == 0 {
				workId = i
				break
			}
		}

		nextStatus := true
		for i := 0; i < c.nReduce && workId == -1; i++ {
			if c.statusReduce[i] == 1 {
				nextStatus = false
				print("reduce任务", i, ", 已运行", currTime-c.timeReduce[i], "s\n\n")
				if currTime-c.timeReduce[i] > 5 {
					print("reduce超时重试：", i, "\n\n")
					workId = i
					break
				}
			}
		}

		if workId != -1 {
			reply.AType = 2
			reply.WorkId = workId + 1
			reply.ReduceFiles = c.reduceFiles[workId]

			c.timeReduce[workId] = currTime
			c.statusReduce[workId] = 1
		} else {
			if nextStatus {
				c.status = 2
			}
			reply.AType = 10
		}
	}
	c.lock.Unlock()

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.lock.Lock()
	// Your code here.
	ret = c.status == 2
	c.lock.Unlock()
	if ret {
		print("完成计算\n")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapFiles = files
	c.status = 0
	c.nReduce = nReduce
	c.nMap = len(files)

	c.timeReduce = make([]int64, nReduce)
	c.timeMap = make([]int64, c.nMap)

	c.statusMap = make([]int, c.nMap)
	c.statusReduce = make([]int, nReduce)

	c.reduceFiles = make([][]string, nReduce)

	c.mapId = 0

	c.server()
	return &c
}
