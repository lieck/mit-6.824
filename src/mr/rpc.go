package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// worker发送请求至master
type WorkArgs struct {
	// 请求类型，10表示申请任务，1表示完成map，2表示完成reduce
	AType int
	// map完成任务的文件列表
	MapFiles []string
	// reduce的中间目录
	ReduceFile string
	// 完成任务的编号
	WorkId int
}

// master回应worker
type WorkReply struct {
	// 任务类型，10表示继续等待，1表示map，2表示reduce，3表示所有任务已经完成
	AType int
	// 任务编号
	WorkId int

	// 任务编号
	Id int

	// map输入文件
	MapFile string
	// reduce输入文件
	ReduceFiles []string

	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
