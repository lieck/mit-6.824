package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func mapWorker(mapf func(string, string) []KeyValue, fileName string, id int, nReduce int) []string {
	//print("执行Map任务" + strconv.Itoa(id) + "\n")

	fileData, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err.Error())
	}
	data := mapf(fileName, string(fileData))
	ans := make([]string, 0)
	file := make([]*os.File, 0)
	encs := make([]*json.Encoder, 0)

	for i := 0; i < nReduce; i++ {
		name := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		create, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return nil
		}
		file = append(file, create)
		ans = append(ans, name)
		encs = append(encs, json.NewEncoder(create))
	}

	for _, kv := range data {
		idx := ihash(kv.Key) % nReduce
		err := encs[idx].Encode(kv)
		if err != nil {
			panic(err.Error())
		}
	}

	for i := 0; i < nReduce; i++ {
		err := file[i].Close()
		if err != nil {
			panic(err.Error())
		}
	}

	return ans
}

func reduceWorker(reducef func(string, []string) string, files []string, reduceId int, id int) string {

	//print("执行Reduce任务" + strconv.Itoa(reduceId) + "\n")

	fileName := "mr-out-" + strconv.Itoa(reduceId) + "-" + strconv.Itoa(id)

	// 读取分区内所有文件
	mp := make(map[string][]string)
	for _, filePath := range files {
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
		if err != nil {
			panic(err.Error())
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}

	// 计算并写入文件
	f, err := os.Create(fileName)
	if err != nil {
		panic(err.Error())
	}
	for key, val := range mp {
		res := reducef(key, val)
		_, err = io.WriteString(f, key+" "+res+"\n")
		if err != nil {
			panic(err.Error())
		}
	}

	return fileName
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	args := WorkArgs{
		AType: 10,
	}
	reply := WorkReply{}

	for true {
		// 发送请求
		for true {
			ok := call("Coordinator.Worker", &args, &reply)
			if ok {
				if reply.AType == 3 {
					return
				}

				if reply.AType != 10 {
					//print("收到请求：", reply.WorkId, "\n\n")
					break
				}

			} else {
				fmt.Printf("call failed!\n")
			}
			//print("worker重试\n\n")
			time.Sleep(time.Second * 5)
		}

		startTime := time.Now().Unix()
		print(reply.Id, "\n\n")
		// map任务
		if reply.AType == 1 {
			args.AType = 1
			args.WorkId = reply.WorkId
			args.MapFiles = mapWorker(mapf, reply.MapFile, reply.Id, reply.NReduce)
		} else {
			args.AType = 2
			args.WorkId = reply.WorkId - 1

			//print("reduce接收", reply.WorkId, "\n")
			args.ReduceFile = reduceWorker(reducef, reply.ReduceFiles, reply.WorkId-1, reply.Id)
		}

		endTime := time.Now().Unix()
		print("任务完成，执行用时：", endTime-startTime, "s\n\n")
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
