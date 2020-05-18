package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {

	// try get a map task
	reply := GetTaskReply{}
	err := call("Master.GetMapTask", BaseArgs{}, &reply)
	fmt.Println(reply.Msg)
	if err == nil && len(reply.Files) > 0 {

		// TODO: execute the map task
		status := executeMapTask(reply.Files)
		args := UpdateTaskStatusArgs{Id: reply.Id, Type: Map, NewStatus: status}
		reply := BaseReply{}
		err = call("Master.UpdateTaskStatus", &args, &reply)
		fmt.Println(reply.Msg)

	} else if err != nil {
		return errors.New("Failed to execute map task")
	} else {
		// get a reduce task if no available map tasks
		reply := GetTaskReply{}
		err = call("Master.GetReduceTask", BaseArgs{}, &reply)
		fmt.Println(reply.Msg)
		if err == nil && len(reply.Files) > 0 {

			// TODO: execute the reduce task
			status := executeReduceTask(reply.Files)
			args := UpdateTaskStatusArgs{Id: reply.Id, Type: Reduce, NewStatus: status}
			reply := BaseReply{}
			err = call("Master.UpdateTaskStatus", &args, &reply)
			fmt.Println(reply.Msg)

		} else if err != nil {
			return errors.New("Failed to execute reduce task")
		} else {
			fmt.Println("All map and reduce tasks complete!")
		}
	}

	// successful procedure
	return nil
}

// execute a map task, given a list of files
func executeMapTask(files []string) TaskStatus {
	// TODO - implement
	fmt.Printf("Map reply.Files: %v\n", files)
	return Succeeded
}

// excecute a reduce task, given a list of files
func executeReduceTask(files []string) TaskStatus {
	// TODO - implement
	fmt.Printf("Reduce reply.Files: %v\n", files)
	return Succeeded
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
	}

	return err
}
