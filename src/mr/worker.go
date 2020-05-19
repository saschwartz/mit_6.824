package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// parameters for how long a reduce task should wait
// for all the necessary files to become available
const (
	ReduceWaitTime     float64       = 5.0
	ReducePollInterval time.Duration = 1.0
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key - from mrsequential.go
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		status := executeMapTask(reply, mapf)
		err = updateTaskStatus(reply.Id, Map, status)

	} else if err != nil {
		return errors.New("Failed to execute map task")
	} else {
		// get a reduce task if no available map tasks
		reply := GetTaskReply{}
		err = call("Master.GetReduceTask", BaseArgs{}, &reply)
		fmt.Println(reply.Msg)
		if err == nil && len(reply.Files) > 0 {

			// TODO: execute the reduce task
			status := executeReduceTask(reply, reducef)
			err = updateTaskStatus(reply.Id, Reduce, status)

		} else if err != nil {
			return errors.New("Failed to execute reduce task")
		} else {
			fmt.Println("All map and reduce tasks complete!")
		}
	}

	// successful procedure
	return nil
}

// helper function to update a task status
func updateTaskStatus(id int, t TaskType, newStatus TaskStatus) error {
	args := UpdateTaskStatusArgs{Id: id, Type: t, NewStatus: newStatus}
	reply := BaseReply{}
	err := call("Master.UpdateTaskStatus", &args, &reply)
	fmt.Println(reply.Msg)
	return err
}

// execute a map task, given a list of files
func executeMapTask(reply GetTaskReply, mapf func(string, string) []KeyValue) TaskStatus {
	fmt.Printf("Map reply.Files: %v\n", reply.Files)

	// try open file
	file, err := os.Open(reply.Files[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Files[0])
		return Failed
	}

	// try read file, then close
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Files[0])
		return Failed
	}
	file.Close()

	// create our intermediate outfiles (initially as tmp)
	ofileMap := make(map[int]*os.File)
	for i := 0; i < reply.NReduceTasks; i++ {
		ofile, _ := os.Create(fmt.Sprintf("mr-%v-%v-tmp", reply.Id, i))
		ofileMap[i] = ofile
	}

	// do the map on the content, and sort
	kva := mapf(reply.Files[0], string(content))
	sort.Sort(ByKey(kva))

	// write the mapped key value pairs to the relevant output files
	for i := 0; i < len(kva); i++ {
		reduceTaskId := ihash(kva[i].Key) % reply.NReduceTasks
		fmt.Fprintf(ofileMap[reduceTaskId], "%v %v\n", kva[i].Key, kva[i].Value)
	}

	// rename our tmp files to real files so that reduce tasks will know
	// the relevant maps are done
	for i := 0; i < reply.NReduceTasks; i++ {
		ofileMap[i].Close()
		os.Rename(fmt.Sprintf("mr-%v-%v-tmp", reply.Id, i), fmt.Sprintf("mr-%v-%v", reply.Id, i))
	}

	return Succeeded
}

// excecute a reduce task, given a list of files
func executeReduceTask(reply GetTaskReply, reducef func(string, []string) string) TaskStatus {

	// check we have all our files.
	// do this for ReduceWaitTime, polling at ReducePollInterval
	// if still missing files at the end of waiting, fail the task
	start := time.Now()
	missingFiles := false
	for time.Since(start).Seconds() < ReduceWaitTime {
		for i := 0; i < len(reply.Files); i++ {
			if _, err := os.Stat(reply.Files[i]); os.IsNotExist(err) {
				missingFiles = true
				break
			}
		}
		if !missingFiles {
			break
		}
		fmt.Printf("Reduce task %v missing files. Waiting.", reply.Id)
		time.Sleep(ReducePollInterval * time.Second)
	}
	if missingFiles {
		fmt.Printf("Reduce task %v did not get files in time, failing.", reply.Id)
		return Failed
	}

	// read our files and concat all the content
	// fail on open or read errors
	for i := 0; i < len(reply.Files); i++ {
		file, err := os.Open(reply.Files[i])
		if err != nil {
			log.Fatalf("cannot open %v", reply.Files[i])
			return Failed
		}

		// try read file, then close
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Files[i])
			return Failed
		}
		file.Close()

		fmt.Println(content)
	}

	// TODO - implement
	fmt.Printf("Reduce reply.Files: %v\n", reply.Files)
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
