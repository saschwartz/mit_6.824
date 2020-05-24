package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
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
	reducef func(string, []string) string) {

	// try get a map task
	for true {
		args := GetTaskArgs{Type: Map}
		reply := GetTaskReply{}
		err := call("Master.GetTask", &args, &reply)

		// execute the map task
		if err == nil && len(reply.Files) > 0 {
			status := executeMapTask(reply, mapf)
			err = updateTaskStatus(reply.Id, Map, status)

		} else if err != nil {
			fmt.Printf("Worker: Failed to execute map task id: %v\n", reply.Id)
		} else {
			// get a reduce task if no available map tasks
			args := GetTaskArgs{Type: Reduce}
			reply := GetTaskReply{}
			err = call("Master.GetTask", &args, &reply)

			// execute the reduce task
			if err == nil && len(reply.Files) > 0 {
				status := executeReduceTask(reply, reducef)
				err = updateTaskStatus(reply.Id, Reduce, status)
				// sleep a bit if the task failed
				if status == Failed {
					time.Sleep(time.Duration(500) * time.Millisecond)
				}
			} else if err != nil {
				fmt.Printf("Worker: Failed to execute reduce task id: %v\n", reply.Id)
			} else {
				// sleep before polling get task again
				fmt.Println("Worker: No tasks currently available.")
				time.Sleep(time.Duration(500) * time.Millisecond)
			}
		}
	}
}

// helper function to update a task status
func updateTaskStatus(id int, t TaskType, newStatus TaskStatus) error {
	args := UpdateTaskStatusArgs{TaskId: id, Type: t, NewStatus: newStatus}
	reply := BaseReply{}
	err := call("Master.UpdateTaskStatus", &args, &reply)
	// fmt.Printf("Worker: RPC UpdateTaskStatus replied '%v'\n", reply.Msg)
	return err
}

func randAlphaString(length int) string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	var ret string
	for i := 0; i < length; i++ {
		ret += string(chars[rand.Intn(len(chars))])
	}
	return ret
}

// execute a map task, given a list of files
func executeMapTask(reply GetTaskReply, mapf func(string, string) []KeyValue) TaskStatus {
	// fmt.Printf("Worker: Map reply.Files: %v\n", reply.Files)

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
	// create with random string so failed-over still running
	// tasks won't write over each other
	ofileMap := make(map[int]*os.File)
	for i := 0; i < reply.NReduceTasks; i++ {
		ofile, _ := os.Create(fmt.Sprintf("mr-%v-%v-tmp-%v", reply.Id, i, randAlphaString(5)))
		ofileMap[i] = ofile
	}

	// do the map on the content, and sort
	kva := mapf(reply.Files[0], string(content))
	sort.Sort(ByKey(kva))

	// write the mapped key value pairs to the relevant output files
	for i := 0; i < len(kva); i++ {
		reduceTaskId := ihash(kva[i].Key) % reply.NReduceTasks
		enc := json.NewEncoder(ofileMap[reduceTaskId])
		err := enc.Encode(kva[i])
		if err != nil {
			// fmt.Printf("Worker: error encoding %v", kva[i])
			return Failed
		}
	}

	// rename our tmp files to real files so that reduce tasks will know
	// the relevant maps are done
	for i := 0; i < reply.NReduceTasks; i++ {
		oldName := ofileMap[i].Name()
		ofileMap[i].Close()
		os.Rename(oldName, fmt.Sprintf("mr-%v-%v", reply.Id, i))
	}

	fmt.Printf("Worker: Map task id: %v complete\n", reply.Id)
	return Succeeded
}

// excecute a reduce task, given a list of files
func executeReduceTask(reply GetTaskReply, reducef func(string, []string) string) TaskStatus {
	// create our outfile
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", reply.Id))

	// check we have all our files.
	mapReply := AllMapTasksCompleteReply{}
	call("Master.AllMapTasksComplete", BaseArgs{}, &mapReply)
	if !mapReply.AllMapTasksDone {
		fmt.Printf("Worker: Reduce task %v missing some files, failing.\n", reply.Id)
		return Failed
	}

	// read our files and concat all the content
	// fail on open or read errors
	kva := []KeyValue{}
	for i := 0; i < len(reply.Files); i++ {
		file, err := os.Open(reply.Files[i])
		if err != nil {
			log.Printf("Worker: cannot open %v", reply.Files[i])
			return Failed
		}

		dec := json.NewDecoder(file)
		var kv KeyValue
		for dec.Decode(&kv) == nil {
			kva = append(kva, kv)
		}

		file.Close()
	}

	// sort kva by our keys
	// then walk through kva, reducing each time we have a
	// set of identical keys (use sliding window approach)
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		values := []string{kva[i].Value}

		// walk up through identical keys
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			values = append(values, kva[j].Value)
			j++
		}

		// do the reduce and write output
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		// slide up
		i = j
	}

	fmt.Printf("Worker: Reduce task id: %v complete\n", reply.Id)
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
