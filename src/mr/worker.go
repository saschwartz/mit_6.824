package mr

import (
	"encoding/json"
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
	ReduceWaitTime     float64       = 3.0
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
	reducef func(string, []string) string) {

	// try get a map task
	for true {
		args := GetTaskArgs{Type: Map}
		reply := GetTaskReply{}
		err := call("Master.GetTask", &args, &reply)
		fmt.Println(reply.Msg)
		if err == nil && len(reply.Files) > 0 {

			// TODO: execute the map task
			status := executeMapTask(reply, mapf)
			err = updateTaskStatus(reply.Id, Map, status)

		} else if err != nil {
			log.Fatal("Failed to execute map task")
		} else {
			// get a reduce task if no available map tasks
			args := GetTaskArgs{Type: Reduce}
			reply := GetTaskReply{}
			err = call("Master.GetTask", &args, &reply)
			fmt.Println(reply.Msg)
			if err == nil && len(reply.Files) > 0 {

				// TODO: execute the reduce task
				status := executeReduceTask(reply, reducef)
				err = updateTaskStatus(reply.Id, Reduce, status)

			} else if err != nil {
				log.Fatal("Failed to execute reduce task")
			} else {
				fmt.Println("No tasks currently available.")
				return
			}
		}
	}
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
		enc := json.NewEncoder(ofileMap[reduceTaskId])
		err := enc.Encode(kva[i])
		if err != nil {
			log.Fatalf("error encoding %v", kva[i])
			return Failed
		}
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
	// create our outfile
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", reply.Id))

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
		fmt.Printf("Reduce task %v missing files. Waiting.\n", reply.Id)
		time.Sleep(ReducePollInterval * time.Second)
	}
	if missingFiles {
		fmt.Printf("Reduce task %v did not get files in time, failing.\n", reply.Id)
		return Failed
	}

	// read our files and concat all the content
	// fail on open or read errors
	kva := []KeyValue{}
	for i := 0; i < len(reply.Files); i++ {
		file, err := os.Open(reply.Files[i])
		if err != nil {
			log.Fatalf("cannot open %v", reply.Files[i])
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

	fmt.Printf("Reduce task %v complete\n", reply.Id)
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
