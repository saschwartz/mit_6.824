package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// task status enum
type TaskStatus int

const (
	Pending   TaskStatus = iota
	Running   TaskStatus = iota
	Succeeded TaskStatus = iota
	Failed    TaskStatus = iota
)

// struct for getting info on a task
type Task struct {
	id     int
	file   string
	status TaskStatus
}

type Master struct {
	nMapTasks    int
	nReduceTasks int
	mapTasks     []Task
	reduceTasks  []Task
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return false
}

// a worker calls this to get a task
func (m *Master) GetTask(args *BaseArgs, reply *GetTaskReply) error {

	// search for a map task
	for i := 0; i < m.nMapTasks; i++ {
		if m.mapTasks[i].status == Pending || m.mapTasks[i].status == Failed {
			reply.Id = m.mapTasks[i].id
			reply.Type = "map"
			reply.File = m.mapTasks[i].file
			return nil
		}
	}

	// if all map tasks taken, look for a reduce task
	reply.Msg = "get task reply message"
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nMapTasks: len(files), nReduceTasks: nReduce}

	// instantiate list of map tasks and reduce tasks
	for i := 0; i < len(files); i++ {
		m.mapTasks = append(m.mapTasks, Task{id: i, status: Pending, file: files[i]})
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, Task{id: i, status: Pending})
	}

	m.server()
	return &m
}
