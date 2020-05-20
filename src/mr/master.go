package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// task status and type enums
type TaskStatus int

const (
	Pending   TaskStatus = iota
	Running   TaskStatus = iota
	Succeeded TaskStatus = iota
	Failed    TaskStatus = iota
)

// how long before assuming a crash has occurred
const (
	TaskTimeoutTime time.Duration = (time.Second * 10)
)

type TaskType int

const (
	Map    TaskType = iota
	Reduce TaskType = iota
)

// struct for getting info on a task
type Task struct {
	id             int
	file           string
	status         TaskStatus
	executionStart time.Time
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
	for i := 0; i < m.nMapTasks; i++ {
		if m.mapTasks[i].status != Succeeded {
			return false
		}
	}
	for i := 0; i < m.nReduceTasks; i++ {
		if m.reduceTasks[i].status != Succeeded {
			return false
		}
	}
	fmt.Println("Job complete.")
	return true
}

// a worker calls this to get a map task
// this will also set the task status to running
func (m *Master) GetMapTask(args *BaseArgs, reply *GetTaskReply) error {

	// search for a map task
	for i := 0; i < m.nMapTasks; i++ {
		task := m.mapTasks[i]
		if task.status == Pending || task.status == Failed ||
			time.Now().Sub(task.executionStart) > TaskTimeoutTime {
			m.mapTasks[i].status = Running
			m.mapTasks[i].executionStart = time.Now()
			reply.Id = m.mapTasks[i].id
			reply.Files = []string{m.mapTasks[i].file}
			reply.Msg = fmt.Sprintf("RPC GetMapTask found a map task: id %v", reply.Id)
			reply.NReduceTasks = m.nReduceTasks
			return nil
		}
	}

	// no task available
	reply.Msg = "RPC GetMapTask says no reduce task available."
	return nil
}

// a worker calls this to get a reduce task
// this will also set the task status to running
func (m *Master) GetReduceTask(args *BaseArgs, reply *GetTaskReply) error {

	// search for a reduce task
	for i := 0; i < m.nReduceTasks; i++ {
		task := m.reduceTasks[i]
		if task.status == Pending || task.status == Failed ||
			time.Now().Sub(task.executionStart) > TaskTimeoutTime {
			m.reduceTasks[i].status = Running
			m.reduceTasks[i].executionStart = time.Now()
			reply.Id = m.reduceTasks[i].id
			reply.Files = make([]string, m.nMapTasks)
			// format is mr-mapId-reduceId
			for j := 0; j < m.nMapTasks; j++ {
				reply.Files[j] = fmt.Sprintf("mr-%d-%d", j, i)
			}
			reply.Msg = fmt.Sprintf("RPC GetReduceTask found a reduce task: id %v", reply.Id)
			return nil
		}
	}

	// no task available
	reply.Msg = "RPC GetReduceTask says no reduce task available."
	return nil
}

// a worker calls this to update a task status
func (m *Master) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *BaseReply) error {
	if args.Type == Map {
		m.mapTasks[args.Id].status = args.NewStatus
		reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus updated map task id: %v to status: %v", args.Id, args.NewStatus)
		return nil
	} else if args.Type == Reduce {
		m.reduceTasks[args.Id].status = args.NewStatus
		reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus updated reduce task %v to status: %v", args.Id, args.NewStatus)
		return nil
	} else {
		return errors.New("Task type must be Map or Reduce.")
	}
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
