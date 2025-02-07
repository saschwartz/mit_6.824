package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	files          []string
	status         TaskStatus
	taskType       TaskType
	executionStart time.Time
	mux            sync.Mutex
}

type Master struct {
	nMapTasks    int
	nReduceTasks int
	tasks        []Task
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
	for i := 0; i < len(m.tasks); i++ {
		m.tasks[i].mux.Lock()
		defer m.tasks[i].mux.Unlock()
		if m.tasks[i].status != Succeeded {
			return false
		}
	}
	fmt.Println("Job complete.")
	return true
}

// a worker calls this to get a task
// this will also set the task status to running
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// search for a task
	for i := 0; i < len(m.tasks); i++ {

		m.tasks[i].mux.Lock()
		defer m.tasks[i].mux.Unlock()

		if m.tasks[i].taskType == args.Type &&
			(m.tasks[i].status == Pending || m.tasks[i].status == Failed ||
				(m.tasks[i].status == Running && time.Now().Sub(m.tasks[i].executionStart) > TaskTimeoutTime)) {

			// task started now
			m.tasks[i].status = Running
			m.tasks[i].executionStart = time.Now()

			reply.Id = m.tasks[i].id
			reply.Files = m.tasks[i].files
			reply.NReduceTasks = m.nReduceTasks
			reply.Msg = fmt.Sprintf("RPC GetTask: task: id %v, type: %v", reply.Id, args.Type)
			return nil
		}
	}

	// no task available
	reply.Msg = fmt.Sprintf("RPC GetTask: no task of type: %v available.", args.Type)
	return nil
}

// this lets a reduce task know whether it can start or not,
// returns true if all map tasks done, false otherwise
func (m *Master) AllMapTasksComplete(args *BaseArgs, reply *AllMapTasksCompleteReply) error {
	reply.AllMapTasksDone = true
	for i := 0; i < len(m.tasks); i++ {
		m.tasks[i].mux.Lock()
		defer m.tasks[i].mux.Unlock()
		if m.tasks[i].taskType == Map && m.tasks[i].status != Succeeded {
			reply.AllMapTasksDone = false
			break
		}
	}
	return nil
}

// a worker calls this to update a task status
func (m *Master) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *BaseReply) error {
	for i := 0; i < len(m.tasks); i++ {
		m.tasks[i].mux.Lock()
		defer m.tasks[i].mux.Unlock()
		if m.tasks[i].id == args.TaskId && m.tasks[i].taskType == args.Type {
			m.tasks[i].status = args.NewStatus
			reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus: updated task type: %v id: %v to status: %v", args.Type, args.TaskId, args.NewStatus)
			return nil
		}
	}
	reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus: no task of type: %v id: %v exists.", args.Type, args.TaskId)
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
		m.tasks = append(m.tasks, Task{id: i, taskType: Map, status: Pending, files: []string{files[i]}})
	}
	for i := 0; i < nReduce; i++ {
		task := Task{id: i, taskType: Reduce, status: Pending, files: make([]string, m.nMapTasks)}
		for j := 0; j < m.nMapTasks; j++ {
			task.files[j] = fmt.Sprintf("mr-%d-%d", j, i)
		}
		m.tasks = append(m.tasks, task)
	}

	m.server()
	return &m
}
