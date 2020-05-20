package mr

import (
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
	TaskTimeoutTime time.Duration = (time.Second * 5)
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
		task := m.tasks[i]

		if task.taskType == args.Type &&
			(task.status == Pending || task.status == Failed ||
				time.Now().Sub(task.executionStart) > TaskTimeoutTime) {
			m.tasks[i].status = Running
			m.tasks[i].executionStart = time.Now()
			reply.Id = m.tasks[i].id
			reply.Files = m.tasks[i].files
			reply.NReduceTasks = m.nReduceTasks
			reply.Msg = fmt.Sprintf("RPC GetTask: task: id %v, type %v", reply.Id, args.Type)
			return nil
		}
	}

	// no task available
	reply.Msg = fmt.Sprintf("RPC GetTask: no task of type %v available.", args.Type)
	return nil
}

// a worker calls this to update a task status
func (m *Master) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *BaseReply) error {
	for i := 0; i < len(m.tasks); i++ {
		if m.tasks[i].id == args.Id && m.tasks[i].taskType == args.Type {
			m.tasks[i].status = args.NewStatus
			reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus: updated task type: %v id: %v to status: %v", args.Type, args.Id, args.NewStatus)
			return nil
		}
		return nil
	}
	reply.Msg = fmt.Sprintf("RPC UpdateTaskStatus: no task of type: %v id: %v exists.", args.Type, args.Id)
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
