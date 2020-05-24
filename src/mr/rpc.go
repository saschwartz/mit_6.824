package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// base rpc args / reply
type BaseArgs struct{}
type BaseReply struct {
	Msg string
}

// GetTask rpc needs to return task id and target files
type GetTaskArgs struct {
	BaseArgs
	Type TaskType
}
type GetTaskReply struct {
	BaseReply
	Id           int
	Files        []string
	NReduceTasks int
}

// UpdateTaskStatus rpc needs a task id and task type
type UpdateTaskStatusArgs struct {
	BaseArgs
	TaskId    int
	Type      TaskType
	NewStatus TaskStatus
}

// just need a bool to tell us whether or not all map tasks
// are done
type AllMapTasksCompleteReply struct {
	BaseReply
	AllMapTasksDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
