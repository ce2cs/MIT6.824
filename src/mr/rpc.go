package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"github.com/google/uuid"
	"os"
)
import "strconv"

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

// Add your RPC definitions here.

type BaseRequest struct {
}

type TaskRequest struct {
	BaseRequest
	WorkerID uuid.UUID
}

type TaskReport struct {
	BaseRequest
	WorkerID uuid.UUID
	Task
}

type BaseReply struct {
	Error error
}

type TaskRequestReply struct {
	BaseReply
	Task
	ReducerNum int
}

type Task struct {
	TaskID int
	TaskStatus
	TaskType
	Resource string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
