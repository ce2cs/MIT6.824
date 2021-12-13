package mr

import (
	"errors"
	"fmt"
	"log"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTaskStatus    map[string]Status
	ReduceTaskStatus map[string]Status
}

type Status int

const UNALLOCATED Status = 0
const ALLOCATED Status = 1
const COMPLECTED Status = 2

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HandleTaskRequest(args *BaseRequest, reply *TaskRequestReply) error {
	for filename, status := range c.MapTaskStatus {
		if status == UNALLOCATED {
			reply.TaskType = "Map"
			reply.FileName = filename
			reply.Error = nil
			c.MapTaskStatus[filename] = ALLOCATED
			return nil
		}
	}
	return errors.New("all files are allocated")
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator.go xxx.so inputfiles...\n")
		os.Exit(1)
	}
	c := Coordinator{}

	// Your code here.
	c.MapTaskStatus = make(map[string]Status, 0)
	for _, filename := range os.Args[2:] {
		c.MapTaskStatus[filename] = UNALLOCATED
	}
	c.server()
	return &c
}
