package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu                   sync.Mutex
	ReducerNum           int
	MapperNum            int
	MapTaskRemainsNum    int
	ReduceTaskRemainsNum int
	TaskChannel          chan Task
	MapTaskStatus        []TaskStatus
	ReduceTaskStatus     []TaskStatus
	WorkerStatus         map[uuid.UUID]WorkerStatus
	WorkerResumeDuration time.Duration
	MaxReplyDelay        time.Duration
}

type TaskStatus int
type WorkerStatus int
type TaskType int

const (
	UNALLOCATED TaskStatus = iota
	ALLOCATED
	FAILED
	SUCCEED
)

const (
	MAPPER TaskType = iota
	REDUCER
	WAIT
	EXIT
)

const (
	BROKEN WorkerStatus = iota
	GOOD
)

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) HandleTaskRequest(args *TaskRequest, reply *TaskRequestReply) (err error) {
	log.Printf("Handle task request: %+v", args)
	c.mu.Lock()
	workerStatus, present := c.WorkerStatus[args.WorkerID]
	c.mu.Unlock()
	if present {
		log.Printf("Worker %v Status: %v", args.WorkerID, c.WorkerStatus[args.WorkerID])
		if workerStatus != GOOD {
			log.Printf("workerStatus: %v", workerStatus)
			reply.Task = Task{}
			reply.Task.TaskType = WAIT
			return
		}
	}

	c.mu.Lock()
	c.WorkerStatus[args.WorkerID] = GOOD
	c.mu.Unlock()
	t, ok := <-c.TaskChannel

	log.Printf("channel output: %+v", t)
	if !ok {
		t.TaskType = EXIT
		reply.Task = t
		return
	}

	reply.Error = nil
	reply.ReducerNum = c.ReducerNum
	reply.Task = t
	if t.TaskType == MAPPER {
		if c.MapTaskStatus[t.TaskID] == SUCCEED {
			log.Printf("map taskID: %v already succeed", t.TaskID)
			t.TaskType = WAIT
			return
		}
	} else if t.TaskType == REDUCER {
		if c.ReduceTaskStatus[t.TaskID] == SUCCEED {
			log.Printf("reduce taskID: %v already succeed", t.TaskID)
			t.TaskType = WAIT
			return
		}
	}
	go c.followTaskStatus(t, args.WorkerID)
	return
}

func (c *Coordinator) followTaskStatus(t Task, workerID uuid.UUID) {
	time.Sleep(c.MaxReplyDelay)
	if t.TaskType == MAPPER {
		if c.MapTaskStatus[t.TaskID] != SUCCEED {
			log.Printf("task id: %v time out", t.TaskID)
			c.TaskChannel <- t
			c.mu.Lock()
			c.WorkerStatus[workerID] = BROKEN
			c.mu.Unlock()
			go c.resumeWorker(workerID)
		}
	} else if t.TaskType == REDUCER {
		if c.ReduceTaskStatus[t.TaskID] != SUCCEED {
			c.TaskChannel <- t
			c.mu.Lock()
			c.WorkerStatus[workerID] = BROKEN
			c.mu.Unlock()
			go c.resumeWorker(workerID)
		}
	}
}

func (c *Coordinator) HandleReportTask(args *TaskReport, reply *BaseReply) (err error) {
	log.Printf("Handle report: %+v", args)
	t := args.Task
	workerID := args.WorkerID

	if t.TaskStatus == SUCCEED {
		if t.TaskType == MAPPER {
			c.mu.Lock()
			c.MapTaskRemainsNum -= 1
			c.MapTaskStatus[t.TaskID] = SUCCEED
			c.mu.Unlock()
			if c.MapTaskRemainsNum == 0 {
				c.fuelReduceTasks()
			}
		} else if t.TaskType == REDUCER {
			c.mu.Lock()
			c.ReduceTaskRemainsNum -= 1
			c.ReduceTaskStatus[t.TaskID] = SUCCEED
			c.mu.Unlock()
			if c.ReduceTaskRemainsNum == 0 {
				close(c.TaskChannel)
			}
		}
	} else {
		t.TaskStatus = ALLOCATED
		c.TaskChannel <- t

		c.mu.Lock()
		c.WorkerStatus[workerID] = BROKEN
		c.mu.Unlock()

		go c.resumeWorker(workerID)
	}
	return
}

func (c *Coordinator) resumeWorker(workerID uuid.UUID) {
	time.Sleep(c.WorkerResumeDuration)
	c.mu.Lock()
	c.WorkerStatus[workerID] = GOOD
	c.mu.Unlock()
}

func (c *Coordinator) fuelReduceTasks() {
	log.Println("all map jobs done, now fueling reduce tasks")
	for i := 0; i < c.ReducerNum; i++ {
		t := Task{}
		t.TaskStatus = UNALLOCATED
		t.TaskID = i
		t.TaskType = REDUCER
		c.TaskChannel <- t
	}
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
	c.mu.Lock()
	if c.ReduceTaskRemainsNum == 0 {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator.go inputfiles...\n")
		os.Exit(1)
	}
	c := Coordinator{}

	if nReduce > len(os.Args[1:]) {
		c.TaskChannel = make(chan Task, nReduce*2)
	} else {
		c.TaskChannel = make(chan Task, len(os.Args[1:])*2)
	}

	fmt.Println(len(os.Args[1:]))
	for i, filename := range os.Args[1:] {
		fmt.Println(i, filename)
		t := Task{}
		t.TaskStatus = UNALLOCATED
		t.TaskType = MAPPER
		t.Resource = filename
		t.TaskID = i
		c.TaskChannel <- t
		c.MapTaskRemainsNum += 1
		c.MapperNum += 1
	}
	c.ReducerNum = nReduce
	c.ReduceTaskRemainsNum = nReduce
	c.WorkerStatus = make(map[uuid.UUID]WorkerStatus)
	c.MapTaskStatus = make([]TaskStatus, c.MapperNum)
	c.ReduceTaskStatus = make([]TaskStatus, c.ReducerNum)
	c.WorkerResumeDuration = 20 * time.Second
	c.MaxReplyDelay = 10 * time.Second
	_, err := os.Stat("reducer-sources")
	if os.IsNotExist(err) {
		err := os.Mkdir("reducer-sources", 0755)
		if err != nil {
			log.Fatalf("created reducer temp directory failed")
		}
	}
	c.server()
	return &c
}
