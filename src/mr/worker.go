package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	workerID := uuid.New()
	for {
		reply, err := RequestTask(workerID)
		log.Printf("%v requested task: %+v\n", workerID, reply)

		if err != nil {
			log.Printf("%v request task failed", workerID)
			time.Sleep(time.Second)
		}

		switch reply.TaskType {
		case MAPPER:
			err := doMapTask(reply.Task, mapf, reply.ReducerNum, workerID)
			if err != nil {
				log.Printf("workder: %v dealing with map task %v failed", workerID, reply.TaskID)
				time.Sleep(time.Second)
			}
		case REDUCER:
			err := doReduceTask(reply.Task, reducef, workerID)
			if err != nil {
				t := reply.Task
				t.TaskStatus = FAILED
				err = ReportTask(workerID, t)
				log.Printf("worker %v dealing with reduce task ID %v failed", workerID, reply.TaskID)
			}
		case WAIT:
			log.Printf("%v wait for 1 second", workerID)
			time.Sleep(time.Second)
		case EXIT:
			log.Printf("job finished, worker exit")
			return
		}
	}

}

func doReduceTask(t Task, reducef func(string, []string) string, workerID uuid.UUID) (err error) {
	reduce_source_pattern := fmt.Sprintf("./reducer-sources/*-%v", t.TaskID)
	match_files, err := filepath.Glob(reduce_source_pattern)
	if err != nil {
		log.Panicf("reduce task id %v resource files not found", t.TaskID)
	}

	kva := make([]KeyValue, 0)

	for _, file_path := range match_files {
		file, err := os.Open(file_path)
		if err != nil {
			log.Panicf("reduce task %v failed: failed to open %v", t.TaskID, file_path)
		}
		json.NewDecoder(file)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", t.TaskID)
	tmpfile, err := ioutil.TempFile("", oname)

	if err != nil {
		log.Panicf("failed to create temp file")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	err = tmpfile.Close()
	if err != nil {
		log.Panicf("failed to close temp file %v", tmpfile.Name())
	}

	err = os.Rename(tmpfile.Name(), oname)
	if err != nil {
		log.Panicf("failed to rename temp file %v", tmpfile.Name())
	}

	t.TaskStatus = SUCCEED
	err = ReportTask(workerID, t)
	if err != nil {
		log.Panicf("failed to report task")
	}
	return
}

func doMapTask(t Task, mapf func(string, string) []KeyValue, reducerNum int, workerID uuid.UUID) (err error) {
	file, err := os.Open(t.Resource)
	defer file.Close()
	fmt.Println(t.Resource)

	if err != nil {
		t.TaskStatus = FAILED
		err = ReportTask(workerID, t)
		log.Panicf("cannot open %v", t.Resource)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		t.TaskStatus = FAILED
		err = ReportTask(workerID, t)
		log.Panicf("cannot read %v", t.Resource)
	}

	kva := mapf(t.Resource, string(content))

	reducerResources := make([]string, reducerNum)

	files := make([]*os.File, reducerNum)
	encoders := make([]*json.Encoder, reducerNum)
	for i := 0; i < reducerNum; i++ {
		reducerResources[i] = fmt.Sprintf("%v-%v", t.TaskID, i)
		files[i], err = ioutil.TempFile("./reducer-sources/", reducerResources[i])
		encoders[i] = json.NewEncoder(files[i])
		if err != nil {
			t.TaskStatus = FAILED
			err = ReportTask(workerID, t)
			log.Println(err)
			log.Panicf("failed to create %v", reducerResources[i])
		}
	}

	for _, kv := range kva {
		reducerID := ihash(kv.Key) % reducerNum
		err = encoders[reducerID].Encode(&kv)
		if err != nil {
			t.TaskStatus = FAILED
			err = ReportTask(workerID, t)
			log.Panicf("encode kv to json failed")
		}
	}

	for i := 0; i < reducerNum; i++ {
		err = files[i].Close()
		if err != nil {
			t.TaskStatus = FAILED
			err = ReportTask(workerID, t)
			log.Panicf("%v failed to close", files[i].Name())
		}
	}

	for i := 0; i < reducerNum; i++ {
		opath := fmt.Sprintf("./reducer-sources/%v-%v", t.TaskID, i)
		err = os.Rename(files[i].Name(), opath)
		if err != nil {
			t.TaskStatus = FAILED
			err = ReportTask(workerID, t)
			log.Panicf("%v failed to rename", files[i].Name())
		}
	}

	t.TaskStatus = SUCCEED
	err = ReportTask(workerID, t)

	if err != nil {
		t.TaskStatus = FAILED
		err = ReportTask(workerID, t)
		log.Panicf("failed to report task")
	}

	return err
}

func RequestTask(workerID uuid.UUID) (reply TaskRequestReply, err error) {
	args := TaskRequest{}
	args.WorkerID = workerID
	reply = TaskRequestReply{}
	ok := call("Coordinator.HandleTaskRequest", &args, &reply)
	if !ok {
		return reply, errors.New("request task failed")
	}
	return reply, nil
}

func ReportTask(workerID uuid.UUID, task Task) (err error) {
	args := TaskReport{}
	reply := BaseReply{}
	args.WorkerID = workerID
	args.Task = task
	ok := call("Coordinator.HandleReportTask", &args, &reply)
	if !ok {
		return errors.New("task report failed")
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
