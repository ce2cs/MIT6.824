package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug {
//		log.Printf(format, a...)
//	}
//	return
//}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key         string
	Value       string
	OpType      string
	SequenceNum int
	ClientID    int64
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32 // set by Kill()
	killedChan chan interface{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandResponse      map[int]chan QueryResponse
	clientLatestResponse map[int64]QueryResponse
	storage              KVDatabase
}

type QueryResponse struct {
	value       string
	err         Err
	timeOut     bool
	sequenceNum int
}

func (kv *KVServer) checkLatestOp(clientID int64, sequenceNum int) (QueryResponse, bool) {
	latestRes := kv.clientLatestResponse[clientID]
	if sequenceNum == latestRes.sequenceNum {
		return kv.clientLatestResponse[clientID], true
	} else {
		return QueryResponse{}, false
	}
}

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	kv.debugLog(LOG, "GetHandler", "Got get request, args: %+v", args)
	latestRes, executed := kv.checkLatestOp(args.ClientID, args.SequenceNum)
	if executed {
		reply.Value = latestRes.value
		reply.Err = latestRes.err
		kv.debugLog(LOG, "GetHandler", "Request already handled, reply: %+v", reply)
		kv.mu.Unlock()
		return
	}
	command := Op{}
	command.Key = args.Key
	command.OpType = GET
	command.SequenceNum = args.SequenceNum
	command.ClientID = args.ClientID
	commandIndex, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debugLog(LOG, "GetHandler", "Current server is not leader, reply: %+v", reply)
		kv.mu.Unlock()
		return
	} else {
		kv.debugLog(LOG, "GetHandler", "Accepted command: %+v, commandIndex: %v, trying to get response from raft", command, commandIndex)
		ch := make(chan QueryResponse)
		kv.commandResponse[commandIndex] = ch
		go kv.monitorTimeout(commandIndex)
		kv.mu.Unlock()
		select {
		case res := <-ch:
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if res.timeOut {
				reply.Err = ErrWrongLeader
				delete(kv.commandResponse, commandIndex)
				kv.debugLog(LOG, "GetHandler", "process command: %+v failed because of timeout, reply: %+v", command, reply)
				return
			}
			reply.Value = res.value
			reply.Err = res.err
			kv.clientLatestResponse[args.ClientID] = res
			delete(kv.commandResponse, commandIndex)
			kv.debugLog(LOG, "GetHandler", "process command: %+v succeed, current storage: %+v, reply: %+v", command, kv.storage, reply)
			return
		case <-kv.killedChan:
			return
		}
	}
}

func (kv *KVServer) monitorTimeout(commandIndex int) {
	time.Sleep(10 * time.Second)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.commandResponse[commandIndex]
	if !ok {
		return
	} else {
		res := QueryResponse{}
		res.timeOut = true
		kv.debugLog(LOG, "monitorTimeout", "Trying to add %+v to commandIndex: %v channel", res, commandIndex)
		ch <- res
		kv.debugLog(LOG, "monitorTimeout", "Added %+v to commandIndex: %v channel", res, commandIndex)
	}
}

func (kv *KVServer) fetchApplyMsg() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			kv.debugLog(LOG, "fetchApplyMsg", "Got apply message: %+v", applyMsg)
			if applyMsg.CommandValid {
				command := applyMsg.Command.(Op)
				res := QueryResponse{}
				res.sequenceNum = command.SequenceNum
				switch command.OpType {
				case GET:
					res.value, res.err = kv.storage.get(command.Key)
				case PUT:
					res.err = kv.storage.put(command.Key, command.Value)
				case APPEND:
					res.err = kv.storage.append(command.Key, command.Value)
				}

				ch, prs := kv.commandResponse[applyMsg.CommandIndex]
				kv.mu.Unlock()
				if prs {
					kv.debugLog(LOG, "fetchApplyMsg", "Trying to add %+v to commandIndex: %v channel", res, applyMsg.CommandIndex)
					ch <- res
					kv.debugLog(LOG, "fetchApplyMsg", "Added %+v to commandIndex: %v channel", res, applyMsg.CommandIndex)
				}
			}
		case <-kv.killedChan:
			return
		}
	}
}

// Put&Append RPC handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.debugLog(LOG, "PutAppendHandler", "Got putAppend request, args: %+v", args)
	latestRes, executed := kv.checkLatestOp(args.ClientID, args.SequenceNum)
	if executed {
		reply.Err = latestRes.err
		kv.debugLog(LOG, "PutAppendHandler", "Request already handled, reply: %+v", reply)
		kv.mu.Unlock()
		return
	}
	command := Op{}
	command.Key = args.Key
	command.Value = args.Value
	command.OpType = args.Op
	command.SequenceNum = args.SequenceNum
	command.ClientID = args.ClientID
	commandIndex, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.debugLog(LOG, "PutAppendHandler", "Current server is not leader, reply: %+v", reply)
		kv.mu.Unlock()
		return
	} else {
		ch := make(chan QueryResponse)
		kv.commandResponse[commandIndex] = ch
		go kv.monitorTimeout(commandIndex)
		kv.mu.Unlock()
		select {
		case res := <-ch:
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if res.timeOut {
				reply.Err = ErrWrongLeader
				delete(kv.commandResponse, commandIndex)
				kv.debugLog(LOG, "PutAppendHandler", "process command: %+v failed because of timeout, reply: %+v", command, reply)
				return
			}
			reply.Err = res.err
			kv.clientLatestResponse[args.ClientID] = res
			delete(kv.commandResponse, commandIndex)
			kv.debugLog(LOG, "PutAppendHandler", "process command: %+v succeed, current storage: %+v, reply: %+v", command, kv.storage, reply)
			return
		case <-kv.killedChan:
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	var signal interface{}
	kv.killedChan <- signal
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.killedChan = make(chan interface{})
	kv.storage = *NewKVDB()
	kv.commandResponse = make(map[int]chan QueryResponse)
	kv.clientLatestResponse = make(map[int64]QueryResponse)
	go kv.fetchApplyMsg()
	return kv
}

func (kv *KVServer) debugLog(logType string, funcName string, format string, info ...interface{}) {
	if !DEBUG {
		return
	}
	prefix := fmt.Sprintf("[server: %v][%v][%v]INFO: ",
		kv.me,
		logType,
		funcName)
	log.Printf(prefix+format, info...)
}
