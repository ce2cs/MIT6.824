package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu            sync.Mutex
	currentLeader int
	sequenceNum   int
	me            int64
}

func nrand() int64 {
	// used for command identification
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//ck.mu.Lock()
	//currentLeader := ck.currentLeader
	//ck.mu.Unlock()
	serverID := ck.currentLeader

	for {
		getArgs := GetArgs{}
		getArgs.Key = key
		getArgs.ClientID = ck.me
		getArgs.SequenceNum = ck.sequenceNum
		getReply := GetReply{}
		ck.debugLog(LOG, "Get", "Sending get RPC to server %v, args: %+v", serverID, getArgs)
		ok := ck.servers[serverID].Call("KVServer.Get", &getArgs, &getReply)

		if !ok || getReply.Err == ErrWrongLeader {
			if !ok {
				ck.debugLog(LOG, "Get", "Failed to get RPC response from server %v", serverID)
			}
			if getReply.Err == ErrWrongLeader {
				ck.debugLog(LOG, "Get", "Failed to send get request to server %v: wrong leader", serverID)
			}

			serverID += 1
			if serverID == len(ck.servers) {
				serverID = 0
			}

		} else {
			ck.currentLeader = serverID
			ck.sequenceNum += 1
			switch getReply.Err {
			case OK:
				ck.debugLog(LOG, "Get", "Succeed to send get request to server %v, response value: %v", serverID, getReply.Value)
				return getReply.Value
			case ErrNoKey:
				ck.debugLog(LOG, "Get", "Succeed to send get request to server %v, response value: nil", serverID)
				return ""
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serverID := ck.currentLeader

	for {
		putAppendArgs := PutAppendArgs{}
		putAppendArgs.Key = key
		putAppendArgs.Value = value
		putAppendArgs.Op = op
		putAppendArgs.ClientID = ck.me
		putAppendArgs.SequenceNum = ck.sequenceNum

		putAppendReply := PutAppendReply{}
		ck.debugLog(LOG, "PutAppend", "Sending putAppend RPC to server %v, args: %+v", serverID, putAppendArgs)
		ok := ck.servers[serverID].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)

		if !ok || putAppendReply.Err == ErrWrongLeader {
			if !ok {
				ck.debugLog(LOG, "PutAppend", "Failed to get RPC response from server %v", serverID)
			}
			if putAppendReply.Err == ErrWrongLeader {
				ck.debugLog(LOG, "PutAppend", "Failed to send putAppend request to server %v: wrong leader", serverID)
			}
			serverID += 1
			if serverID == len(ck.servers) {
				serverID = 0
			}
		} else {
			ck.currentLeader = serverID
			ck.debugLog(LOG, "PutAppend", "Succeed to process PutAppend request on server %v", serverID)
			ck.sequenceNum += 1
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) debugLog(logType string, funcName string, format string, info ...interface{}) {
	if !DEBUG {
		return
	}
	prefix := fmt.Sprintf("[Client: %v][%v][%v][currentLeader: %v][sequenceNum: %v]INFO: ",
		ck.me,
		logType,
		funcName,
		ck.currentLeader,
		ck.sequenceNum)
	log.Printf(prefix+format, info...)
}
