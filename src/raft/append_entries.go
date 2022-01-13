package raft

import (
	"time"
)

// RPC args and reply definitions
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int

	//debug info
	Idx int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// fast backup info
	FoundConflict bool
	XTerm         int
	XIdx          int
	XLastIndex    int
}

func (rf *Raft) prepareAppendEntriesArgs(appendEntriesArgs *AppendEntriesArgs, serverID int) {
	appendEntriesArgs.Term = rf.currentTerm
	appendEntriesArgs.LeaderID = rf.me
	appendEntriesArgs.PrevLogIndex = rf.nextIndex[serverID] - 1
	//TODO: may add some code to judge whether prevLogIndex is out of our log
	appendEntriesArgs.PrevLogTerm = Max(rf.log.getLogTermByIndex(appendEntriesArgs.PrevLogIndex), rf.lastIncludedTerm)
	appendEntriesArgs.LeaderCommitIndex = rf.commitIndex
	//appendEntriesArgs.Entries = make([]LogEntry, 0)
	appendedLogs := rf.log.sliceToEnd(rf.nextIndex[serverID])
	appendEntriesArgs.Entries = make([]LogEntry, len(appendedLogs))
	copy(appendEntriesArgs.Entries, appendedLogs)
	//for _, logEntry := range rf.log.sliceToEnd(rf.nextIndex[serverID]) {
	//	appendEntriesArgs.Entries = append(appendEntriesArgs.Entries, logEntry)
	//}
	// debug info
	appendEntriesArgs.Idx = rf.appendEntriesRPCIdx
	rf.appendEntriesRPCIdx += 1
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = true

	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Got append entries request, args: %+v", args)
	rf.checkAndSetTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		//reply.ConflictTerm = -1
		rf.debugLog(ALL, LOG, "AppendEntriesHandler", "Append entries failed: request's term is outdated")
	} else {
		rf.resetTimer()
	}

	//TODO: may add some code to judge whether prevLogIndex is out of our log
	var termNeedMatch int
	if args.PrevLogIndex == rf.lastIncludedIndex {
		termNeedMatch = rf.lastIncludedTerm
	} else {
		termNeedMatch = rf.log.getLogTermByIndex(args.PrevLogIndex)
	}
	if reply.Success && args.PrevLogTerm != termNeedMatch {
		reply.FoundConflict = true
		reply.Success = false

		reply.XTerm = termNeedMatch
		reply.XIdx = rf.log.getFirstIndexByTerm(reply.XTerm)
		reply.XLastIndex = rf.log.getLastIndex()

		//TODO specify info
		rf.debugLog(ALL, LOG, "AppendEntriesHandler",
			"Append entries failed: log entry conflict\ncurrent log: %v, prevLogTerm: %v, prevLogIndex %v",
			rf.log, args.PrevLogTerm, args.PrevLogIndex)
		rf.log.removeAfterNInclusive(args.PrevLogIndex)
	}

	if !reply.Success {
		rf.debugLog(ALL, LOG, "AppendEntriesHandler",
			"Finished handle append entries request, reply: %+v, current log: %v", reply, rf.log)
		return
	}

	//rf.debugLog(ALL, LOG, "AppendEntriesHandler",
	//	"Comparing commitIndex %v and %v",
	//	args.LeaderCommitIndex, rf.commitIndex)

	if args.LeaderCommitIndex > rf.commitIndex {
		origin := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommitIndex, rf.log.getLastIndex()+len(args.Entries))
		rf.debugLog(ALL, LOG, "AppendEntriesHandler",
			"Update commitIndex from %v to %v",
			origin, rf.commitIndex)
	}

	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Append entries matched, start appending %v",
		args.Entries)

	for i := 0; i < len(args.Entries); i++ {
		updateIdx := args.PrevLogIndex + 1 + i
		if updateIdx <= rf.log.getLastIndex() {
			if rf.log.getLogTermByIndex(updateIdx) == args.Entries[i].Term {
				continue
			}
			rf.log.deleteFromIdx(updateIdx)
		}
		rf.log.append(args.Entries[i])
	}

	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Finished handle append entries request, reply: %+v, current log: %v", reply, rf.log)
}

func (rf *Raft) sendAppendEntriesRetry(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
	retriedTimes int) bool {
	if COUNT_RPC {
		rf.mu.Lock()
		rf.rpcCount += 1
		rf.debugLog(ALL, LOG,
			"sendAppendEntries", "send append entries to server %v, rpc count increase, now %v",
			server, rf.rpcCount)
		rf.mu.Unlock()
	}
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	if COUNT_RPC {
		rf.mu.Lock()
		rf.rpcCount -= 1
		rf.debugLog(ALL, LOG,
			"sendAppendEntries", "received append entries reply from server %v, rpc count decrease, now: %v",
			server, rf.rpcCount)
		rf.mu.Unlock()
	}
	for !ok && !rf.killed() && retriedTimes > 0 {
		time.Sleep(RPC_RESEND_DURATION * time.Millisecond)
		if COUNT_RPC {
			rf.mu.Lock()
			rf.rpcCount += 1
			rf.debugLog(ALL, LOG,
				"sendAppendEntries", "trying to resend append entries to server %v, rpc count increase, now %v",
				server, rf.rpcCount)
			rf.mu.Unlock()
		}
		ok = rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
		retriedTimes -= 1
		if COUNT_RPC {
			rf.mu.Lock()
			rf.rpcCount -= 1
			rf.debugLog(ALL, LOG,
				"sendAppendEntries", "received send append entries reply from server %v, rpc count decrease, now %v",
				server, rf.rpcCount)
			rf.mu.Unlock()
		}
	}
	return ok
}

func (rf *Raft) appendEntriesOnServer(serverID int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntriesRetry(serverID, args, reply, 0)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !ok {
		rf.debugLog(ALL, LOG, "AppendEntriesOnServer", "server %v does not response", serverID)
		return
	}

	if rf.currentTerm > reply.Term {
		rf.debugLog(ALL, LOG, "sendAppendEntries", "Outdated rpc, term changed from %v to %v",
			reply.Term, rf.currentTerm)
		return
	} else if rf.currentTerm < reply.Term {
		rf.checkAndSetTerm(reply.Term)
		return
	}

	if !reply.Success && reply.FoundConflict {

		fastBackIdx := rf.getFastBackIdx(reply)

		rf.nextIndex[serverID] = Min(args.PrevLogIndex,
			rf.nextIndex[serverID],
			fastBackIdx)
		rf.debugLog(Lab2B, LOG, "appendEntriesOnServer",
			"Trying to replicate logs on server %v failed, nextIndex back to %v",
			serverID, rf.nextIndex[serverID])

		if rf.nextIndex[serverID] < rf.logStartIdx {
			rf.debugLog(Lab2B, ERROR, "appendEntriesOnServer",
				"Trying to replicate logs on server %v failed, nextIndex becomes negative, fastBackIdx: %v, "+
					"args.PrevLogIndex: %v, reply: %+v",
				serverID, args.PrevLogIndex, reply)
			//rf.log.getLastIndexByTerm(reply.XTerm)
			//rf.nextIndex[serverID] = 1
			//return
		}
	} else {
		addedLastIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverID] = Max(addedLastIndex+1, rf.nextIndex[serverID])
		rf.matchIndex[serverID] = Max(addedLastIndex, rf.matchIndex[serverID])
		rf.debugLog(Lab2B, LOG, "appendEntriesOnServer",
			"Succeed to send appendEntries to server %v, nextIndex updated to: %v, matchIndex:%v",
			serverID, rf.nextIndex[serverID], rf.matchIndex[serverID])
	}
}

func (rf *Raft) getFastBackIdx(reply *AppendEntriesReply) int {
	if reply.XTerm == -1 {
		return reply.XLastIndex + 1
	} else if !rf.log.hasTerm(reply.XTerm) {
		return reply.XIdx
	} else {
		return rf.log.getLastIndexByTerm(reply.XTerm)
	}
}
