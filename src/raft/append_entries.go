package raft

import "time"

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
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Got append entries request, args: %+v", args)
	defer rf.debugLog(ALL, LOG, "AppendEntrieHandler",
		"Finished handle append entries request, reply: %+v, current log: %v", reply, rf.log)
	rf.checkAndSetTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.debugLog(ALL, LOG, "AppendEntriesHandler", "Append entries failed: request's term is outdated")
	} else {
		rf.resetTimer()
	}

	if args.PrevLogTerm != rf.log.getLogTermByIndex(args.PrevLogIndex) {
		reply.Success = false
		//TODO specify info
		rf.debugLog(ALL, LOG, "AppendEntriesHandler",
			"Append entries failed: log entry conflict\ncurrent log: %v, prevLogTerm: %v, prevLogIndex %v",
			rf.log, args.PrevLogTerm, args.PrevLogIndex)
		rf.log.Logs = rf.log.removeAfterNInclusive(args.PrevLogIndex)
	}

	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Comparing commitIndex %v and %v",
		args.LeaderCommitIndex, rf.commitIndex)

	if !reply.Success {
		return
	}

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
		if args.PrevLogIndex+1+i <= rf.log.getLastIndex() {
			ok := rf.log.set(args.PrevLogIndex+1+i, args.Entries[i])
			if !ok {
				rf.debugLog(ALL, ERROR, "AppendEntriesHandler",
					"Failed to copy received entries to log at index %v", args.PrevLogIndex+1+i)
			}
		} else {
			rf.log.append(args.Entries[i])
		}
	}

	rf.debugLog(ALL, LOG, "AppendEntriesHandler",
		"Append entries succeed, current log: %v",
		rf.log)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//rf.debugLog(Lab2A, LOG, "sendAppendEntries", "send heartbeat to %v server", server)
	//rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	for !ok && !rf.killed() {
		ok = rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
		time.Sleep(RPC_RESEND_DURATION * time.Millisecond)
	}
	//rf.mu.Lock()
	//rf.debugLog(Lab2A, LOG, "sendAppendEntries", "%v server received heartbeat", server)
	//rf.mu.Unlock()
}
