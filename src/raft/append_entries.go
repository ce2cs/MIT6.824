package raft

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
	rf.debugLog(LOG, "AppendEntriesHandler", "Got append entries request")
	rf.checkAndSetTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.debugLog(LOG, "AppendEntriesHandler", "Append entries failed: request's term is outdated")
	} else if args.PrevLogIndex > len(rf.log)-1 {
		reply.Success = false
		//TODO specify info
		rf.debugLog(LOG, "AppendEntriesHandler", "Append entries failed: log entry conflict")
	} else if len(rf.log) > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		//TODO specify info
		reply.Success = false
		rf.debugLog(LOG, "AppendEntriesHandler", "Append entries failed: log entry conflict")
	}
	reply.Success = true
	if len(args.Entries) == 0 {
		rf.debugLog(LOG, "RequestVoteHandler", "Received heartbeat: reset timer")
		rf.resetTimer()
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.debugLog(LOG, "sendAppendEntries", "send heartbeat to %v server", server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	retriedTimes := 0
	for !ok && !rf.killed() {
		rf.mu.Lock()
		rf.debugLog(LOG, "sendAppendEntries",
			"send append entries %v server failed, retried %v times",
			server,
			retriedTimes)
		rf.mu.Unlock()
		ok = rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
		retriedTimes += 1
	}
	rf.mu.Lock()
	rf.debugLog(LOG, "sendAppendEntries", "%v server received heartbeat", server)
	rf.mu.Unlock()
}
