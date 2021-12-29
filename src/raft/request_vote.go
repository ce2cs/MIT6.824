package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// required fields in fig 2
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	// required fields in fig 2
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.debugLog(LOG, "RequestVoteHandler", "Got vote request")
	defer rf.mu.Unlock()
	rf.checkAndSetTerm(args.Term)
	reply.Term = rf.currentTerm
	lastEntryTerm := rf.getLastLogTerm()
	lastEntryIndex := rf.getLastLogIndex()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.debugLog(LOG, "RequestVoteHandler", "Refuse vote request: request's term is outdated")
	} else if rf.votedFor >= 0 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
		// election restrictions in chapter 5.4
		rf.debugLog(LOG, "RequestVoteHandler", "Refuse vote request: already voted")
	} else if args.LastLogTerm < lastEntryTerm {
		reply.VoteGranted = false
		rf.debugLog(LOG, "RequestVoteHandler", "Refuse vote request: request's last log term is outdated")
	} else if args.Term == lastEntryTerm && args.LastLogIndex < lastEntryIndex {
		reply.VoteGranted = false
		rf.debugLog(LOG, "RequestVoteHandler", "Refuse vote request: request's last log index is outdated")
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.debugLog(LOG, "RequestVoteHandler", "Granted vote: voted for %v", args.CandidateID)
		if rf.identity == CANDIDATE {
			rf.becomeFollower()
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.debugLog(LOG, "sendRequestVote", "send request vote to server %v", server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	for !ok && !rf.killed() {
		ok = rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	}
	rf.mu.Lock()
	rf.debugLog(LOG, "sendRequestVote",
		"server %v received vote request, vote result is %v",
		server, reply.VoteGranted)
	rf.mu.Unlock()
}
