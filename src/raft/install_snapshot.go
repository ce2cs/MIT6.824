package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) prepareInstallSnapshotArgs(args *InstallSnapshotArgs) {
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Snapshot = rf.persister.ReadSnapshot()
}

func (rf *Raft) installSnapshotArgsOnServer(serverID int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[serverID].Call("Raft.InstallSnapshotHandler", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		rf.debugLog(Lab2D, LOG, "installSnapshotArgsOnServer",
			"send installSnapshot failed: server %v does not response", serverID)
		return
	}
	rf.debugLog(Lab2D, LOG, "installSnapshotArgsOnServer",
		"Received installSnapshot response from server %v, reply: %+v", serverID, reply)

	rf.checkAndSetTerm(reply.Term)
	rf.nextIndex[serverID] = Max(rf.lastIncludedIndex+1, rf.nextIndex[serverID])
	rf.matchIndex[serverID] = Max(rf.lastIncludedIndex, rf.matchIndex[serverID])
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.debugLog(Lab2D, LOG, "InstallSnapshotHandler",
		"Received installSnapshot request, args: %+v", args)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.debugLog(Lab2D, LOG, "InstallSnapshotHandler",
			"sender's term %v is outdated, refuse the request", args.Term)
		return
	}

	rf.checkAndSetTerm(args.Term)
	if rf.identity == FOLLOWER {
		rf.resetTimer()
	}

	rf.applySnapshot(&args.Snapshot, args.LastIncludedTerm, args.LastIncludedIndex)
}
