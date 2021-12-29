package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// constants
type serverIdentity uint8

const (
	FOLLOWER = serverIdentity(iota)
	CANDIDATE
	LEADER
)

func (s serverIdentity) String() string {
	identities := []string{"follower", "candidate", "leader"}
	i := uint8(s)
	return identities[i]
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// required fields in fig 2
	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile states for all servers
	commitIndex int
	lastApplied int

	// volatile states on leaders
	nextIndex  []int
	matchIndex []int

	// self defined fields
	identity    serverIdentity
	remainsTime int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	rf.debugLog(LOG, "getState", "Getting state...")
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.identity == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.identity == LEADER {
			rf.claimAuthority()
			rf.resetTimer()
		} else if rf.remainsTime <= 0 {
			rf.startElection()
			rf.resetTimer()
		}
		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		rf.mu.Lock()
		rf.remainsTime -= 30
		rf.debugLog(LOG, "ticker", "Election time remains %v milliseconds", rf.remainsTime)
		rf.mu.Unlock()
	}
}

func (rf *Raft) claimAuthority() {
	rf.debugLog(LOG, "claimAuthority", "Claiming Authority...")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var appendEntriesArgs AppendEntriesArgs
		var appendEntriesReply AppendEntriesReply

		appendEntriesArgs.Term = rf.currentTerm
		appendEntriesArgs.LeaderID = rf.me
		appendEntriesArgs.PrevLogIndex = rf.getLastLogIndex()
		appendEntriesArgs.PrevLogTerm = rf.getLastLogTerm()

		go func(i int) {
			rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.checkAndSetTerm(appendEntriesReply.Term)
		}(i)
	}
}

func (rf *Raft) startElection() {
	rf.debugLog(LOG, "startElection", "Start Election...")
	rf.currentTerm += 1
	rf.identity = CANDIDATE
	rf.votedFor = rf.me
	go func() {
		isWinner := rf.requestVotes()
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if isWinner {
			if rf.identity == CANDIDATE {
				rf.debugLog(LOG, "startElection", "Become a leader")
				rf.identity = LEADER
			}
		} else {
			rf.becomeFollower()
		}
	}()
}

//func (rf *Raft) requestVotes() bool {
//	rf.debugLog(LOG, "requestVotes", "Request votes...")
//	var mu sync.Mutex
//	cond := sync.NewCond(&mu)
//
//	grantedVotes := 1
//	receivedResults := 1
//	peerNums := len(rf.peers)
//	threshold := peerNums/2 + 1
//	isWinner := false
//
//	for i := 0; i < len(rf.peers) && i != rf.me; i++ {
//		var requestVoteArgs RequestVoteArgs
//		var requestVoteReply RequestVoteReply
//		rf.mu.Lock()
//		requestVoteArgs.CandidateID = rf.me
//		requestVoteArgs.Term = rf.currentTerm
//		requestVoteArgs.LastLogIndex = rf.getLastLogIndex()
//		requestVoteArgs.LastLogTerm = rf.getLastLogTerm()
//		rf.mu.Unlock()
//
//		go func(i int) {
//			rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
//			rf.mu.Lock()
//			defer rf.mu.Unlock()
//			rf.checkAndSetTerm(requestVoteReply.Term)
//			mu.Lock()
//			defer mu.Unlock()
//			if requestVoteReply.VoteGranted {
//				grantedVotes += 1
//				rf.debugLog(LOG,
//					"requestVotes",
//					"Got one vote, now accumulates %v votes",
//					grantedVotes)
//				cond.Broadcast()
//			} else {
//				rf.debugLog(LOG,
//					"requestVotes",
//					"Request vote got rejected")
//				cond.Broadcast()
//			}
//			receivedResults += 1
//		}(i)
//	}
//
//	mu.Lock()
//	for grantedVotes < threshold && receivedResults != peerNums {
//		cond.Wait()
//	}
//
//	if grantedVotes >= threshold {
//		rf.debugLog(LOG,
//			"requestVotes",
//			"Win the election")
//		cond.Broadcast()
//		isWinner = true
//	} else {
//		rf.debugLog(LOG,
//			"requestVotes",
//			"Lose the election")
//		cond.Broadcast()
//		isWinner = false
//	}
//	mu.Unlock()
//	return isWinner
//}

func (rf *Raft) requestVotes() bool {
	rf.debugLog(LOG, "requestVotes", "Request votes...")
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	grantedVotes := 1
	receivedResults := 1
	peerNums := len(rf.peers)
	threshold := peerNums/2 + 1
	isWinner := false

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var requestVoteArgs RequestVoteArgs
		var requestVoteReply RequestVoteReply
		rf.mu.Lock()
		requestVoteArgs.CandidateID = rf.me
		requestVoteArgs.Term = rf.currentTerm
		requestVoteArgs.LastLogIndex = rf.getLastLogIndex()
		requestVoteArgs.LastLogTerm = rf.getLastLogTerm()
		rf.mu.Unlock()

		go func(i int) {
			rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.checkAndSetTerm(requestVoteReply.Term)
			mu.Lock()
			defer mu.Unlock()
			if requestVoteReply.VoteGranted {
				grantedVotes += 1
				rf.debugLog(LOG,
					"requestVotes",
					"Got one vote, now accumulates %v votes",
					grantedVotes)
			} else {
				rf.debugLog(LOG,
					"requestVotes",
					"Request vote got rejected")
			}
			receivedResults += 1
			cond.Broadcast()
		}(i)
	}

	mu.Lock()
	for grantedVotes < threshold && receivedResults != peerNums {
		cond.Wait()
	}

	if grantedVotes >= threshold {
		rf.debugLog(LOG,
			"requestVotes",
			"Win the election")
		cond.Broadcast()
		isWinner = true
	} else {
		rf.debugLog(LOG,
			"requestVotes",
			"Lose the election")
		cond.Broadcast()
		isWinner = false
	}
	mu.Unlock()
	return isWinner
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	peersNum := len(peers)
	rf.me = me
	rf.log = make([]LogEntry, 0)
	rf.currentTerm = 1
	rf.resetTimer()
	rf.votedFor = -1
	rf.identity = FOLLOWER
	rf.nextIndex = make([]int, peersNum)
	rf.matchIndex = make([]int, peersNum)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
