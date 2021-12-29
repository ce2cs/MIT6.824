package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const MAX_TERM_DURATION = 600
const MIN_TERM_DURATION = 300
const ERROR = "Error"
const LOG = "LOG"

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	var lastLogTerm int
	if lastLogIndex < 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	return lastLogTerm
}

func (rf *Raft) resetTimer() {
	rf.debugLog(LOG, "resetTimer", "Reset the timer")
	rf.remainsTime = rand.Intn(MAX_TERM_DURATION-MIN_TERM_DURATION) + MIN_TERM_DURATION
}

func (rf *Raft) becomeFollower() {
	rf.identity = FOLLOWER
	rf.debugLog(LOG, "becomeFollower", "Becomes follower")
	rf.resetTimer()
}

func (rf *Raft) debugLog(logType string, funcName string, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%v: [%v][ServerID: %v][Term: %v][ServerIdentity:%v][Function:%v] INFO: ",
			time.Now(),
			logType,
			rf.me,
			rf.currentTerm,
			rf.identity,
			funcName)
		DPrintf(prefix+format, a...)
	}
}

func (rf *Raft) checkAndSetTerm(term int) {
	if term > rf.currentTerm {
		rf.debugLog(LOG,
			"checkAndSetTerm",
			"Found greater term, update term from %v to %v and become follower",
			rf.currentTerm,
			term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.becomeFollower()
	}
}
