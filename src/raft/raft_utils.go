package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// DEBUG switch
type DebugPart uint8

const (
	Lab2A = DebugPart(iota)
	Lab2B
	Lab2C
	Lab2D
	ALL
)

func (p DebugPart) String() string {
	parts := []string{"Lab2A", "Lab2B", "Lab2C", "Lab2D", "ALL"}
	i := uint8(p)
	return parts[i]
}

const DEBUG = false
const DEBUG_PART = ALL
const COUNT_RPC = false

const MAX_TERM_DURATION = 600
const MIN_TERM_DURATION = 300
const TICK_DURATION = 50
const RPC_RESEND_DURATION = 100
const ERROR = "Error"
const LOG = "LOG"

func (rf *Raft) resetTimer() {
	rf.debugLog(Lab2A, LOG, "resetTimer", "Reset the timer")
	rf.remainsTime = rand.Intn(MAX_TERM_DURATION-MIN_TERM_DURATION) + MIN_TERM_DURATION
}

func (rf *Raft) becomeFollower() {
	rf.identity = FOLLOWER
	rf.debugLog(ALL, LOG, "becomeFollower", "Becomes follower")
	rf.resetTimer()
}

func (rf *Raft) becomeLeader() {
	rf.identity = LEADER
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.debugLog(ALL, LOG, "becomeLeader", "Becomes leader")
	rf.claimAuthority()
	rf.resetTimer()
}

func (rf *Raft) debugLog(part DebugPart, logType string, funcName string, format string, a ...interface{}) {
	if DEBUG && (part == DEBUG_PART || part == ALL || DEBUG_PART == ALL) {
		prefix := fmt.Sprintf("%v[Mili: %v][%v][ServerID: %v][Term: %v][ServerIdentity:%v][Function:%v] INFO: ",
			part,
			time.Now().UnixMilli(),
			logType,
			rf.me,
			rf.currentTerm,
			rf.identity,
			funcName)
		log.Printf(prefix+format, a...)
	}
}

func (rf *Raft) checkAndSetTerm(term int) {
	if term > rf.currentTerm {
		rf.debugLog(Lab2A, LOG,
			"checkAndSetTerm",
			"Found greater term, update term from %v to %v and become follower",
			rf.currentTerm,
			term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.becomeFollower()
	}
}
