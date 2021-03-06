package raft

type LogEntry struct {
	Command interface{}
	Term    int
}

type CommandLogs struct {
	Logs     []LogEntry
	StartIdx int
}

func (logs *CommandLogs) append(entry LogEntry) {
	logs.Logs = append(logs.Logs, entry)
}

func (logs *CommandLogs) get(index int) (LogEntry, bool) {
	arrayIdx := logs.getArrayIdx(index)
	if arrayIdx < 0 || arrayIdx >= len(logs.Logs) {
		return LogEntry{}, false
	}
	return logs.Logs[arrayIdx], true
}

func (logs *CommandLogs) getArrayIdx(index int) int {
	return index - logs.StartIdx
}

func (logs *CommandLogs) slice(startIdx int, endIndex int) []LogEntry {
	startArrayIdx := logs.getArrayIdx(startIdx)
	endArrayIdx := logs.getArrayIdx(endIndex)
	return logs.Logs[startArrayIdx:endArrayIdx]
}

func (logs *CommandLogs) sliceToEnd(startIdx int) []LogEntry {
	arrayIdx := logs.getArrayIdx(startIdx)
	if arrayIdx >= len(logs.Logs) || arrayIdx < 0 {
		return make([]LogEntry, 0)
	}
	return logs.Logs[arrayIdx:]
}

func (logs *CommandLogs) deleteFromIdx(startIdx int) {
	if startIdx > logs.getLastIndex() {
		return
	}
	arrayIdx := logs.getArrayIdx(startIdx)
	logs.Logs = logs.Logs[:arrayIdx]
}

func (logs *CommandLogs) deleteUntilIndex(untilIdx int) {
	if untilIdx > logs.getLastIndex() {
		logs.Logs = make([]LogEntry, 0)
		return
	}
	arrayIdx := logs.getArrayIdx(untilIdx)
	logs.Logs = logs.Logs[arrayIdx:]
}

func (logs *CommandLogs) removeAfterNInclusive(n int) {
	arrayIdx := logs.getArrayIdx(n)
	if arrayIdx < 0 {
		logs.Logs = make([]LogEntry, 0)
	}
	if arrayIdx >= len(logs.Logs) {
		return
	}
	logs.Logs = logs.Logs[:arrayIdx]
}

func (logs *CommandLogs) getFirstIndexByTerm(term int) int {
	res := 0

	for i := logs.StartIdx; i <= logs.getLastIndex(); i++ {
		entry, _ := logs.get(i)
		if entry.Term == term {
			res = i
			break
		}
	}

	return res
}

func (logs *CommandLogs) getLastIndexByTerm(term int) int {
	res := 0

	for i := logs.getLastIndex(); i >= logs.getFirstIndex(); i-- {
		entry, _ := logs.get(i)
		if entry.Term == term {
			res = i
			break
		}
	}

	return res
}

func (logs *CommandLogs) getLastIndex() int {
	return len(logs.Logs) - 1 + logs.StartIdx
}

func (logs *CommandLogs) getFirstIndex() int {
	return logs.StartIdx
}

func (logs *CommandLogs) getLength() int {
	return len(logs.Logs)
}

func (logs *CommandLogs) getLogTermByIndex(index int) int {
	logEntry, ok := logs.get(index)
	if !ok {
		return -1
	}
	return logEntry.Term
}

func (logs *CommandLogs) set(index int, entry LogEntry) bool {
	arrayIdx := logs.getArrayIdx(index)
	if arrayIdx < 0 || arrayIdx >= len(logs.Logs) {
		return false
	}
	logs.Logs[arrayIdx] = entry
	return true
}

func (logs *CommandLogs) hasTerm(term int) bool {
	for _, entry := range logs.Logs {
		if entry.Term == term {
			return true
		}
	}
	return false
}

func NewCommandLogs() *CommandLogs {
	logs := CommandLogs{}
	logs.StartIdx = 1
	logs.Logs = make([]LogEntry, 0)
	return &logs
}
