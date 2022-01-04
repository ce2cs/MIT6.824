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
	return logs.Logs[startIdx-logs.StartIdx : endIndex-logs.StartIdx]
}

func (logs *CommandLogs) sliceToEnd(startIdx int) []LogEntry {
	arrayIdx := logs.getArrayIdx(startIdx)
	if arrayIdx >= len(logs.Logs) || arrayIdx < 0 {
		return make([]LogEntry, 0)
	}
	return logs.Logs[arrayIdx:]
}

func (logs *CommandLogs) removeAfterNInclusive(n int) []LogEntry {
	arrayIdx := logs.getArrayIdx(n)
	if arrayIdx < 0 {
		return make([]LogEntry, 0)
	}
	if arrayIdx >= len(logs.Logs) {
		return logs.Logs
	}
	return logs.Logs[:n]
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

func NewCommandLogs() *CommandLogs {
	logs := CommandLogs{}
	logs.StartIdx = 1
	logs.Logs = make([]LogEntry, 0)
	return &logs
}
