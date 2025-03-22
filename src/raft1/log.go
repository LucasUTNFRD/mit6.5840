package raft

type LogEntry struct {
	Command any
	Term    int
}

// To avoid confusion, we will use the term "index" to refer to the index of a log entry,
// and "length" to refer to the number of entries in the log.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isLogUpToDate(candidateLastLogTerm, candidateLastLogIndex int) bool {
	lastIndex := rf.getLastLogIndex()
	lastTerm := rf.getLastLogTerm()

	if candidateLastLogTerm == lastTerm { // same term, compare log index
		return candidateLastLogIndex >= lastIndex
	}

	return candidateLastLogTerm > lastTerm
}
