package raft

type LogEntry struct {
	Command any
	Term    int
}

// To avoid confusion, we will use the term "index" to refer to the index of a log entry,
// and "length" to refer to the number of entries in the log.
func (rf *Raft) getLastLogIndex() int {
	return rf.lastSnapshotIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
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

// Here's a concrete example of how this transformation works:
// **Original log (before any snapshots):**
// ```
// Log array indices: [0,  1,  2,  3,  4,  5]
// Log entries:       [dummy, e1, e2, e3, e4, e5]
// Logical indices:   [0,  1,  2,  3,  4,  5]
// lastSnapshotIndex = 0
// ```

// **After taking a snapshot up to index 3:**
// ```
// Log array indices: [0,  1,  2]
// Log entries:       [dummy, e4, e5]
// Logical indices:   [3,  4,  5]
// lastSnapshotIndex = 3
// ```

// The key here is that after compaction:
// - Array index 1 now contains the entry with logical index 4
// - To convert from logical index to array index: `arrayIndex = logicalIndex - lastSnapshotIndex`
// - To convert from array index to logical index: `logicalIndex = arrayIndex + lastSnapshotIndex`

// we need a helper method to convert between logical log indices (used by the state machine)
// and physical indices in the log array.
// This is necessary because after compaction, the log indices no longer match the array indices
func (rf *Raft) getVirtualLogIndex(originalIndex int) int {
	if originalIndex < rf.lastSnapshotIndex {
		panic("snapshot index")
	}

	return originalIndex - rf.lastSnapshotIndex
}

func (rf *Raft) getPhysicalLogIndex(index int) int {
	return index + rf.lastSnapshotIndex
}
