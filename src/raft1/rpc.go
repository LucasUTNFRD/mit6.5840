package raft

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int // the leader's current term number.
	LeaderID     int // The ID of the leader, allowing followers to redirect clients if necessary.
	PrevLogIndex int // The index of the log entry immediately preceding the new entries being sent. This is crucial for the consistency check.
	PrevLogTerm  int // The term number of the prevLogIndex entry, again essential for consistency.
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}
