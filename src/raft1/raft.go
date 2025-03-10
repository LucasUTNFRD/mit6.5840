package raft

// TODO:
// The goal for Part 3A is for a single leader to be elected,
// for the leader to remain the leader if there are no failures,
// and for a new leader to take over if the old leader fails or
// if packets to/from the old leader are lost.

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// Persisten state
	currentTerm uint
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex uint
	lastApplied uint

	// Leader state
	nextIndex  []int
	matchIndex []int

	// When to start elections after no append entry messages
	electionTimeout time.Time
	// When to next send empty message
	heartbeatTimeout time.Time

	voteCount uint
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	heartbeat = time.Duration(100) * time.Millisecond
)

type LogEntry struct {
	Command any
	Term    uint
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return int(rf.currentTerm), rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint
	CandidateID  int
	LastLogIndex uint
	LastLogTerm  uint
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint // the leader's current term number.
	LeaderID     uint // The ID of the leader, allowing followers to redirect clients if necessary.
	PrevLogIndex uint // The index of the log entry immediately preceding the new entries being sent. This is crucial for the consistency check.
	PrevLogTerm  uint // The term number of the prevLogIndex entry, again essential for consistency.
	Entries      []LogEntry
	LeaderCommit uint
}

type AppendEntriesReply struct {
	Term    uint // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimeout()
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID // Record that we voted for this candidate
		rf.resetElectionTimeout()      // Reset after granting vote
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isLogUpToDate(candidateLastLogTerm, candidateLastLogIndex uint) bool {
	// Get the last log entry in the receiver's log
	lastLogIndex := uint(len(rf.log) - 1)
	lastLogTerm := rf.log[len(rf.log)-1].Term

	// Compare terms first
	if candidateLastLogTerm > lastLogTerm {
		return true
	}
	// If terms are equal, compare log lengths
	if candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex {
		return true
	}
	// Otherwise, the candidate's log is not up-to-date
	return false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Call this under lock contention
// NOTE:
// eventLoop hold a lock when calling this
// timeout holds a lock when calling this
// requestVote holds a lock when calling this
func (rf *Raft) resetElectionTimeout() {
	// Randomize election timeout between 250 and 500 ms
	ms := time.Duration(250+rand.Intn(250)) * time.Millisecond
	DPrintf("[Id:%v Term:%v]", rf.me, rf.currentTerm)
	rf.electionTimeout = time.Now().Add(ms)
}

// If a follower receives no communication over a period of time
// called the election timeout, then it assumes there is no viable
// leader and begins an election to choose a new leader
func (rf *Raft) timeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	hasTimedOut := time.Now().After(rf.electionTimeout)
	if hasTimedOut {
		rf.currentTerm++
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.resetElectionTimeout()
		rf.voteCount = 1 // we count ourselves
		DPrintf("[Id:%v Term:%v] starting election", rf.me, rf.currentTerm)
		rf.startElection()
	}
}

// NOTE:
// resetElectionTimeout() fn holds a lock when calling this

func (rf *Raft) startElection() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peerId int) {
				rf.mu.Lock()

				lastLogIndex := uint(len(rf.log) - 1)
				lastLogTerm := rf.log[len(rf.log)-1].Term

				req := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.mu.Unlock()

				var reply RequestVoteReply

				DPrintf("[Id:%v Term:%v] sending RequestVote to %v", rf.me, rf.currentTerm, peerId)
				ok := rf.sendRequestVote(peerId, req, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// it should check that
					// rf.currentTerm hasn't changed since the decision to become a candidate.
					if rf.state != Candidate || rf.currentTerm != req.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.resetElectionTimeout()
						rf.votedFor = -1
					}

					if reply.VoteGranted {
						rf.voteCount++
					}

				}
			}(peer)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	quoroum := len(rf.peers)/2 + 1

	winElection := rf.voteCount >= uint(quoroum)
	if winElection && rf.state == Candidate {
		DPrintf("[Id:%v Term:%v] wins election", rf.me, rf.currentTerm)
		rf.state = Leader
		rf.heartbeatTimeout = time.Now()

		// TODO:
		// initalize next index
		// initialize matchIndex
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	hasTimedOut := time.Now().After(rf.heartbeatTimeout)
	if hasTimedOut {
		DPrintf("[Id:%v Term:%v] sending heartbeat", rf.me, rf.currentTerm)
		rf.heartbeatTimeout = time.Now().Add(heartbeat)
		for peer := range rf.peers {
			if peer != rf.me {
				go func(peerID int) {
					rf.mu.Lock()
					req := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderID: uint(rf.me),
					}
					rf.mu.Unlock()
					var reply AppendEntriesReply

					ok := rf.sendAppendEntries(peerID, req, &reply)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.resetElectionTimeout()
						}

						DPrintf("[Id:%v Term:%v] received heartbeat reply", rf.me, rf.currentTerm)
					}
				}(peer)
			}
		}
	}
}

// RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimeout()
	}

	// Reset election timeout because we received a message
	rf.resetElectionTimeout()

	reply.Term = rf.currentTerm
}

func (rf *Raft) eventLoop() {
	// Initialize election timer
	rf.mu.Lock()
	rf.resetElectionTimeout()
	rf.mu.Unlock()

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		// TODO:
		// Re-implemet this usign select statements to do this asynchronously
		switch state {
		case Leader:
			rf.heartbeat()
			// s.advanceCommitIndex()
		case Follower:
			rf.timeout()
			// s.advanceCommitIndex()
		case Candidate:
			rf.timeout()
			rf.becomeLeader()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0}) // Dummy element to make this 1-indexed
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start mainLoop goroutine
	go rf.eventLoop()

	return rf
}
