package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

	state State

	// Persistent state on all servers: updated on stable storage before responding to RPCs

	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	snapshot    []byte     // snapshot of server state

	// Volatile state on all servers

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Election and heartbeat timeouts
	electionTimeout  time.Time // when to start elections after no append entry messages
	heartbeatTimeout time.Time // when to next send empty message

	voteCount int // number of votes received in current election

	applyCh chan raftapi.ApplyMsg // channel to send committed log entries to the service

	lastSnapshotIndex int // index of the last snapshot
	// lastSnapshotTerm  int // term of the last snapshot

	condApply *sync.Cond
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

// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.votedFor) != nil || e.Encode(rf.currentTerm) != nil || e.Encode(rf.log) != nil || e.Encode(rf.lastSnapshotIndex) != nil {
		panic("failed to encode state")
	}

	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil || d.Decode(&rf.lastSnapshotIndex) != nil {
		panic("failed to decode raft persistent state")
	}

	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
	rf.snapshot = rf.persister.ReadSnapshot()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Id:%v Term:%v] received snapshot request for index %v, current lastSnapshotIndex=%v",
		rf.me, rf.currentTerm, index, rf.lastSnapshotIndex)

	if index <= rf.lastSnapshotIndex {
		DPrintf("[Id:%v Term:%v] ignoring snapshot request for index %v (already have snapshot up to %v)",
			rf.me, rf.currentTerm, index, rf.lastSnapshotIndex)
		return
	}

	defer rf.persist()

	splitIdx := rf.getVirtualLogIndex(index)
	oldLastSnapshotIndex := rf.lastSnapshotIndex
	rf.lastSnapshotIndex = index

	DPrintf("[Id:%v Term:%v] creating snapshot at index %v , discarding log entries %v to %v",
		rf.me, rf.currentTerm, index, oldLastSnapshotIndex+1, index)

	rf.log = append([]LogEntry{{Term: rf.log[splitIdx].Term}}, rf.log[splitIdx+1:]...)
	rf.snapshot = snapshot

	DPrintf("[Id:%v Term:%v] log size after snapshot: %v entries, snapshot size: %v bytes",
		rf.me, rf.currentTerm, len(rf.log), len(snapshot))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID // Record that we voted for this candidate
		rf.resetElectionTimeout()      // Reset after granting vote
	} else {
		reply.VoteGranted = false
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3B).
	if rf.state != Leader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	DPrintf("[Id:%v Term:%v] appended command to log at index %v", rf.me, rf.currentTerm, rf.getLastLogIndex()+1)
	rf.persist()

	// issue and append entrie RPC
	rf.heartbeatTimeout = time.Now()

	return rf.getLastLogIndex(), rf.currentTerm, true
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
	ms := time.Duration(300 + (rand.Int63() % 300))
	DPrintf("[Id:%v Term:%v] resetting election", rf.me, rf.currentTerm)
	rf.electionTimeout = time.Now().Add(ms * time.Millisecond)
}

// If a follower receives no communication over a period of time
// called the election timeout, then it assumes there is no viable
// leader and begins an election to choose a new leader
func (rf *Raft) timeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	hasTimedOut := time.Now().After(rf.electionTimeout)
	if hasTimedOut {
		rf.becomeCandidate()
		rf.resetElectionTimeout()
		rf.persist()
		DPrintf("[Id:%v Term:%v] starting election", rf.me, rf.currentTerm)
		rf.startElection()
	}
}

// NOTE:
// resetElectionTimeout() fn holds a lock when calling this
func (rf *Raft) startElection() {
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go func(peerId int, request *RequestVoteArgs) {
				var reply RequestVoteReply

				ok := rf.sendRequestVote(peerId, req, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Candidate || rf.currentTerm != req.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						rf.resetElectionTimeout()
						rf.persist()
					}

					if reply.VoteGranted {
						rf.voteCount++
					}

				}
			}(peer, req)
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	quoroum := len(rf.peers)/2 + 1

	winElection := rf.voteCount >= (quoroum)
	if winElection && rf.state == Candidate {
		DPrintf("[Id:%v Term:%v] wins election", rf.me, rf.currentTerm)
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		lastIndex := rf.getLastLogIndex() + 1
		for peer := range rf.peers {
			rf.nextIndex[peer] = lastIndex
			DPrintf("[Id:%v Term:%v] peer %v nextIndex %v matchIndex %v", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		}

		rf.heartbeatTimeout = time.Now() // trigger a heartbeatTimeout inmediately
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("[%d] Server %d recvied snapshot request from %d", rf.currentTerm, rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // old term
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimeout()

	if args.LastSnapshotIndex <= rf.lastSnapshotIndex { // have snapshot
		return
	}

	defer rf.persist()

	rf.lastSnapshotIndex = args.LastSnapshotIndex
	rf.log[0].Term = args.LastSnapshotTerm
	rf.commitIndex = args.LastSnapshotIndex
	rf.lastApplied = args.LastSnapshotIndex
	rf.snapshot = args.Snapshot

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastSnapshotTerm,
		SnapshotIndex: args.LastSnapshotIndex,
	}

	for i := 1; i <= len(rf.log); i++ {
		if rf.getPhysicalLogIndex(i) == args.LastSnapshotIndex && rf.log[i].Term == args.LastSnapshotTerm { // have later logs
			rf.log = append([]LogEntry{{Term: args.LastSnapshotTerm}}, rf.log[i+1:]...) // keep later logs
			rf.applyCh <- msg
			return
		}
	}

	// no later logs, can remove all logs
	rf.log = []LogEntry{{Term: args.LastSnapshotTerm}}
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	rf.nextIndex[server] = args.LastSnapshotIndex + 1
	rf.matchIndex[server] = args.LastSnapshotIndex

	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		if rf.log[rf.getVirtualLogIndex(n)].Term != rf.currentTerm {
			continue
		}

		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.condApply.Signal()
			break
		}
	}
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// not leader or old term
	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimeout()
		return
	}

	if reply.Success { // success append logs
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = max(rf.matchIndex[server], newMatchIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 { // follower's log is shorter than leader's log
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else { // term conflict in reply.ConflictTerm
		newNextIndex := rf.getLastLogIndex()
		for ; newNextIndex > 0; newNextIndex-- {
			if rf.log[rf.getVirtualLogIndex(newNextIndex)].Term == reply.ConflictTerm {
				break
			}
		}

		if newNextIndex > 0 {
			rf.nextIndex[server] = newNextIndex
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}

		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// update commitIndex
	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		if rf.log[rf.getVirtualLogIndex(n)].Term != rf.currentTerm {
			continue
		}
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.condApply.Signal()
			break
		}
	}
}

func (rf *Raft) leaderBroadcast() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastSnapshotIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastSnapshotIndex: rf.lastSnapshotIndex,
				LastSnapshotTerm:  rf.log[0].Term,
				Snapshot:          rf.snapshot,
			}
			go rf.handleInstallSnapshot(i, args)
		} else {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.getVirtualLogIndex(rf.nextIndex[i]-1)].Term,
				Entries:      rf.log[rf.getVirtualLogIndex(rf.nextIndex[i]):],
				LeaderCommit: rf.commitIndex,
			}
			if args.PrevLogIndex == rf.lastSnapshotIndex {
				args.PrevLogTerm = rf.log[0].Term
			}
			go rf.handleAppendEntries(i, args)
		}

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
		rf.heartbeatTimeout = time.Now().Add(heartbeat)
		rf.leaderBroadcast()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimeout()
	lastIndex := rf.getLastLogIndex()

	// follower's log is shorter than leader's log
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		DPrintf("[%d] Server %d recv apeend entires from %d, but snapshot index %d", rf.currentTerm, rf.me,
			args.LeaderID, rf.lastSnapshotIndex)
		reply.ConflictIndex = rf.lastSnapshotIndex + 1 // make leader send entries after snapshot
		return
	}

	// check previous log term
	if prevLogTerm := rf.log[rf.getVirtualLogIndex(args.PrevLogIndex)].Term; prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm

		for i := args.PrevLogIndex - 1; i >= 0; i-- { // find all conflict logs in prevLogTerm
			if rf.log[rf.getVirtualLogIndex(args.PrevLogIndex)].Term != prevLogTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}

		return
	}

	// remove conflict logs
	i := args.PrevLogIndex + 1 // last not conflict log
	j := 0                     // head of entries which is not conflict
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[rf.getVirtualLogIndex(args.PrevLogIndex)].Term != args.Entries[j].Term { // conflict
			break
		}
	}
	rf.log = append(rf.log[:rf.getVirtualLogIndex(i)], args.Entries[j:]...) // remove conflict logs and append new logs

	reply.Success = true // success append logs

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastLogIndex()
		rf.commitIndex = min(args.LeaderCommit, lastIndex) // in paper figure 2 AppendEntries RPC (5)
		rf.condApply.Signal()
	}
}

func (rf *Raft) eventLoop() {
	// Initialize election timer
	rf.mu.Lock()
	rf.resetElectionTimeout()
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Leader:
			rf.heartbeat()
		case Follower:
			rf.timeout()
		case Candidate:
			rf.timeout()
			rf.becomeLeader()
		}

	}
}


func (rf *Raft) applyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.condApply.Wait()
		}

		rf.lastApplied ++
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.getVirtualLogIndex(rf.lastApplied)].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
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
	rf.becomeFollower(0)
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0}) // Dummy element to make this 1-indexed

	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start mainLoop goroutine
	go rf.eventLoop()
	go rf.applyRoutine()

	return rf
}
