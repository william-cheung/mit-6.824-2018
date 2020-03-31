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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "sync/atomic"

import "bytes"
import "labgob"

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

const (
	HeartbeatInterval  = 100 // in milliseconds
	MinElectionTimeout = 500 // in milliseconds
	MaxElectionTimeout = 800 // in milliseconds
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int         // Term in which the entry is created
	Command interface{} // Command for state machine
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int // Latest term this peer has seen
	votedFor    int // CandidateId that received vote in current term
	state       int // This peer's current state or role

	heartbeatCh chan int // Channel for heartbeat notification
	heartbeat   int32    // Indicates a heartbeat is received but not checked

	log []LogEntry // Log entries. Start from index 1

	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	applyCh chan ApplyMsg // Channel to send apply message to state machine

	// For each server, index of the next log entry to send to that server
	nextIndex []int

	// For each server, index of highest log entry known to be replicated
	// on server
	matchIndex []int

	replicateCond *sync.Cond // To notify log needs to be replicated
	logCommitted  *sync.Cond // To notify a new log entry has been committed

	isKilled bool // Whether this peer has been killed
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) transitionTo(term int, state int) {
	if term < rf.currentTerm {
		DPrintf("Warning: try to update currentTerm of %d from %d to %d",
			rf.me, rf.currentTerm, term)
		return
	}
	if term == rf.currentTerm && state == rf.state {
		DPrintf("Warning: try to update currentTerm and state <%d, %d> "+
			"of %d, but none is changed",
			rf.me, rf.currentTerm, rf.state)
		return
	}

	DPrintf("state of %d changed: <%d, %d> -> <%d, %d>",
		rf.me, rf.currentTerm, rf.state, term, state)
	rf.currentTerm = term
	rf.state = state

	switch state {
	case Follower:
		rf.votedFor = -1
		rf.onHeartbeatReceived(-1) // reset election timer
	case Candidate:
		rf.votedFor = rf.me
	case Leader:
		for peer, _ := range rf.peers {
			rf.nextIndex[peer] = len(rf.log)
			if peer == rf.me {
				rf.matchIndex[peer] = len(rf.log) - 1
			} else {
				rf.matchIndex[peer] = 0
			}
		}
	}
}

func (rf *Raft) onHeartbeatReceived(leaderId int) {
	DPrintf("heartbeat received: %d --> %d", leaderId, rf.me)

	atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)

	select {
	case rf.heartbeatCh <- 1:
	default:
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	DPrintf("persist state of %d: currentTerm %d, votedFor %d, log %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Failed to read persisted state")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
	}

	DPrintf("read persist state of %d: currentTerm %d, votedFor %d, log %v",
		rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so followers can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // Leader's commitIndex
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term int // currentTerm for leader to update itself

	// true if follwer contained entry matching
	// PrevLogIndex and PrevLogTerm
	Success bool

	// Fields for fast back up of nextIndex for a follower
	XTerm  int // term in the conflicting log entry (if any)
	XIndex int // index of first log entry with XTerm (if any)
	XLen   int // log length of the follower
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.onHeartbeatReceived(args.LeaderId)

	persist := false

	defer func() {
		if persist {
			rf.persist()
		}
	}()

	// assert args.Term >= rf.currentTerm

	if args.Term > rf.currentTerm || rf.state == Candidate {
		// higher term discovered, or this peer is a candidate
		// receiving a heartbeat message from a leader, convert
		// to follower
		persist = args.Term > rf.currentTerm
		rf.transitionTo(args.Term, Follower)
	}

	// assert args.Term == rf.currentTerm

	// check if there is an inconsistency at index args.PrevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.XLen = len(rf.log) - 1
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		reply.XLen = len(rf.log) - 1
		for index := args.PrevLogIndex - 1; index > 0; index-- {
			if rf.log[index].Term == reply.XTerm {
				reply.XIndex = index
			} else {
				break
			}
		}
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	for i, entry := range args.Entries {
		j := args.PrevLogIndex + i + 1
		// if an existing entry conflicts with a incoming one,
		// delete the existing entry and all that follow it
		if j < len(rf.log) && entry.Term != rf.log[j].Term {
			rf.log = rf.log[:j]
			persist = true
		}
		// append any new entries not already in the log
		if j >= len(rf.log) {
			rf.log = append(rf.log, entry)
			persist = true
		}
	}

	if len(args.Entries) > 0 {
		DPrintf("log of %d after append: %v", rf.me, rf.log)
	}

	// update commitIndex of the peer
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.PrevLogIndex + len(args.Entries)
		if newCommit > args.LeaderCommit {
			newCommit = args.LeaderCommit
		}
		// value of commitIndex must be monotonically increasing
		if newCommit > rf.commitIndex {
			DPrintf("commitIndex of %d changed: %d -> %d",
				rf.me, rf.commitIndex, newCommit)
			rf.commitIndex = newCommit
			rf.logCommitted.Broadcast()
		}
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("AppendEntries( %d --> %d ): args %+v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("AppendEntries( %d <-- %d ): %+v reply %+v", rf.me, server, ok, reply)
	return ok
}

//
// RequestVote RPC arguments structure
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting votes
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate receives vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	persist := false

	if args.Term > rf.currentTerm {
		// discovers server with higher term, transitions to follower
		rf.transitionTo(args.Term, Follower)
		// sets votedFor after transition (pls notice the "after")
		rf.votedFor = args.CandidateId
		persist = true
	}

	// assert args.Term == rf.currentTerm

	var voteGranted bool
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if candidate's log is at least as up-to-date as
		// this peer's log. if it is, grant vote
		myLastTerm := rf.log[len(rf.log)-1].Term
		if args.LastLogTerm != myLastTerm {
			voteGranted = args.LastLogTerm > myLastTerm
		} else {
			voteGranted = args.LastLogIndex >= len(rf.log)-1
		}
		if voteGranted {
			rf.votedFor = args.CandidateId
			persist = true
		}
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, voteGranted

	if persist {
		rf.persist()
	}
}

//
// Send a RequestVote RPC to a server.
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("RequestVote( %d --> %d ): args %+v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("RequestVote( %d <-- %d ): %+v reply %+v", rf.me, server, ok, reply)
	return ok
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
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.replicateCond.Broadcast()
		rf.persist()
	}
	rf.mu.Unlock()

	DPrintf("Start command %v @%d -> %d, %d, %v",
		command, rf.me, index, term, isLeader)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Kill peer %d", rf.me)

	rf.replicateCond.Broadcast()
	rf.logCommitted.Broadcast()

	rf.isKilled = true
}

func (rf *Raft) IsKilled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isKilled

}

func (rf *Raft) getElectionTimeout() time.Duration {
	t := rand.Intn(MaxElectionTimeout-MinElectionTimeout) +
		MinElectionTimeout
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) runAsFollower() {
	for {
		if rf.IsKilled() {
			return
		}

		electionTimeout := rf.getElectionTimeout()
		t := time.NewTimer(electionTimeout)

		// rf.heartbeatCh has priority over t.C
		// here is a workaround
		select {
		case <-rf.heartbeatCh:
			atomic.CompareAndSwapInt32(
				&rf.heartbeat, 1, 0)
			t.Stop()
			// heartbeat received, continue to reset
			// the timer
			continue
		case <-t.C:
			if atomic.CompareAndSwapInt32(
				&rf.heartbeat, 1, 0) {
				continue
			}
		}
		break
	}

	// times out, transitions to candidate
	rf.mu.Lock()
	rf.transitionTo(rf.currentTerm+1, Candidate)
	rf.persist()
	rf.mu.Unlock()
}

// Returns true if vote is granted
func (rf *Raft) requestVote(
	server int, term, lastLogIndex, lastLogTerm int) bool {

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var reply RequestVoteReply

	if server == rf.me {
		return true
	}

	if rf.sendRequestVote(server, &args, &reply) {
		if reply.VoteGranted {
			return true
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// new term discovered, transition to
			// follower
			rf.transitionTo(reply.Term, Follower)
			rf.persist()
		}
		rf.mu.Unlock()
	}
	return false
}

func (rf *Raft) collectVotes() <-chan bool {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	votesCh := make(chan bool, len(rf.peers))
	for peer, _ := range rf.peers {
		go func(peer int) {
			votesCh <- rf.requestVote(
				peer,
				currentTerm,
				lastLogIndex,
				lastLogTerm)
		}(peer)
	}
	return votesCh

}

func (rf *Raft) runForLeader() {
	// start election timer
	timeoutCh := time.After(rf.getElectionTimeout())

	// collect votes from followers
	votesCh := rf.collectVotes()

	rf.mu.Lock()
	// check if the peer has already been killed or discovers
	// new term and becomes a follower
	if rf.isKilled || rf.state == Follower {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	countVotes := 0
	nextState := Candidate

	for {
		select {
		case vote := <-votesCh:
			if vote {
				countVotes += 1
			}
			// if votes received from majority of peers,
			// become leader
			if countVotes > len(rf.peers)/2 {
				nextState = Leader
			} else {
				continue
			}
		case <-rf.heartbeatCh:
			// the candidate discovers a leader,
			// tansition to follower
			atomic.CompareAndSwapInt32(
				&rf.heartbeat, 1, 0)
			nextState = Follower
		case <-timeoutCh:
			// if the candidate has discovered a leader
			if atomic.CompareAndSwapInt32(
				&rf.heartbeat, 1, 0) {
				nextState = Follower
			} else { // election timer expires
				// start a new election
				nextState = Candidate
			}
		}
		break
	}

	// state transition
	rf.mu.Lock()
	if rf.state == Candidate {
		if nextState == Candidate {
			rf.transitionTo(rf.currentTerm+1, nextState)
			rf.persist()
		} else {
			rf.transitionTo(rf.currentTerm, nextState)
		}
	}
	rf.mu.Unlock()
}

// Returns false if the leader loses its authority
func (rf *Raft) appendEntries(server int, count int) bool {
	rf.mu.Lock()

	if rf.isKilled || rf.state != Leader {
		rf.mu.Unlock()
		return rf.state == Leader
	}

	term := rf.currentTerm

	lastLogIndex := len(rf.log) - 1
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	nEntries := lastLogIndex - prevLogIndex // always >= 0
	if count >= 0 && nEntries > count {
		nEntries = count
	}
	entries := rf.log[nextIndex : nextIndex+nEntries]

	commitIndex := rf.commitIndex

	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	var reply AppendEntriesReply

	if !rf.sendAppendEntries(server, &args, &reply) {
		return true
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm {
		return false
	}

	if reply.Success {
		nextIndex := prevLogIndex + 1 + nEntries
		if nextIndex > rf.nextIndex[server] {
			rf.nextIndex[server] = nextIndex
			rf.matchIndex[server] = nextIndex - 1
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.transitionTo(reply.Term, Follower)
			rf.persist()
			return false
		}

		// AppendEntries fails because of log
		// inconsistency

		rf.replicateCond.Broadcast()

		if reply.XLen < prevLogIndex {
			rf.nextIndex[server] = reply.XLen + 1
			return true
		}

		for i := prevLogIndex - 1; i >= 0; i-- {
			term := rf.log[i].Term
			if term == reply.XTerm {
				rf.nextIndex[server] = i + 1
				return true
			} else if term < reply.XTerm {
				break
			}
		}
		rf.nextIndex[server] = reply.XIndex
	}
	return true
}

// Replicates log entries to followers
func (rf *Raft) replicateLog(server int) {
	for {
		rf.mu.Lock()
		for {
			if rf.isKilled || rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[server] >= len(rf.log) {
				rf.replicateCond.Wait()
			} else {
				break
			}
		}
		rf.mu.Unlock()

		if !rf.appendEntries(server, 100) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Commits log entries of a leader
func (rf *Raft) commitLog() {
	for {
		rf.mu.Lock()
		if rf.isKilled || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf("matchIndex of %d: %v", rf.me, rf.matchIndex)
		N := rf.commitIndex
		for peer, _ := range rf.peers {
			n := rf.matchIndex[peer]
			if n <= rf.commitIndex {
				continue
			}
			count := 0
			for peer, _ := range rf.peers {
				if n <= rf.matchIndex[peer] {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				N = n
			}
		}
		committed := N - rf.commitIndex
		if committed == 0 || rf.log[N].Term != rf.currentTerm {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		DPrintf("commitIndex of %d changed: %d -> %d",
			rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.logCommitted.Broadcast()
		rf.mu.Unlock()
	}
}

func (rf *Raft) runAsLeader() {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go rf.replicateLog(peer)
		}
	}

	go rf.commitLog()

	for {
		rf.mu.Lock()
		if rf.isKilled || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		timeoutCh := time.After(HeartbeatInterval * time.Millisecond)

		// Broadcast heartbeats to followers
		ch := make(chan bool, len(rf.peers))
		for peer, _ := range rf.peers {
			if peer == rf.me {
				ch <- true
				continue
			}
			go func(peer int) {
				ch <- rf.appendEntries(peer, 0)
			}(peer)
		}

		for {
			select {
			case ok := <-ch:
				// if the leader loses its authority,
				// it should step down immediately.
				if !ok {
					return
				}
				continue
			case <-timeoutCh:
			}
			break
		}
	}
}

// Applies log entries to state machine
func (rf *Raft) applyLog() {
	for {
		rf.mu.Lock()

		for {
			if rf.isKilled {
				rf.mu.Unlock()
				return
			}

			if rf.lastApplied == rf.commitIndex {
				rf.logCommitted.Wait()
			} else {
				break
			}
		}

		msgs := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			j := i - rf.lastApplied - 1
			msgs[j] = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) run() {
	go rf.applyLog()

	for {
		rf.mu.Lock()
		state, isKilled := rf.state, rf.isKilled
		rf.mu.Unlock()

		if isKilled {
			break
		}

		DPrintf("loop: state of %d: %d", rf.me, state)

		switch state {
		case Follower:
			rf.runAsFollower()
		case Candidate:
			rf.runForLeader()
		case Leader:
			rf.runAsLeader()
		default:
			panic("Illigal state")
		}

	}
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

	DPrintf("Start peer %d", rf.me)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatCh = make(chan int)

	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.replicateCond = sync.NewCond(&rf.mu)
	rf.logCommitted = sync.NewCond(&rf.mu)

	rf.isKilled = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
