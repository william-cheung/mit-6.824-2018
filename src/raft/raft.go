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

// import "bytes"
// import "labgob"

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
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
	heartbeatCh chan int
	isKilled    bool // Whether this peer has been killed
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
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so followers can redirect clients
}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term int // currentTerm for leader to update itself

	// true if follwer contained entry matching
	// PrevLogIndex and PrevLogTerm
	Success bool
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

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.transitionTo(args.Term, Follower)
	}

	reply.Term, reply.Success = rf.currentTerm, true

	select {
	case rf.heartbeatCh <- 1:
	default:
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("AppendEntries(%d->%d): args %+v --> %+v reply %+v", rf.me, server, args, ok, reply)
	return ok
}

//
// RequestVote RPC arguments structure
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate requesting votes
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate receives vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		rf.transitionTo(args.Term, Follower)
		reply.Term, reply.VoteGranted = args.Term, true
		return
	}

	var voteGranted bool
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		voteGranted = true
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, voteGranted
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("RequestVote(%d->%d): args %+v --> %+v reply %+v", rf.me, server, args, ok, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.isKilled = true
}

func (rf *Raft) IsKilled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isKilled

}

func (rf *Raft) runAsFollower() {
	electionTimeout := time.Duration(rand.Intn(300)+500) * time.Millisecond
	for {
		if rf.IsKilled() {
			return
		}

		t := time.NewTimer(electionTimeout)
		select {
		case <-rf.heartbeatCh:
			t.Stop()
			continue
		case <-t.C:
		}
		break
	}

	rf.mu.Lock()
	if !rf.isKilled {
		rf.transitionTo(rf.currentTerm+1, Candidate)
	}
	rf.mu.Unlock()
}

func (rf *Raft) collectVotes() <-chan bool {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	votesCh := make(chan bool, len(rf.peers))
	for peer, _ := range rf.peers {
		go func(peer int) {
			var reply RequestVoteReply

			if peer == rf.me {
				rf.RequestVote(&args, &reply)
				votesCh <- true
				return
			}

			ok := rf.sendRequestVote(peer, &args, &reply)
			if ok {
				if reply.VoteGranted {
					votesCh <- true
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.transitionTo(reply.Term, Follower)
				}
				rf.mu.Unlock()

			}
			votesCh <- false
		}(peer)
	}
	return votesCh

}

func (rf *Raft) runForLeader() {
	electionTimeout := time.Duration(rand.Intn(300)+500) * time.Millisecond

	timeoutCh := time.After(electionTimeout)

	votesCh := rf.collectVotes()
	half := len(rf.peers) / 2
	count, countVotes := 0, 0

	for vote := range votesCh {
		if vote {
			countVotes += 1
		}
		count += 1
		if countVotes > half || count-countVotes > half {
			break
		}
	}

	state := Candidate
	if countVotes > half {
		state = Leader
	} else {
		select {
		case <-rf.heartbeatCh:
			state = Follower
		case <-timeoutCh:
			state = Candidate
		}
	}

	rf.mu.Lock()
	if rf.state == Candidate {
		if state == Candidate {
			rf.transitionTo(rf.currentTerm+1, state)
		} else {
			rf.transitionTo(rf.currentTerm, state)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) runAsLeader() {
	heartbeatInterval := 100 * time.Millisecond
	for {
		timeoutCh := time.After(heartbeatInterval)

		rf.mu.Lock()
		term, state, isKilled := rf.currentTerm, rf.state, rf.isKilled
		rf.mu.Unlock()

		if isKilled || state != Leader {
			return
		}

		args := AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		failCh := make(chan bool, len(rf.peers))
		for peer, _ := range rf.peers {
			go func(peer int) {
				if peer == rf.me {
					failCh <- false
					return
				}
				var reply AppendEntriesReply
				if !rf.sendAppendEntries(peer, &args, &reply) {
					failCh <- false
					return
				}
				if reply.Success {
					failCh <- false
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.transitionTo(reply.Term, Follower)
				}
				rf.mu.Unlock()
				failCh <- true
			}(peer)
		}

		count := 0
	loop:
		for {
			select {
			case fail := <-failCh:
				if fail {
					return
				}
				count += 1

			case <-timeoutCh:
				break loop
			}
			if count >= len(rf.peers) {
				<-timeoutCh
				break
			}
		}
	}
}

func (rf *Raft) run() {
	for {
		rf.mu.Lock()
		term, state, isKilled := rf.currentTerm, rf.state, rf.isKilled
		rf.mu.Unlock()
		if isKilled {
			break
		}
		DPrintf("loop: state of %d: <%d, %d>", rf.me, term, state)
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

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("Start peer %d", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatCh = make(chan int)
	rf.isKilled = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
