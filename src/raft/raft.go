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
import "sync/atomic"
import "labrpc"
import "time"

// import "bytes"
// import "labgob"



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
type LogEntry struct {
	Command interface{}
	Term int // term when entry was received by leader
}
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	//voted for to be initialized to -1
	VotedFor   int 	// candidateId that received vote in current term (or null if none)
	Log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	CommitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int // index of highest log entry applied to state machine
	NextIndex  []int // for each server, index of the next log entry to send to that server
	MatchIndex []int // for each server, index of highest log entry known to be replicated on server
	State      string // "follower", "candidate", or "leader"
	ElectionTimeout int // timeout for elections
	ApplyCh   chan ApplyMsg // channel to send ApplyMsg to service or tester
	LastHeartbeat time.Time // time of last heartbeat received




}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//lock for thread safety
	rf.mu.Lock()
	term = rf.CurrentTerm // get the current term
	isleader = (rf.State == "leader")
	rf.mu.Unlock()
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term 	  int    // candidate's term
	CandidateId int
	LastlogIndex int // index of candidate's last log entry
	LastlogTerm  int // term of candidate's last log entry
	// What else does a candidate need to send when asking for votes
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// What does a server reply with when receiving a vote request?
	// It should include the current term and whether it granted the vote.
	// If the term in the request is less than the server's current term,
	// the server should not grant the vote and should return false.
	// If the term in the request is greater than or equal to the server's
	// current term, the server should grant the vote and update its term.
	// If the server is not a candidate, it should return false.
	// If the server is a candidate and the request is valid, it should
	// grant the vote and return true.
	// Make sure to handle the case where the server's term is updated
	// while processing the request.
	// This can happen if the server receives a heartbeat from a leader
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// Default reply values
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	
	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	
	// Rule 2: If args.Term > currentTerm, update term and become follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1  // Reset vote for new term
		rf.State = "follower"
		rf.persist()
	}
	
	// Update reply term after potential update
	reply.Term = rf.CurrentTerm
	
	// Rule 3: Check if we can vote for this candidate
	canVote := (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId)
	
	// Rule 4: Check if candidate's log is at least as up-to-date as ours
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.Log[lastLogIndex].Term
	}
	
	logUpToDate := (args.LastlogTerm > lastLogTerm) || 
					(args.LastlogTerm == lastLogTerm && args.LastlogIndex >= lastLogIndex)
	
	// Grant vote if both conditions are met
	if canVote && logUpToDate {
		rf.VotedFor = args.CandidateId
		rf.LastHeartbeat = time.Now()
		reply.VoteGranted = true
		rf.persist()
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{}
	rf.CommitIndex = 0
	//liast of varibles I think this should have
	//last applied, nextIndex, matchindex, state, election timeout:yes/no, channel for apply messages, last heartbeat time
	//as the paper says, everyone is a follower at the start
	rf.LastApplied = 0
	rf.State = "follower"
	rf.ApplyCh = applyCh
	rf.ElectionTimeout = 150 + me*10 // stagger election timeouts to avoid
	// simultaneous elections. 150ms is a good base value.
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.LastHeartbeat = time.Now()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
