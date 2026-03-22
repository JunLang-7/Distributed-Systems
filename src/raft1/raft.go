package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
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
	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []int
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
	// other properties
	role          Role
	electionTick  *time.Timer
	heartbeatTick *time.Timer
	rng           *rand.Rand
}

// Timeouts
const (
	ElectionTimeout  int64 = 1000
	HeartbeatTimeout int64 = 100
)

func (rf *Raft) RandomElectionTimeout() time.Duration {
	ms := ElectionTimeout + rf.rng.Int63()%ElectionTimeout
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) RandomHeartbeatTimeout() time.Duration {
	ms := HeartbeatTimeout + rf.rng.Int63()%HeartbeatTimeout
	return time.Duration(ms) * time.Millisecond
}

// Role raft role
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// ChangeRole change role from `rf.role` to `newRole` and reset timer
func (rf *Raft) ChangeRole(newRole Role) {
	DPrintf("{Node %v} Change Role from %v to %v", rf.me, rf.role, newRole)
	rf.role = newRole
	switch newRole {
	case Follower:
		rf.electionTick.Reset(rf.RandomElectionTimeout())
		rf.heartbeatTick.Stop()
	case Candidate:
		rf.electionTick.Reset(rf.RandomElectionTimeout())
	case Leader:
		rf.electionTick.Stop()
		rf.heartbeatTick.Reset(rf.RandomHeartbeatTimeout())
	}
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

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// RequestVoteReply RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.ChangeRole(Follower)
	}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Log check should be here (3B).
		rf.votedFor = args.CandidateId
		// reset election timer to avoid split vote
		rf.electionTick.Reset(rf.RandomElectionTimeout())
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		DPrintf("{Node %v} grants vote to {Node %v} for term %v", rf.me, args.CandidateId, args.Term)
		return
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, false
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

// AppendEntriesArgs AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// AppendEntriesReply AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.ChangeRole(Follower)
	rf.electionTick.Reset(rf.RandomElectionTimeout())
	reply.Term, reply.Success = rf.currentTerm, true
}

// sendAppendEntries send AppendEntries RPC to server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {
		select {
		case <-rf.electionTick.C:
			rf.mu.Lock()
			// On conversion to candidate, start election (§5.2)
			rf.ChangeRole(Candidate)
			rf.StartElection()
			rf.mu.Unlock()
		case <-rf.heartbeatTick.C:
			rf.mu.Lock()
			if rf.role == Leader {
				rf.BroadcastHeartbeat()
				rf.heartbeatTick.Reset(rf.RandomHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// StartElection start election.
func (rf *Raft) StartElection() {
	// Increment currentTerm
	rf.currentTerm += 1
	// Vote for self
	rf.votedFor = rf.me
	args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	votes := 1
	DPrintf("{Node %v} start election with RequestVoteArgs %v", rf.me, args)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			// Send RequestVote RPCs to all other servers
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v}", rf.me, reply, peer)
				if args.Term == rf.currentTerm && rf.role == Candidate {
					if reply.VoteGranted {
						votes++
						// If votes received from majority of servers: become leader
						if votes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives over half of the votes, becomes leader", rf.me)
							rf.ChangeRole(Leader)
							// send heartbeat to prevent others from new election
							rf.BroadcastHeartbeat()
						}
					} else if reply.Term > rf.currentTerm {
						// another server establishes itself as leader
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.ChangeRole(Follower)
					}
				}
			}
		}(peer)
	}
}

// BroadcastHeartbeat broadcast heartbeat to all followers.
func (rf *Raft) BroadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			rf.mu.Unlock()
			reply := new(AppendEntriesReply)
			// Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server
			if rf.sendAppendEntries(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.currentTerm && rf.role == Leader {
					if !reply.Success {
						// If AppendEntries RPC received from new leader: convert to follower
						if reply.Term > rf.currentTerm {
							rf.ChangeRole(Follower)
							rf.currentTerm, rf.votedFor = reply.Term, -1
						}
					}
				}
				DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v", rf.me, args, peer, reply)
			}
		}(peer)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1_000_000))
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		role:          Follower,
		electionTick:  time.NewTimer(time.Duration(ElectionTimeout+rng.Int63n(ElectionTimeout)) * time.Millisecond),
		heartbeatTick: time.NewTimer(time.Duration(HeartbeatTimeout+rng.Int63n(HeartbeatTimeout)) * time.Millisecond),
		rng:           rng,
	}
	// stop heartbeat ticker when server is not leader
	rf.heartbeatTick.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
