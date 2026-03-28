package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
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
	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int
	matchIndex []int
	// other properties
	role          Role
	electionTick  *time.Timer
	heartbeatTick *time.Timer
	rng           *rand.Rand
	applyCh       chan raftapi.ApplyMsg
	applyCond     *sync.Cond
	replicateCond []*sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	raftstate := rf.encodeState()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("{Node %v} failed to read persisted log", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex, rf.lastApplied = rf.log[0].Index, rf.log[0].Index
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.log[0].Index
	if index <= firstIndex || index > rf.log[len(rf.log)-1].Index {
		DPrintf("{Node %v} snapshot is out of right index}", rf.me)
		return
	}
	// discard its log entries before `index`
	newLog := make([]LogEntry, len(rf.log)-(index-firstIndex))
	copy(newLog, rf.log[index-firstIndex:])
	rf.log = newLog
	rf.log[0].Command = nil
	// save raft state into snapshot
	rf.persister.Save(rf.encodeState(), snapshot)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if this server isn't the leader, returns false.
	if rf.role != Leader {
		return -1, -1, false
	}
	index := rf.log[len(rf.log)-1].Index + 1
	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, Index: index})
	rf.persist()
	rf.nextIndex[rf.me], rf.matchIndex[rf.me] = index+1, index
	DPrintf("{Node %v} starts agreement on a new log entry with command %v in term %v", rf.me, command, rf.currentTerm)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.replicateCond[peer].Signal()
	}

	return index, rf.currentTerm, true
}

func (rf *Raft) needReplication(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the logs of peer is behind the leader
	return rf.role == Leader && rf.matchIndex[peer] < rf.log[len(rf.log)-1].Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()
	for true {
		for !rf.needReplication(peer) {
			rf.replicateCond[peer].Wait()
		}
		// send log to peer
		rf.leaderReplication(peer)
	}
}

func (rf *Raft) ticker() {
	for {
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
				rf.heartbeatTick.Reset(rf.HeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		// check if the commitIndex is advanced
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		firstIndex := rf.log[0].Index
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, 0)
		entries = append(entries, rf.log[lastApplied-firstIndex+1:commitIndex-firstIndex+1]...)
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// StartElection start election.
func (rf *Raft) StartElection() {
	// Increment currentTerm
	rf.currentTerm += 1
	// Vote for self
	rf.votedFor = rf.me
	rf.persist()
	lastLog := rf.log[len(rf.log)-1]
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
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
		go rf.leaderReplication(peer)
	}
}

// leaderReplication send log to peer.
func (rf *Raft) leaderReplication(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	firstIndex := rf.log[0].Index
	lastIndex := rf.log[len(rf.log)-1].Index
	if rf.nextIndex[peer] < firstIndex {
		rf.nextIndex[peer] = firstIndex
	}
	if rf.nextIndex[peer] > lastIndex+1 {
		rf.nextIndex[peer] = lastIndex + 1
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	// if prevLogIndex is out of range, only send InstallSnapshot RPC
	if prevLogIndex < firstIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: firstIndex,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == Leader && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.ChangeRole(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else {
					rf.matchIndex[peer], rf.nextIndex[peer] = args.LastIncludedIndex, args.LastIncludedIndex+1
				}
			}
			DPrintf("{Node %v} send InstallSnapshot %v to {Node %v} and get reply %v", rf.me, args, peer, reply)
		}
		return
	}
	prevLogTerm := rf.log[prevLogIndex-firstIndex].Term
	entries := make([]LogEntry, 0)
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	if rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer] {
		entries = make([]LogEntry, len(rf.log[rf.nextIndex[peer]-firstIndex:]))
		copy(entries, rf.log[rf.nextIndex[peer]-firstIndex:])
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := new(AppendEntriesReply)
	// Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server
	if rf.sendAppendEntries(peer, args, reply) {
		DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v",
			rf.me, args, peer, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term == rf.currentTerm && rf.role == Leader {
			if reply.Success {
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				if newMatchIndex > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatchIndex
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					// advance CommitIndex if possible
					rf.advanceCommitIndex()
				}
			} else {
				if reply.Term > rf.currentTerm {
					// If AppendEntries RPC received from new leader: convert to follower
					rf.ChangeRole(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else if reply.Term == rf.currentTerm {
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3)
					firstIndex := rf.log[0].Index
					lastIndex := rf.log[len(rf.log)-1].Index
					rf.nextIndex[peer] = min(max(reply.ConflictIndex, firstIndex), lastIndex + 1)
					if reply.ConflictTerm != -1 {
						if args.PrevLogIndex >= firstIndex {
							// find nextIndex through binary search
							left, right := firstIndex-1, args.PrevLogIndex
							for left+1 < right {
								mid := (left + right) / 2
								if rf.log[mid-firstIndex].Term <= reply.ConflictTerm {
									left = mid
								} else {
									right = mid
								}
							}
							if right >= firstIndex && rf.log[right-firstIndex].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = right + 1
							}
						}
					}
				}
			}
		}
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4).
func (rf *Raft) advanceCommitIndex() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if newCommitIndex <= rf.log[len(rf.log)-1].Index &&
			rf.currentTerm == rf.log[newCommitIndex-rf.log[0].Index].Term {
			DPrintf("{Node %v} advances commitIndex from %v to %v in term %v",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1_000_000))
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]LogEntry, 1),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		role:          Follower,
		electionTick:  time.NewTimer(time.Duration(ElectionTimeout+rng.Int63n(ElectionTimeout)) * time.Millisecond),
		heartbeatTick: time.NewTimer(time.Duration(HeartbeatTimeout+rng.Int63n(HeartbeatTimeout)) * time.Millisecond),
		rng:           rng,
		applyCh:       applyCh,
		replicateCond: make([]*sync.Cond, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	// stop heartbeat ticker when server is not leader
	rf.heartbeatTick.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range rf.peers {
		rf.nextIndex[peer], rf.matchIndex[peer] = rf.log[len(rf.log)-1].Index+1, 0
		if peer != rf.me {
			rf.replicateCond[peer] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to send log entries to peer
			go rf.replicator(peer)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// start apply goroutine to apply log entries to state machine
	go rf.applier()
	return rf
}
