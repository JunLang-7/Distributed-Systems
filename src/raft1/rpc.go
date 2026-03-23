package raft

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	lastLog := rf.log[len(rf.log)-1]
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
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

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	firstIndex := rf.log[0].Index
	if args.PrevLogIndex < firstIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index ||
		args.PrevLogTerm != rf.log[args.PrevLogIndex-firstIndex].Term {
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.log[len(rf.log)-1].Index
		// find the first index of conflict term
		if lastIndex < args.PrevLogIndex {
			reply.ConflictIndex, reply.ConflictTerm = lastIndex+1, -1
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex
			for index > firstIndex && rf.log[index-firstIndex-1].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	// Append any new entries not already in the log
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || entry.Term != rf.log[entry.Index-firstIndex].Term {
			rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...)
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		// Send each newly committed entry on `applyCh` on each peer.
		rf.applyCond.Signal()
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

// sendAppendEntries send AppendEntries RPC to server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
