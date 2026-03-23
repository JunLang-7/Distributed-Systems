package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// Role raft role
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

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
		rf.heartbeatTick.Reset(rf.HeartbeatTimeout())
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
			rf.matchIndex[peer] = 0
		}
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// Timeouts
const (
	ElectionTimeout  int64 = 500
	HeartbeatTimeout int64 = 100
)

func (rf *Raft) RandomElectionTimeout() time.Duration {
	ms := ElectionTimeout + rf.rng.Int63()%ElectionTimeout
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) HeartbeatTimeout() time.Duration {
	ms := HeartbeatTimeout
	return time.Duration(ms) * time.Millisecond
}
