package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me  int
	Req any
	ID  uint64
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu               sync.Mutex
	me               int
	rf               raftapi.Raft
	applyCh          chan raftapi.ApplyMsg
	maxraftstate     int // snapshot if log grows this big
	sm               StateMachine
	epoch            uint64
	nexSeq           uint64
	dead             bool                         // applyCh is closed when this is true
	waitChByOpID     map[uint64]chan submitResult // map from op ID to channel for delivering result
	pendingLogByOpID map[int]uint64               // map from log index to op ID
}

// submitResult represents the result of a submitted operation
type submitResult struct {
	err rpc.Err
	rep any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1_000_000))
	rsm := &RSM{
		me:               me,
		maxraftstate:     maxraftstate,
		applyCh:          make(chan raftapi.ApplyMsg),
		sm:               sm,
		waitChByOpID:     make(map[uint64]chan submitResult),
		pendingLogByOpID: make(map[int]uint64),
		epoch:            rng.Uint64(),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			rep := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			// Check if this index has a pending request
			if expectID, ok := rsm.pendingLogByOpID[msg.CommandIndex]; ok {
				if expectID == op.ID {
					// This is the expected request, deliver the result
					if ch, exists := rsm.waitChByOpID[op.ID]; exists {
						ch <- submitResult{rpc.OK, rep}
						// move out from pending list
						delete(rsm.waitChByOpID, op.ID)
					}
				} else {
					// Different request got committed at this index
					// The original request must have lost leadership
					if ch, exists := rsm.waitChByOpID[op.ID]; exists {
						ch <- submitResult{rpc.ErrWrongLeader, rep}
						delete(rsm.waitChByOpID, op.ID)
					}
				}
				delete(rsm.pendingLogByOpID, msg.CommandIndex)
			}
			rsm.mu.Unlock()
		}
	}

	// applyCh is closed, wake up all pending requests
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.dead = true
	for _, ch := range rsm.waitChByOpID {
		ch <- submitResult{rpc.ErrWrongLeader, nil}
	}
	rsm.waitChByOpID = make(map[uint64]chan submitResult)
	rsm.pendingLogByOpID = make(map[int]uint64)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	rsm.mu.Lock()
	op := Op{Me: rsm.me, Req: req, ID: rsm.epoch<<32 | rsm.nexSeq}
	rsm.nexSeq++
	rsm.mu.Unlock()

	index, startTerm, isLeader := rsm.rf.Start(op)
	// if not the leader, return ErrWrongLeader immediately
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan submitResult, 1)
	rsm.mu.Lock()
	// If applyCh closed, exit out of all loops.
	if rsm.dead {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// Record the pending request with its log index and op ID
	rsm.pendingLogByOpID[index] = op.ID
	rsm.waitChByOpID[op.ID] = ch
	rsm.mu.Unlock()

	// Periodic ticker to check if we've lost leadership
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case res := <-ch:
			// Successfully received result from reader goroutine
			return res.err, res.rep
		case <-ticker.C:
			// Periodically check if we've lost leadership
			rsm.mu.Lock()
			currentTerm, isLeader := rsm.rf.GetState()
			// detect that it has lost leadership by noticing that Raft's term has changed
			if currentTerm != startTerm || !isLeader {
				// Leadership has changed, clean up and return error
				delete(rsm.pendingLogByOpID, index)
				delete(rsm.waitChByOpID, op.ID)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
