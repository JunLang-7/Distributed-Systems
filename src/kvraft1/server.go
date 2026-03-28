package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me    int
	rsm   *rsm.RSM
	mu    sync.Mutex
	Store map[string]ValueStore
}

type ValueStore struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {
	case rpc.GetArgs:
		state, ok := kv.Store[req.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: state.Value, Version: state.Version, Err: rpc.OK}
	case *rpc.GetArgs:
		state, ok := kv.Store[req.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: state.Value, Version: state.Version, Err: rpc.OK}
	case rpc.PutArgs:
		state, ok := kv.Store[req.Key]
		if !ok {
			if req.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.Store[req.Key] = ValueStore{Value: req.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if req.Version != state.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		state.Value = req.Value
		state.Version += 1
		kv.Store[req.Key] = state
		return rpc.PutReply{Err: rpc.OK}
	case *rpc.PutArgs:
		state, ok := kv.Store[req.Key]
		if !ok {
			if req.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.Store[req.Key] = ValueStore{Value: req.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if req.Version != state.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		state.Value = req.Value
		state.Version += 1
		kv.Store[req.Key] = state
		return rpc.PutReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Store)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[string]ValueStore
	if d.Decode(&store) != nil {
		panic("failed to decode snapshot")
	} else {
		kv.mu.Lock()
		kv.Store = store
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, mu: sync.Mutex{}, Store: make(map[string]ValueStore)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
