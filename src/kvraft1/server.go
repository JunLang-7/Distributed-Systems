package kvraft

import (
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
	store map[string]valueStore
}

type valueStore struct {
	value   string
	version rpc.Tversion
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
		state, ok := kv.store[req.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: state.value, Version: state.version, Err: rpc.OK}
	case *rpc.GetArgs:
		state, ok := kv.store[req.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: state.value, Version: state.version, Err: rpc.OK}
	case rpc.PutArgs:
		state, ok := kv.store[req.Key]
		if !ok {
			if req.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.store[req.Key] = valueStore{value: req.Value, version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if req.Version != state.version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		state.value = req.Value
		state.version += 1
		kv.store[req.Key] = state
		return rpc.PutReply{Err: rpc.OK}
	case *rpc.PutArgs:
		state, ok := kv.store[req.Key]
		if !ok {
			if req.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.store[req.Key] = valueStore{value: req.Value, version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if req.Version != state.version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		state.value = req.Value
		state.version += 1
		kv.store[req.Key] = state
		return rpc.PutReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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

	kv := &KVServer{me: me, mu: sync.Mutex{}, store: make(map[string]valueStore)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
