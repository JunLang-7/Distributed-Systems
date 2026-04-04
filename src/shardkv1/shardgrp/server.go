package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVServer struct {
	me     int
	rsm    *rsm.RSM
	gid    tester.Tgid
	mu     sync.Mutex
	store  map[string]ValueStore
	owned  map[shardcfg.Tshid]bool
	frozen map[shardcfg.Tshid]bool
	seen   map[shardcfg.Tshid]shardcfg.Tnum
}

type ValueStore struct {
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {
	case rpc.GetArgs:
		shid := shardcfg.Key2Shard(req.Key)
		if !kv.owned[shid] || kv.frozen[shid] {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		state, ok := kv.store[req.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: state.Value, Version: state.Version, Err: rpc.OK}
	case rpc.PutArgs:
		shid := shardcfg.Key2Shard(req.Key)
		if !kv.owned[shid] || kv.frozen[shid] {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		state, ok := kv.store[req.Key]
		if !ok {
			if req.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.store[req.Key] = ValueStore{Value: req.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if req.Version != state.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		state.Value = req.Value
		state.Version += 1
		kv.store[req.Key] = state
		return rpc.PutReply{Err: rpc.OK}
	case shardrpc.FreezeShardArgs:
		lastNum := kv.seen[req.Shard]
		if req.Num < lastNum || !kv.owned[req.Shard] {
			return shardrpc.FreezeShardReply{Num: lastNum, Err: rpc.ErrWrongGroup}
		}
		if req.Num > lastNum {
			kv.seen[req.Shard] = req.Num
		}
		kv.frozen[req.Shard] = true
		state := make(map[string]ValueStore)
		for k, v := range kv.store {
			if shardcfg.Key2Shard(k) == req.Shard {
				state[k] = v
			}
		}
		return shardrpc.FreezeShardReply{State: kv.encodeShardState(state), Num: lastNum, Err: rpc.OK}
	case shardrpc.InstallShardArgs:
		lastNum := kv.seen[req.Shard]
		if req.Num < lastNum {
			return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}
		if req.Num > lastNum {
			kv.seen[req.Shard] = req.Num
		}
		state := kv.decodeShardState(req.State)
		for k := range kv.store {
			if shardcfg.Key2Shard(k) == req.Shard {
				delete(kv.store, k)
			}
		}
		for k, v := range state {
			kv.store[k] = v
		}
		kv.owned[req.Shard] = true
		kv.frozen[req.Shard] = false
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		lastNum := kv.seen[req.Shard]
		if req.Num < lastNum {
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}
		if req.Num > lastNum {
			kv.seen[req.Shard] = req.Num
		}
		for k := range kv.store {
			if shardcfg.Key2Shard(k) == req.Shard {
				delete(kv.store, k)
			}
		}
		delete(kv.owned, req.Shard)
		delete(kv.frozen, req.Shard)
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) encodeShardState(data map[string]ValueStore) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(data)
	return w.Bytes()
}

func (kv *KVServer) decodeShardState(data []byte) map[string]ValueStore {
	if data == nil || len(data) < 1 {
		return make(map[string]ValueStore)
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	s := make(map[string]ValueStore)
	if d.Decode(&s) != nil {
		return make(map[string]ValueStore)
	}
	return s
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.owned)
	e.Encode(kv.frozen)
	e.Encode(kv.seen)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[string]ValueStore
	var owned map[shardcfg.Tshid]bool
	var frozen map[shardcfg.Tshid]bool
	var seen map[shardcfg.Tshid]shardcfg.Tnum
	if d.Decode(&store) != nil || d.Decode(&owned) != nil || d.Decode(&frozen) != nil || d.Decode(&seen) != nil {
		panic("failed to decode snapshot")
	} else {
		kv.mu.Lock()
		kv.store = store
		kv.owned = owned
		kv.frozen = frozen
		kv.seen = seen
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongGroup {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongGroup {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r, ok := rep.(shardrpc.FreezeShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = r
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r, ok := rep.(shardrpc.InstallShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = r
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r, ok := rep.(shardrpc.DeleteShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = r
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:    gid,
		me:     me,
		mu:     sync.Mutex{},
		store:  make(map[string]ValueStore),
		owned:  make(map[shardcfg.Tshid]bool),
		frozen: make(map[shardcfg.Tshid]bool),
		seen:   make(map[shardcfg.Tshid]shardcfg.Tnum),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.owned[shardcfg.Tshid(i)] = true
		}
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
