package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		config := ck.sck.Query()
		shid := shardcfg.Key2Shard(key)
		gid, servers, ok := config.GidServers(shid)
		if !ok {
			continue
		}
		if ck.rcks[gid] == nil {
			clk := shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = clk
		}
		val, version, err := ck.rcks[gid].Get(key)
		if err == rpc.ErrWrongGroup {
			continue
		}
		return val, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	uncertain := false
	for {
		config := ck.sck.Query()
		shid := shardcfg.Key2Shard(key)
		gid, servers, ok := config.GidServers(shid)
		if !ok {
			continue
		}
		if ck.rcks[gid] == nil {
			clk := shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = clk
		}
		err := ck.rcks[gid].Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			// We may have sent the RPCs but lost replies; keep this bit so a
			// later ErrVersion can be surfaced as ErrMaybe.
			uncertain = true
			continue
		}
		if err == rpc.ErrVersion && uncertain {
			return rpc.ErrMaybe
		}
		return err
	}
}
