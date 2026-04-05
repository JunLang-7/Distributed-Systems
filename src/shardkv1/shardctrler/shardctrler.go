package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	cfgCurrentKey = "shardcfg/current"
	cfgNextKey    = "shardcfg/next"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	rcks map[tester.Tgid]*shardgrp.Clerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, rcks: make(map[tester.Tgid]*shardgrp.Clerk)}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) getClerk(cfg *shardcfg.ShardConfig, gid tester.Tgid) (*shardgrp.Clerk, bool) {
	srvs, ok := cfg.Groups[gid]
	if !ok || len(srvs) == 0 {
		return nil, false
	}
	if ck, ok := sck.rcks[gid]; ok {
		return ck, true
	}
	ck := shardgrp.MakeClerk(sck.clnt, srvs)
	sck.rcks[gid] = ck
	return ck, true
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	cur := sck.Query()
	nextVal, _, err := sck.IKVClerk.Get(cfgNextKey)
	if err != rpc.OK {
		// nothing to change
		return
	}
	next := shardcfg.FromString(nextVal)
	// if there is a stored next configuration with a
	// higher configuration number than the current one
	if next.Num > cur.Num {
		sck.ChangeConfigTo(next)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	want := cfg.String()
	for {
		err := sck.IKVClerk.Put(cfgCurrentKey, want, 0)
		if err == rpc.OK {
			return
		}

		// In unreliable networks, a successful first Put may lose its reply;
		// a retry can surface as ErrMaybe/ErrVersion. Confirm by reading back.
		if err == rpc.ErrMaybe || err == rpc.ErrVersion {
			val, _, gerr := sck.IKVClerk.Get(cfgCurrentKey)
			if gerr == rpc.OK && val == want {
				return
			}
			if gerr == rpc.ErrNoKey {
				continue
			}
		}

		if err != rpc.ErrMaybe {
			log.Fatalf("InitConfig err:%s ", err)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		// store `new` as next cfg
		_, ver, err := sck.IKVClerk.Get(cfgNextKey)
		if err == rpc.OK || err == rpc.ErrNoKey {
			v := ver
			if err == rpc.ErrNoKey {
				v = 0
			}
			err = sck.IKVClerk.Put(cfgNextKey, new.String(), v)
			if err == rpc.OK {
				break
			}
		}
	}
	cur := sck.Query()
	for i := range shardcfg.NShards {
		if cur.Shards[i] == new.Shards[i] {
			continue
		}

		s := shardcfg.Tshid(i)
		srcGid := cur.Shards[i]
		dstGid := new.Shards[i]

		src, ok := sck.getClerk(cur, srcGid)
		if !ok {
			continue
		}
		dst, ok := sck.getClerk(new, dstGid)
		if !ok {
			continue
		}

		var state []byte
		for {
			var err rpc.Err
			state, err = src.FreezeShard(s, new.Num)
			if err == rpc.OK {
				break
			}
		}
		for {
			if err := dst.InstallShard(s, state, new.Num); err == rpc.OK {
				break
			}
		}
		for {
			if err := src.DeleteShard(s, new.Num); err == rpc.OK {
				break
			}
		}
	}

	for {
		// get version
		_, ver, err := sck.IKVClerk.Get(cfgCurrentKey)
		if err != rpc.OK {
			continue
		}
		err = sck.IKVClerk.Put(cfgCurrentKey, new.String(), ver)
		if err == rpc.OK {
			return
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	val, _, err := sck.IKVClerk.Get(cfgCurrentKey)
	if err == rpc.OK {
		return shardcfg.FromString(val)
	}
	log.Fatalf("Query err:%s ", err)
	return nil
}
