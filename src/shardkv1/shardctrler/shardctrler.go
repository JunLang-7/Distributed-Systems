package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"encoding/json"
	"log"
	"time"

	"6.5840/kvraft1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	cfgCurrentKey = "shardcfg/current"
	cfgNextKey    = "shardcfg/next"
	cfgLeaseKey   = "ctrller/lease"
	LeaseTimeout  = time.Second * 5
	RenewInterval = time.Second * 1
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	rcks map[tester.Tgid]*shardgrp.Clerk

	killed int32 // set by Kill()

	controllerID string
	isLeader     bool
	leaseExpiry  int64
}

// Make a ShardCltler, which stores its state in a kvraft.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, rcks: make(map[tester.Tgid]*shardgrp.Clerk), controllerID: kvtest.RandValue(8)}
	srvs := []string{tester.ServerName(tester.GRP0, 0), tester.ServerName(tester.GRP0, 1), tester.ServerName(tester.GRP0, 2)}
	sck.IKVClerk = kvraft.MakeClerk(clnt, srvs)
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

func cfgEqual(a *shardcfg.ShardConfig, b *shardcfg.ShardConfig) bool {
	if a == nil || b == nil {
		return false
	}
	return a.String() == b.String()
}

func (sck *ShardCtrler) shouldAbortChange(target *shardcfg.ShardConfig) bool {
	cur := sck.Query()
	if cur.Num >= target.Num {
		return true
	}
	val, _, err := sck.IKVClerk.Get(cfgNextKey)
	if err != rpc.OK {
		return true
	}
	next := shardcfg.FromString(val)
	if next.Num != target.Num || !cfgEqual(next, target) {
		return true
	}
	return false
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	acquired := sck.tryAcquireLease()
	cur := sck.Query()
	nextVal, _, err := sck.IKVClerk.Get(cfgNextKey)
	if err != rpc.OK {
		// nothing to change
		if acquired {
			sck.releaseLease()
		}
		return
	}
	next := shardcfg.FromString(nextVal)
	// if there is a stored next configuration with a
	// higher configuration number than the current one
	if next.Num > cur.Num {
		sck.ChangeConfigTo(next)
	}
	if acquired {
		sck.releaseLease()
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
		cur := sck.Query()
		if new.Num <= cur.Num {
			return
		}

		// store `new` as next cfg
		val, ver, err := sck.IKVClerk.Get(cfgNextKey)
		if err == rpc.ErrNoKey {
			if sck.IKVClerk.Put(cfgNextKey, new.String(), 0) == rpc.OK {
				break
			}
			continue
		}
		if err != rpc.OK {
			continue
		}

		next := shardcfg.FromString(val)
		if next.Num > new.Num {
			return
		}
		if next.Num == new.Num {
			if cfgEqual(next, new) {
				break
			}
			return
		}
		if sck.IKVClerk.Put(cfgNextKey, new.String(), ver) == rpc.OK {
			break
		}
	}
	lastRenew := time.Now()
	cur := sck.Query()
	for i := range shardcfg.NShards {
		if sck.shouldAbortChange(new) {
			return
		}

		// periodically check & renew lease
		if sck.isLeader && time.Since(lastRenew) > RenewInterval {
			sck.renewLease()
			lastRenew = time.Now()
		}

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
		freezeSkipped := false
		for {
			if sck.shouldAbortChange(new) {
				return
			}
			var err rpc.Err
			state, err = src.FreezeShard(s, new.Num)
			if err == rpc.OK {
				break
			}
			if err == rpc.ErrWrongGroup {
				// Another controller may have already moved/deleted this shard for this Num.
				freezeSkipped = true
				break
			}
		}
		if freezeSkipped {
			continue
		}
		for {
			if sck.shouldAbortChange(new) {
				return
			}
			err := dst.InstallShard(s, state, new.Num)
			if err == rpc.OK || err == rpc.ErrWrongGroup {
				break
			}
		}
		for {
			if sck.shouldAbortChange(new) {
				return
			}
			err := src.DeleteShard(s, new.Num)
			if err == rpc.OK || err == rpc.ErrWrongGroup {
				break
			}
		}
	}

	for {
		if sck.shouldAbortChange(new) {
			return
		}
		// get version
		_, ver, err := sck.IKVClerk.Get(cfgCurrentKey)
		if err != rpc.OK {
			continue
		}
		err = sck.IKVClerk.Put(cfgCurrentKey, new.String(), ver)
		if err == rpc.OK {
			break
		}
	}
	sck.releaseLease()
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

// try to acquire leader
func (sck *ShardCtrler) tryAcquireLease() bool {
	now := time.Now().UnixNano()
	leaseEnd := now + int64(LeaseTimeout)

	lease := ControllerLease{
		ID:       sck.controllerID,
		LeaseEnd: leaseEnd,
	}
	// serialize to json
	leaseStr := lease.String()

	// if lease doesn't exist or is empty (released)
	val, ver, err := sck.IKVClerk.Get(cfgLeaseKey)
	if err == rpc.ErrNoKey || val == "" {
		// try to acquire the lease
		v := ver
		if err == rpc.ErrNoKey {
			v = 0
		}
		if sck.IKVClerk.Put(cfgLeaseKey, leaseStr, v) == rpc.OK {
			sck.isLeader = true
			sck.leaseExpiry = leaseEnd
			//log.Printf("ShardCtrler %s acquired lease", sck.controllerID)
			return true
		}
		return false
	}

	if err != rpc.OK {
		return false
	}

	// lease exists and not empty, check if is expired or owned
	curLease := fromString(val)

	// if it's owned by itself, renew lease and return
	if curLease.ID == sck.controllerID {
		sck.isLeader = true
		sck.leaseExpiry = leaseEnd
		return true
	}

	if now >= curLease.LeaseEnd {
		// expired
		if sck.IKVClerk.Put(cfgLeaseKey, leaseStr, ver) == rpc.OK {
			sck.isLeader = true
			sck.leaseExpiry = leaseEnd
			//log.Printf("ShardCtrler %s acquired lease", sck.controllerID)
			return true
		}
	}

	// Otherwise, return false for failing to acquire lease
	return false
}

// check itself if is still leader
func (sck *ShardCtrler) isStillLeader() bool {
	if !sck.isLeader {
		return false
	}

	now := time.Now().UnixNano()
	if now < sck.leaseExpiry {
		return true
	}

	// local lease expired, RPC check from kvsrv
	val, _, err := sck.IKVClerk.Get(cfgLeaseKey)
	if err != rpc.OK {
		sck.isLeader = false
		return false
	}

	curLease := fromString(val)
	if curLease.ID != sck.controllerID || now >= curLease.LeaseEnd {
		sck.isLeader = false
		return false
	}

	sck.isLeader = true
	return true
}

// renew its lease
func (sck *ShardCtrler) renewLease() bool {
	if !sck.isStillLeader() {
		return false
	}
	now := time.Now().UnixNano()
	leaseEnd := now + int64(LeaseTimeout)
	lease := ControllerLease{
		ID:       sck.controllerID,
		LeaseEnd: leaseEnd,
	}
	leaseStr := lease.String()

	val, ver, err := sck.IKVClerk.Get(cfgLeaseKey)
	if err != rpc.OK {
		sck.isLeader = false
		return false
	}

	curLease := fromString(val)
	// check if it's still its lease
	if curLease.ID != sck.controllerID {
		sck.isLeader = false
		return false
	}

	// renew the lease
	if sck.IKVClerk.Put(cfgLeaseKey, leaseStr, ver) == rpc.OK {
		sck.isLeader = true
		sck.leaseExpiry = leaseEnd
		//log.Printf("ShardCtrler %s renewed lease", sck.controllerID)
		return true
	}

	return false
}

// release lease
func (sck *ShardCtrler) releaseLease() {
	if !sck.isLeader {
		return
	}

	val, ver, err := sck.IKVClerk.Get(cfgLeaseKey)
	if err != rpc.OK {
		return
	}

	curLease := fromString(val)
	// don't release other's lease
	if curLease.ID != sck.controllerID {
		return
	}

	sck.IKVClerk.Put(cfgLeaseKey, "", ver)
	sck.isLeader = false
	//log.Printf("ShardCtrler %s released lease", sck.controllerID)
}

type ControllerLease struct {
	ID       string `json:"id"`
	LeaseEnd int64  `json:"lease_end"`
}

func (lease *ControllerLease) String() string {
	b, _ := json.Marshal(lease)
	return string(b)
}

func fromString(s string) *ControllerLease {
	var lease ControllerLease
	if err := json.Unmarshal([]byte(s), &lease); err != nil {
		return &ControllerLease{}
	}
	return &lease
}
