package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const maxRetryRoundsPerGroup = 3

// 超时时间配置
const (
	rpcCallTimeoutShort = 300 * time.Millisecond // Get/Put 用
	rpcCallTimeoutLong  = 1 * time.Second        // Shard 迁移操作用
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) callWithTimeout(server, method string, args any, reply any, timeout time.Duration) bool {
	done := make(chan bool, 1)
	go func() {
		done <- ck.Clnt.Call(server, method, args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for round := 0; ; round++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := new(rpc.GetReply)
			if ok := ck.callWithTimeout(ck.servers[srv], "KVServer.Get", &args, &reply, rpcCallTimeoutShort); ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.leader = srv
				return reply.Value, reply.Version, reply.Err
			}
		}
		if round >= maxRetryRoundsPerGroup {
			return "", 0, rpc.ErrWrongGroup
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	resent := false
	for round := 0; ; round++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := new(rpc.PutReply)
			if ok := ck.callWithTimeout(ck.servers[srv], "KVServer.Put", &args, &reply, rpcCallTimeoutShort); ok {
				if reply.Err == rpc.ErrWrongLeader {
					resent = true
					continue
				}
				ck.leader = srv
				if reply.Err == rpc.ErrVersion && resent {
					return rpc.ErrMaybe
				}
				return reply.Err
			}
			resent = true
		}
		if round >= maxRetryRoundsPerGroup {
			return rpc.ErrWrongGroup
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := new(shardrpc.FreezeShardReply)
			if ok := ck.callWithTimeout(ck.servers[srv], "KVServer.FreezeShard", &args, &reply, rpcCallTimeoutLong); ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.leader = srv
				return reply.State, reply.Err
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, Num: num, State: state}
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := new(shardrpc.InstallShardReply)
			if ok := ck.callWithTimeout(ck.servers[srv], "KVServer.InstallShard", &args, &reply, rpcCallTimeoutLong); ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.leader = srv
				return reply.Err
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.leader + i) % len(ck.servers)
			reply := new(shardrpc.DeleteShardReply)
			if ok := ck.callWithTimeout(ck.servers[srv], "KVServer.DeleteShard", &args, &reply, rpcCallTimeoutLong); ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.leader = srv
				return reply.Err
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}
