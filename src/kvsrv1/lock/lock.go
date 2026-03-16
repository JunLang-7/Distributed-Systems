package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	name     string
	identify string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, name: lockname, identify: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		holder, version, err := lk.ck.Get(lk.name)
		switch err {
		case rpc.ErrNoKey:
			// lock is free
			if err := lk.ck.Put(lk.name, lk.identify, 0); err == rpc.OK {
				return
			}
		case rpc.OK:
			switch holder {
			case "":
				// lock is free
				if err := lk.ck.Put(lk.name, lk.identify, version); err == rpc.OK {
					return
				}
			case lk.identify:
				// has acquired the lock
				return
			default:
				// lock is held by other client, wait and retry
				time.Sleep(time.Millisecond * 20)
			}
		default:
			time.Sleep(time.Millisecond * 20)
		}
	}
}

func (lk *Lock) Release() {
	for {
		holder, version, err := lk.ck.Get(lk.name)
		switch err {
		case rpc.ErrNoKey:
			// Already released.
			return
		case rpc.OK:
			if holder != lk.identify {
				// Don't release another client's lock.
				return
			}

			if putErr := lk.ck.Put(lk.name, "", version); putErr == rpc.OK {
				return
			}

			// Put may have succeeded even if reply was lost.
			nextHolder, _, getErr := lk.ck.Get(lk.name)
			if getErr == rpc.ErrNoKey || (getErr == rpc.OK && nextHolder != lk.identify) {
				return
			}
			time.Sleep(time.Millisecond * 20)
		default:
			time.Sleep(time.Millisecond * 20)
		}
	}
}
