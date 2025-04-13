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
	ck kvtest.IKVClerk
	// You may add code here
	key string
	id  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		id:  kvtest.RandValue(8),
		key: l,
	}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.key, "unlocked", 0)
			if err == rpc.OK {
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if val != "unlocked" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err = lk.ck.Put(lk.key, lk.id, ver)
		if err == rpc.OK {
			return
		} else if err == rpc.ErrVersion {
			continue
		} else if err == rpc.ErrMaybe {
			currentVal, _, checkErr := lk.ck.Get(lk.key)
			if checkErr == rpc.OK && currentVal == lk.id {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if val != lk.id {
			return
		}

		err = lk.ck.Put(lk.key, "unlocked", ver)
		if err == rpc.OK {
			return
		} else if err == rpc.ErrVersion {
			continue
		} else if err == rpc.ErrMaybe {
			currentVal, _, checkErr := lk.ck.Get(lk.key)
			if checkErr == rpc.OK && currentVal == "unlocked" {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
