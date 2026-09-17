package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname_ string
	clientID  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname_ = lockname
	lk.clientID = kvtest.RandValue(8)
	return lk
}

// key 不存在或 value 为空表示锁空闲，使用条件 Put 写入自己的客户端 ID。
// 如果 Put 返回 ErrMaybe，重新 Get 确认持有者是否为自己。
func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockname_)
		if err == rpc.OK && value != "" {
			continue
		}

		putErr := lk.ck.Put(lk.lockname_, lk.clientID, version)
		if putErr == rpc.OK {
			return
		}
		if putErr == rpc.ErrMaybe {
			value, _, getErr := lk.ck.Get(lk.lockname_)
			if getErr == rpc.OK && value == lk.clientID {
				return
			}
		}
	}
}

// 使用条件 Put 将自己持有的锁清空；结果不确定时重新检查持有者。
func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.lockname_)
		if err == rpc.ErrNoKey || (err == rpc.OK && value != lk.clientID) {
			return
		}
		if err != rpc.OK {
			continue
		}

		putErr := lk.ck.Put(lk.lockname_, "", version)
		if putErr == rpc.OK {
			return
		}
		if putErr == rpc.ErrMaybe {
			value, _, getErr := lk.ck.Get(lk.lockname_)
			if getErr == rpc.OK && value != lk.clientID {
				return
			}
		}
	}
}
