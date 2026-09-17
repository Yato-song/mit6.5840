package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// store value
	values_ map[string]string
	// store value version
	versions_ map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	// init KV Server
	kv.values_ = make(map[string]string)
	kv.versions_ = make(map[string]rpc.Tversion)
	
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.

	// lock
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if key exists
	if val, ok := kv.values_[args.Key]; ok {
		if version, ok := kv.versions_[args.Key]; ok {
			reply.Value = val
			reply.Version = version
			reply.Err = rpc.OK

			return
		}
		panic("find value of key " + args.Key + " but not find version")
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.

	// lock
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if key exists
	if _, ok := kv.values_[args.Key]; ok {
		// check version
		if version, ok := kv.versions_[args.Key]; ok {
			if version == args.Version {
				kv.values_[args.Key] = args.Value
				kv.versions_[args.Key] = version + 1
				reply.Err = rpc.OK
				return
			} else {
				reply.Err = rpc.ErrVersion
				return
			}
		}

		panic("find value of key " + args.Key + " but not find version")
	}

	// new key
	if args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}
	kv.values_[args.Key] = args.Value
	kv.versions_[args.Key] = 1
	reply.Err = rpc.OK
}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
