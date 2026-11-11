package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	mu sync.Mutex

	// Your definitions here.
	// store value
	values_ map[string]string
	// store value version
	versions_ map[string]rpc.Tversion
}

type SnapshotState struct {
	Values   map[string]string
	Versions map[string]rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch v := req.(type) {
	case rpc.GetArgs:
		// Handle Get request
		var rep rpc.GetReply
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if val, ok := kv.values_[v.Key]; ok {
			if version, ok := kv.versions_[v.Key]; ok {
				rep.Value = val
				rep.Version = version
				rep.Err = rpc.OK
				return rep
			}
			panic("find value of key " + v.Key + " but not find version")
		}
		rep.Err = rpc.ErrNoKey
		return rep
	case rpc.PutArgs:
		// Handle Put request
		var rep rpc.PutReply
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// check if key exists
		if _, ok := kv.values_[v.Key]; ok {
			// check version
			if version, ok := kv.versions_[v.Key]; ok {
				if version == v.Version {
					kv.values_[v.Key] = v.Value
					kv.versions_[v.Key] = version + 1
					rep.Err = rpc.OK
					return rep
				} else {
					rep.Err = rpc.ErrVersion
					return rep
				}
			}

			panic("find value of key " + v.Key + " but not find version")
		}

		// new key
		if v.Version != 0 {
			rep.Err = rpc.ErrNoKey
			return rep
		}
		kv.values_[v.Key] = v.Value
		kv.versions_[v.Key] = 1
		rep.Err = rpc.OK
		return rep
	default:
		panic("unknown request type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return kv.GenerateSnapshot()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) == 0 {
		return
	}

	var snapshot SnapshotState
	dc := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dc.Decode(&snapshot); err != nil {
		panic("failed to decode snapshot: " + err.Error())
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.values_ = snapshot.Values
	kv.versions_ = snapshot.Versions
}

func (kv *KVServer) GenerateSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := SnapshotState{
		Values:   make(map[string]string),
		Versions: make(map[string]rpc.Tversion),
	}

	for k, v := range kv.values_ {
		snapshot.Values[k] = v
	}

	for k, v := range kv.versions_ {
		snapshot.Versions[k] = v
	}

	var buf bytes.Buffer
	ec := labgob.NewEncoder(&buf)
	ec.Encode(snapshot)
	return buf.Bytes()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	
	reply.Value = rep.(rpc.GetReply).Value
	reply.Version = rep.(rpc.GetReply).Version
	reply.Err = rep.(rpc.GetReply).Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	
	reply.Err = rep.(rpc.PutReply).Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,
		values_: make(map[string]string),
		versions_: make(map[string]rpc.Tversion),
	}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
