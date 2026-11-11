package rsm

import (
	"crypto/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	submitTimeout = 900 * time.Millisecond
)

type Epoch [16]byte

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me    int
	Epoch Epoch // 随机16位字节串
	Id    int
	Req   any
}

type Uid struct {
	Me    int
	Epoch Epoch
	Id    int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	mp    map[Uid]chan interface{}
	id    int
	epoch Epoch // fresh for each RSM instance, including after restart
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		mp:           make(map[Uid]chan interface{}),
		id:           0,
	}
	// Your initialization code here.
	if _, err := rand.Read(rsm.epoch[:]); err != nil {
		panic("rsm: failed to generate epoch: " + err.Error())
	}

	snp := persister.ReadSnapshot()
	if len(snp) > 0 {
		rsm.sm.Restore(snp)
	}


	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// 启动reader协程
	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
/*
1.
*/
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	// assign id
	id := rsm.AssignId()
	op := Op{Me: rsm.me, Epoch: rsm.epoch, Id: id, Req: req}
	uid := Uid{Me: rsm.me, Epoch: rsm.epoch, Id: id}
	ch := make(chan interface{}, 1)
	rsm.mp[uid] = ch
	_, term, isleader := rsm.rf.Start(op)
	rsm.mu.Unlock()

	if !isleader {
		// current server is not leader
		rsm.mu.Lock()
		delete(rsm.mp, uid)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	retry := time.NewTicker(submitTimeout)
	defer retry.Stop()

	for {
		select {
		case res := <-ch:
			// req hsa been applied on majority servers, return result
			rsm.mu.Lock()
			delete(rsm.mp, uid)
			rsm.mu.Unlock()
			return rpc.OK, res
		case <-retry.C:
			// check leadership
			term_now, ld := rsm.rf.GetState()
			// case 1: 不再是leader，或者leader任期发生了改变，返回ErrWrongLeader
			if !ld || term_now != term {
				rsm.mu.Lock()
				delete(rsm.mp, uid)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			// case 2: 仍然是leader，继续等待
			continue
		}
	}
}

// AssignId returns a unique id for each request
// should called in lock
func (rsm *RSM) AssignId() int {
	id := rsm.id
	rsm.id++
	return id
}

/*
每个server创建一个rsm，每个rsm有一个reader协程，该协程负责从raft的applyCh中读区消息并处理
1. 读到Command：解析Op，调用server层的DoOp接口执行对应操作。然后根据op中的me判断自己是否是leader，若是，则需要利用channel去通知Submit协程该请求已经完成，Submit协程可以返回结果给客户端。
2. 读到Snapshot：Todo
*/
func (rsm *RSM) reader() {
	for {
		msg := <-rsm.applyCh
		// command 场景
		if msg.CommandValid {
			op := msg.Command.(Op)
			uid := Uid{Me: op.Me, Epoch: op.Epoch, Id: op.Id}

			// 调用DoOp
			res := rsm.sm.DoOp(op.Req)

			// 判断是否需要生成snapshot
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate * 9 / 10 {
				snp := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snp)
			}

			if op.Me != rsm.me {
				// follower 直接返回
				continue
			}

			// leader
			rsm.mu.Lock()
			ch, ok := rsm.mp[uid]
			if !ok {
				// Submit请求超时，已清理channel，直接返回
				rsm.mu.Unlock()
				continue
			}
			ch <- res
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			// snapshot 场景
			rsm.sm.Restore(msg.Snapshot)
		}
	}
}
