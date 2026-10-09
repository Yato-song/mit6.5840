package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"
	"log"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state RaftState
	applyCh chan raftapi.ApplyMsg
		
	replicate  []chan struct{} // 唤醒每个 follower 的复制协程

	// heartbeat timer
	lastHeratBeat time.Time
	lastSendHeartBeat time.Time
	heartBeatInterval time.Duration
	// 选举&&心跳超时时长
	electionTimeout time.Duration

	// 状态机需要的条件变量
	cond sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.state.GetCurrentTerm())
	if rf.state.GetRole() == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	if rf.state.GetRole() != Leader {
		// 非leader 返回false
		rf.mu.Unlock()
		return index, term, false
	}

	// leader
	term = int(rf.state.GetCurrentTerm())
	new_log := LogEntry{
		Term: rf.state.GetCurrentTerm(),
		Command: command,
	}
	rf.state.AppendLogEntry(new_log)
	index = rf.state.GetLastLogIndex()
	rf.state.SetMatchIndex(rf.me, index)
	// todo: 持久化

	// 触发日志同步
	for idx := range rf.peers {
		if idx != rf.me {
			rf.trigger(idx)
		}
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.CheckElection()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}


func (rf *Raft) CheckElection() {
	rf.mu.Lock()

	if rf.state.GetRole() == Leader {
		if time.Since(rf.lastSendHeartBeat) < rf.heartBeatInterval {
			// 心跳包间隔未过
			rf.mu.Unlock()
			return
		}

		// 超过心跳间隔，需要发送心跳包
		rf.lastSendHeartBeat = time.Now()
		rf.mu.Unlock()

		// 并发向follower发送心跳
		for idx := range rf.peers {
			if idx == rf.me {
				// 排除自己
				continue
			}

			// 通过trigger函数触发心跳
			rf.trigger(idx)
		}

		return
	} else if rf.state.GetRole() == Candidate && time.Since(rf.lastHeratBeat) < rf.electionTimeout {
		// 选举中：不需要做任何事
		rf.mu.Unlock()
		return 
	} else if rf.state.GetRole() == Follower && time.Since(rf.lastHeratBeat) < rf.electionTimeout {
		// follower心跳未超时：不需要做任何事
		rf.mu.Unlock()
		return
	} 

	// 执行到此处对应两种情况：
	// 1. Follower && 心跳超时: 需要开启选举
	// 2. Candidate && 选举超时: 需要重新进行选举
	rf.lastHeratBeat = time.Now()
	rf.state.SetCurrentTerm(rf.state.GetCurrentTerm() + 1)
	rf.state.SetVotedFor(ServerID(rf.me))
	// todo: 持久化
	rf.state.SetRole(Candidate)
	// 初始值为1，表示投票给自己
	vote_num := 1
	election_term := rf.state.GetCurrentTerm()
	req := &RequestVoteArgs{
		Term: election_term,
		CandidateId: ServerID(rf.me),
		LastLogTerm: rf.state.GetLastLogTerm(),
		LastLogIndex: rf.state.GetLastLogIndex(),
	}
	
	log.Printf("[node=%v, term=%v, currentRole=%v] 发起选举", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole())
	rf.mu.Unlock()
	
	// 并发向其他节点请求选票
	for idx, _ := range rf.peers {
		if(idx == rf.me) {
			// 排除自己
			continue
		}
		// 
		go func(idx int) {
			var rep RequestVoteReply
			if !rf.sendRequestVote(idx, req, &rep) {
				// Request Vote RPC超时
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Request Vote RPC returned
			if rep.Term > rf.state.GetCurrentTerm() {
				// 自己的term已过时：更新自己的term，并退回follower角色
				rf.state.SetCurrentTerm(rep.Term)
				rf.state.SetRole(Follower)
				rf.state.SetVotedFor(-1)
				// todo: 持久化
				return
			}

			if rf.state.GetRole() != Candidate || election_term != rf.state.GetCurrentTerm() {
				// 等待RPC期间，当前节点的角色，任期发生了改变：忽略此次RPC调用
				return
			}

			if election_term == rep.Term && rep.VoteGranted {
				// 成功获取了选票
				vote_num++;
				if vote_num > len(rf.peers)/2 {
					// 选票过半，可以当选为leader
					rf.state.InitAsLeader(rf.me)
					log.Printf("[node=%v, term=%v, currentRole=%v] 当选为leader", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole())
				}
			}

		} (idx)
	}
}


// log entry同步的触发函数。把一个空结构体放入对应channel内，通知协程进行日志同步的操作
// 入参：需要触发同步的raft节点的下标
func (rf *Raft) trigger(peer int) {
	select {
	case rf.replicate[peer] <- struct{}{}:
	default:
		// 已存在待处理复制请求，合并忽略
		// do nothing here
	}
}

// 日志同步协程执行该函数进行日志同步
// 入参：需要触发同步的raft节点的下标
// 思路：
// 1. 从channel中取出一个信号
// 2. 加锁，准备同步请求
// 3. 解锁，发送AppendLogRPC
// 4. 检查RPC调用结果：
// 	4.1 调用超时：等待下次同步触发
//  4.2 调用成功，但返回false：加锁 调整请求参数立刻重新请求
func (rf *Raft) replicator(peer int) {
	for {
		<- rf.replicate[peer]
		
		for {
			rf.mu.Lock()

			if rf.state.GetRole() != Leader {
				// 非leader 什么也不做
				rf.mu.Unlock()
				break
			}

			next := rf.state.GetNextId(peer)
			pre := next - 1
			term := rf.state.GetCurrentTerm()
			req := &AppendEntriesArgs{
				Term: term,
				LeaderId: ServerID(rf.me),
				PrevLogIndex: pre,
				PrevLogTerm: rf.state.GetLogEntry(pre).Term,
				Entries: rf.state.CopyLogFrom(next),
				LeaderCommit: rf.state.GetCommitIndex(),
			}
			// 记录追加日志最后一个entry的index
			last_idx := pre + len(req.Entries)
			rf.mu.Unlock()

			// 发送同步请求
			var rep AppendEntriesReply
			if !rf.sendAppendEntries(peer, req, &rep) {
				// 调用超时：等待下次心跳或者同步到来时重新同步
				break
			}

			
			rf.mu.Lock()

			// 先判断term
			if rep.Term > rf.state.GetCurrentTerm() {
				// 自己的term已过时：更新自己的term，并退回follower角色
				rf.state.SetCurrentTerm(rep.Term)
				rf.state.SetRole(Follower)
				rf.state.SetVotedFor(-1)
				// todo: 持久化
				
				rf.mu.Unlock()
				break
			}

			// 判断是否是失效的rpc调用
			if rf.state.GetRole() != Leader || rf.state.GetCurrentTerm() != term {
				// 失效rpc响应，忽略，不进行任何操作
				rf.mu.Unlock()
				break
			}

			// 调用成功，且返回true
			if rep.Success {
				// 更新nextIndex 和 matchIndex
				if rf.state.GetMatchIndex(peer) < last_idx {
					rf.state.SetMatchIndex(peer, last_idx)
				}
				rf.state.SetNextId(peer, rf.state.GetMatchIndex(peer) + 1)

				// 更新commitIndex
				rf.state.UpdateCommitIndex()
				rf.cond.Signal()

				rf.mu.Unlock()
				break;
			}

			// 至此 表示PrevLogIndex && PrevLogTerm不匹配，调整重试
			rf.state.SetNextId(peer, next - 1)
			rf.mu.Unlock()
			continue
		}
	}
}

// 将已提交log应用到上层状态机的后台协程
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		// 已commit的log和已apply的log进度一致，阻塞等待
		for rf.state.GetLastApplied() >= rf.state.GetCommitIndex() {
			rf.cond.Wait()
		}

		index := rf.state.GetLastApplied() + 1
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command: rf.state.GetLogEntry(index).Command,
			CommandIndex: index,
		}
		rf.mu.Unlock()

		// 上层接收可能阻塞，不能持有 Raft 锁。
		rf.applyCh <- msg

		rf.mu.Lock()
		rf.state.lastApplied = index
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.cond = *sync.NewCond(&rf.mu)
	rf.lastHeratBeat = time.Now()
	rf.lastSendHeartBeat = time.Now()
	rf.heartBeatInterval = 100 * time.Millisecond
	rf.electionTimeout = time.Duration(600 + rand.Intn(400)) * time.Millisecond
	rf.state = RaftState{
		currentTerm: TermNumber(0),
		votedFor: -1,
		logs: make([]LogEntry, 1),
		role: Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.replicate = make([]chan struct{}, len(peers))
	for i := range rf.replicate {
		rf.replicate[i] = make(chan struct{}, 1)
	}

	for i := range rf.replicate {
		// 为每一个节点创建日志同步协程
		go rf.replicator(i)
	}

	go rf.applier()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	
	return rf
}


func (rf *Raft) AsUpToDateAsMe(term TermNumber, idx int) bool {
	res := false
	if term == rf.state.GetLastLogTerm() {
		if idx >= rf.state.GetLastLogIndex() {
			res = true
		}
	} else if term > rf.state.GetLastLogTerm() {
		res = true
	}
	return res
}
