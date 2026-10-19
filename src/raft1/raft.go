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
	"bytes"
	"fmt"

	"6.5840/labgob"
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
	pendingSnapshot bool // 记录是否有待传给上层的snapshot
		
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

	// my code
	buf := new(bytes.Buffer) // *bytes.Buffer
	encoder := labgob.NewEncoder(buf)
	if err := encoder.Encode(rf.state.GetCurrentTerm()); err != nil {
		panic(fmt.Errorf("raft persist currentTerm: %w", err))
	}
	if err := encoder.Encode(rf.state.GetVotedFor()); err != nil {
		panic(fmt.Errorf("raft persist voteFor: %w", err))
	}
	if err := encoder.Encode(rf.state.GetAllLogEntry()); err != nil {
		panic(fmt.Errorf("raft persist logs: %w", err))
	}
	if err := encoder.Encode(rf.state.GetLastIncludeIndex()); err != nil {
		panic(fmt.Errorf("raft persist lastIncludeIndex: %w", err))
	}
	if err := encoder.Encode(rf.state.GetLastIncludeTerm()); err != nil {
		panic(fmt.Errorf("raft persist lastIncludeTerm: %w", err))
	}

	rf.persister.Save(buf.Bytes(), rf.state.GetSnapshotBytes())
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

	// my code
	reader := bytes.NewReader(data)
	decoder := labgob.NewDecoder(reader)

	var term TermNumber
	var vote ServerID
	var logs []LogEntry
	var index int
	var l_term TermNumber

	if err := decoder.Decode(&term); err != nil {
		panic(fmt.Errorf("raft decode currentTerm: %w", err))
	}
	if err := decoder.Decode(&vote); err != nil {
		panic(fmt.Errorf("raft decode voteFor: %w", err))
	}
	if err := decoder.Decode(&logs); err != nil {
		panic(fmt.Errorf("raft decode logs: %w", err))
	}
	if err := decoder.Decode(&index); err != nil {
		panic(fmt.Errorf("raft decode lastIncludeIndex: %w", err))
	}
	if err := decoder.Decode(&l_term); err != nil {
		panic(fmt.Errorf("raft decode lastIncludTerm: %w", err))
	}

	// 全部解码成功
	rf.state.SetCurrentTerm(term)
	rf.state.SetVotedFor(vote)
	rf.state.SetLogEntry(logs)
	rf.state.SetLastIncludeIndex(index)
	rf.state.SetLastIncludeTerm(l_term)
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
// 该接口的作用是 上层应用通知本节点 Raft：“我已经把执行到 index 的状态做成快照了，你可以保存它并裁剪对应日志。”
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.state.GetLastIncludeIndex() {
    	return
	}

	// trim log to index
	term := rf.state.GetLogEntry(index).Term
	rf.state.TruncateLogsTo(index)
	rf.state.SetLastIncludeIndex(index)
	rf.state.SetLastIncludeTerm(term)
	rf.state.SetSnapshotBytes(snapshot)
	
	rf.persist()
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
	// 持久化
	rf.persist()

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

			go rf.sendHeartBeat(idx)

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
	// 持久化
	rf.persist()

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
				// 持久化
				rf.persist()
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


// 发送心跳协程的执行函数。发送心跳和日志同步协程相互独立，防止互相阻塞
func (rf *Raft) sendHeartBeat(peer int) {
	rf.mu.Lock()

	if rf.state.GetRole() != Leader {
		rf.mu.Unlock()
		return
	}

	pre := rf.state.GetNextId(peer) - 1
	pre_log_term := TermNumber(-1)
	// 处理存在snapshot时的边界日志场景
	if pre <= rf.state.GetLastIncludeIndex() {
		// 原前驱已被裁剪，使用快照边界发送空心跳。
		pre = rf.state.GetLastIncludeIndex()
		pre_log_term = rf.state.GetLastIncludeTerm()
	} else {
		pre_log_term = rf.state.GetLogEntry(pre).Term
	}
	req := &AppendEntriesArgs{
		Term: rf.state.GetCurrentTerm(),
		LeaderId: ServerID(rf.me),
		PrevLogIndex: pre,
		PrevLogTerm: pre_log_term,
		Entries: []LogEntry{},
		LeaderCommit: rf.state.GetCommitIndex(),
	}

	rf.mu.Unlock()

	var rep AppendEntriesReply
	if !rf.sendAppendEntries(peer, req, &rep) {
		// 调用超时，返回
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rep.Term > rf.state.GetCurrentTerm() {
        rf.state.SetCurrentTerm(rep.Term)
        rf.state.SetRole(Follower)
        rf.state.SetVotedFor(-1)
        rf.persist()
    }

	// 心跳回复不修改 nextIndex、matchIndex 或 commitIndex
}

// 将已提交log应用到上层状态机的后台协程
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		// 已commit的log和已apply的log进度一致，阻塞等待
		for rf.state.GetLastApplied() >= rf.state.GetCommitIndex() && !rf.pendingSnapshot {
			rf.cond.Wait()
		}

		var msg raftapi.ApplyMsg
		if rf.pendingSnapshot {
			if rf.state.GetLastIncludeIndex() <= rf.state.GetLastApplied() {
				rf.pendingSnapshot = false
				rf.mu.Unlock()
				continue
			}

			msg = raftapi.ApplyMsg{
				CommandValid: false,
				SnapshotValid: true,
				Snapshot: rf.state.GetSnapshotBytes(),
				SnapshotTerm: int(rf.state.GetLastIncludeTerm()),
				SnapshotIndex: rf.state.GetLastIncludeIndex(),
			}
			
			// 推进 lastApplied 和 commitIndex
			if rf.state.GetLastApplied() < rf.state.GetLastIncludeIndex() {
				rf.state.SetLastApplied(rf.state.GetLastIncludeIndex())
			}
			if rf.state.GetLastIncludeIndex() > rf.state.GetCommitIndex() {
				rf.state.SetCommitIndex(rf.state.GetLastIncludeIndex())
			}

			rf.pendingSnapshot = false
		} else {
			index := rf.state.GetLastApplied() + 1
			msg = raftapi.ApplyMsg{
				CommandValid: true,
				Command: rf.state.GetLogEntry(index).Command,
				CommandIndex: index,
				SnapshotValid: false,
			}
			rf.state.SetLastApplied(index)
		}
		rf.mu.Unlock()

		// 上层接收可能阻塞，不能持有 Raft 锁。
		rf.applyCh <- msg
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
	rf.pendingSnapshot = false
	rf.cond = *sync.NewCond(&rf.mu)
	rf.lastHeratBeat = time.Now()
	rf.lastSendHeartBeat = time.Now()
	rf.heartBeatInterval = 100 * time.Millisecond
	rf.electionTimeout = time.Duration(600 + rand.Intn(400)) * time.Millisecond
	rf.state = RaftState{
		currentTerm: TermNumber(0),
		votedFor: -1,
		logs: make([]LogEntry, 1),
		lastIncludedIndex: -1,
		lastIncludeTerm: -1,
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state.SetSnapshotBytes(persister.ReadSnapshot())
	base := rf.state.GetLastIncludeIndex()
	if base < 0 {
		base = 0 // 兼容无快照时边界为 -1 的设计
	}
	rf.state.SetLastApplied(base)
	rf.state.SetCommitIndex(base)

	for i := range rf.replicate {
		// 为每一个节点创建日志同步协程
		go rf.replicator(i)
	}

	go rf.applier()

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
