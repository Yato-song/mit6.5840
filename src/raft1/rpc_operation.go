package raft

import (
	"log"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term TermNumber
	CandidateId ServerID
	LastLogIndex int
	LastLogTerm TermNumber
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term TermNumber
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term TermNumber
	LeaderId ServerID
	PrevLogIndex int
	PrevLogTerm TermNumber
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term TermNumber
	Success bool
	ConflictType int
	MaxIndex int
	ConflictTerm TermNumber
	ConflictTermFirstIndex int
}

type InstallSnapshotArgs struct {
	Term TermNumber // leader's term
	LeaderID ServerID
	LastIncludeIndex int
	LastIncludeTerm TermNumber
	Data []byte // raw bytes of snapshot
}

type InstallSnapshotReply struct {
	Term TermNumber
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC handler.
// 该函数用于处理其他peer的RequestVote请求
/* 规则：
1. 请求中的term 不得小于当前节点的 currentTerm
2. 当前节点的voteFor 为空，或者等于请求中的candidateId，且请求节点的日志至少和当前节点的日志一样新，则可以grant vote
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.state.GetCurrentTerm() {
		rf.state.SetCurrentTerm(args.Term)
		rf.state.SetRole(Follower)
		rf.state.SetVotedFor(-1)
		// 持久化
		rf.persist()
	}

	if args.Term < rf.state.GetCurrentTerm()  {
		reply.Term = rf.state.GetCurrentTerm()
		reply.VoteGranted = false
		log.Printf("[node=%v, term=%v, currentRole=%v] 拒绝投票: 请求term(%v)小于当前节点 term", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole(), args.Term)
		return
	}

	if rf.state.GetVotedFor() != ServerID(-1) && rf.state.GetVotedFor() != args.CandidateId {
		reply.Term = rf.state.GetCurrentTerm()
		reply.VoteGranted = false
		log.Printf("[node=%v, term=%v, currentRole=%v] 拒绝投票: 已投票(%v)且并非投给请求节点", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole(), rf.state.GetVotedFor())
		return
	}

	// check if candidate's log is at least as up-to-date as receiver's log
	if !rf.AsUpToDateAsMe(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.state.GetCurrentTerm()
		reply.VoteGranted = false
		log.Printf("[node=%v, term=%v, currentRole=%v] 拒绝投票: 候选人日志没有当前节点新", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole())
		return
	}

	// grant vote

	// Figure2 Rules for followers, second rule: 投票完要重置election timeout计时
	rf.lastHeratBeat = time.Now()

	rf.state.SetVotedFor(args.CandidateId)
	// 持久化
	rf.persist()

	reply.Term = rf.state.GetCurrentTerm()
	reply.VoteGranted = true
	log.Printf("[node=%v, term=%v, currentRole=%v] 投票成功, 选票投给node%v", rf.me, rf.state.GetCurrentTerm(), rf.state.GetRole(), args.CandidateId)
	return
}

// 该函数用于处理来自leader的日志追加请求
/*
step1: 任期检查
	- args.Term < currentTerm：返回当前任期，直接拒绝，不重置计时。
	- args.Term > currentTerm：更新任期、清空 votedFor，转为 Follower。
	- args.Term == currentTerm：如果自己是 Candidate，也转为 Follower，但不要清空 votedFor。
step2: 重置选举计时
step3: 检查RPC中的PrevLogIndex && PrevLogTerm 对应的log entry时否则在当前节点中存在（即leader在尝试找到和follower达成一致的某个entry）
	检查index和term是否全部相同。若不相同，拒绝；相同，可以继续step4
step4: 合并日志：逐条比较RPC中包含的log entry和当前节点PrevLogIndex && PrevLogTerm 后的log entry
	- 本地存在且任期相同：保留，继续比较。
	- 本地存在但任期不同：删除本地从该位置开始的所有日志，追加请求中从该位置开始的剩余日志。
	- 本地不存在：直接追加请求中剩余的日志。
step5: 更新当前节点commitIndex
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// step1 任期检查
	if args.Term < rf.state.GetCurrentTerm() {
		reply.Term = rf.state.GetCurrentTerm()
		reply.Success = false
		return
	}

	if args.Term > rf.state.GetCurrentTerm() {
		rf.state.SetCurrentTerm(args.Term)
		rf.state.SetRole(Follower)
		rf.state.SetVotedFor(-1)

		// 持久化
		rf.persist()
	}

	if args.Term == rf.state.GetCurrentTerm() && rf.state.GetRole() == Candidate {
		rf.state.SetRole(Follower)
	}

	// step2 重置选举计时
	rf.lastHeratBeat = time.Now()

	// step3 PrevLogIndex && PrevLogTerm检查
	/*
		case1: PrevLogIndex位于快照边界前
			a) 来自leader的log entry完全被本地快照包含：直接返回success即可
			b) 来自leader的log entry跨过快照边界：裁减请求中的log entries，把边界后的log进行合并
		case2: PrevLogIndex等于快照边界lastIncludeIndex
				合并log
		case3: PrevLogIndex位于快照边界后的日志中
				判断PrevLogIndex && PrevLogTerm的合法性
	*/
	idx := args.PrevLogIndex
	last_log_idx := args.PrevLogIndex + len(args.Entries)
	if idx < rf.state.GetLastIncludeIndex() {
		last_idx := idx + len(args.Entries)
		// case a
		if last_idx <= rf.state.GetLastIncludeIndex() {
			reply.Term = rf.state.GetCurrentTerm()
			reply.Success = true
			return
		} else {
			// 裁减冗余日志 等待后续合并
			act_begin_idx := rf.state.GetLastIncludeIndex() - args.PrevLogIndex 
			args.Entries = append([]LogEntry(nil), args.Entries[act_begin_idx:]...)
			idx = rf.state.GetLastIncludeIndex()
		}
	} else if idx == rf.state.GetLastIncludeIndex() {
		if rf.state.GetLastIncludeTerm() != args.PrevLogTerm {
			// 异常场景
			log.Printf(
				"[FATAL] snapshot boundary mismatch: node=%d currentTerm=%d "+
					"leader=%d requestTerm=%d index=%d "+
					"localLogTerm=%d requestLogTerm=%d",
				rf.me, rf.state.GetCurrentTerm(),
				args.LeaderId, args.Term, args.PrevLogIndex,
				rf.state.GetLastIncludeTerm(), args.PrevLogTerm,
			)
			panic("snapshot boundary mismatch")
		}
		// 合法PrevLogIndex && PrevLogTerm，等待日志合并
	} else {
		if idx > rf.state.GetLastLogIndex() {
			// 当前节点不存在 idx = PrevLogIndex 的entry
			reply.Term = rf.state.GetCurrentTerm()
			reply.Success = false
			reply.ConflictType = 1
			reply.MaxIndex = rf.state.GetLastLogIndex()
			return
		}
		log_entry := rf.state.GetLogEntry(idx)
		if log_entry.GetTerm() != args.PrevLogTerm {
			// 存在 idx = PrevLogIndex 的entry, 但term对应不上
			reply.Term = rf.state.GetCurrentTerm()
			reply.Success = false
			reply.ConflictType = 2
			reply.ConflictTerm = log_entry.GetTerm()
			// 计算冲突term的第一个log entry的index
			first := idx
			for first > rf.state.GetLastIncludeIndex()+1 &&
				rf.state.GetLogEntry(first-1).Term == log_entry.Term {
				first--
			}
			reply.ConflictTermFirstIndex = first
			return
		}
	}

	// step4 合并日志
	idx++
	// 这里 不可以 直接删除PrevLogIndex && PrevLogTerm 后的entry然后直接把args中的entry直接追加到当前节点的日志后面
	// 因为网络可能让先发出的appendEntriesRPC后到达，如果采用删除追加，则会让后发出的更新更多的entry被后到达的RPC覆盖删除掉
	for _, l := range args.Entries {
		if idx <= rf.state.GetLastLogIndex() {
			entry := rf.state.GetLogEntry(idx)
			if entry.GetTerm() == l.GetTerm() {
				// case1 本地存在entry且任期相同
				idx++;
				continue
			} else {
				// case2 本地存在但任期不同
				rf.state.TruncateLogsFrom(idx)
				rf.state.AppendLogEntry(l)
				idx++
			}
		} else {
			// case3 本地不存在
			rf.state.AppendLogEntry(l)
			idx++
		}
	}
	// 持久化
	rf.persist()

	// step5 更新当前节点commitIndex
	newCommit := min_int(
		args.LeaderCommit,
		last_log_idx,
	)
	if newCommit > rf.state.GetCommitIndex() {
		rf.state.SetCommitIndex(newCommit)
	}
	rf.cond.Signal()

	reply.Term = rf.state.GetCurrentTerm()
	reply.Success = true
	return
}

// 该函数用于处理来自leader的快照更新请求
/*
step1: 先判断term是否合理
step2: 
       裁减日志
	   更新lastIncludedIndex && lastIncludedTerm && snapshot
	- case1: 节点不存在index为LastIncludeIndex的log entry。这表示快照比当前节点的最新日志还新，此时直接丢弃本地日志，并更新snapshot
	- case2: 节点存在index为LastIncludeIndex的log entry， 但这个entry的Term和LastIncludeTerm不一致。 这表示此处的entry和leader发生了
			 不一致，也就表示此entry之后的所有entry也不可能与leader一致。所以直接丢弃所有日志，更新snapshot
	- case3: 节点存在index为LastIncludeIndex的log entry， 且Term一致。此时裁减包含该entry在内的之前的日志，保留之后的日志，更新snapshot
step3: 持久化
step4: 应用快照到状态机
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// step1
	if args.Term < rf.state.GetCurrentTerm() {
		reply.Term = rf.state.GetCurrentTerm()
		// reply.Success = false
		return
	}

	if args.Term > rf.state.GetCurrentTerm() {
		rf.state.SetCurrentTerm(args.Term)
		rf.state.SetRole(Follower)
		rf.state.SetVotedFor(-1)

		// 持久化
		rf.persist()
	}

	if args.Term == rf.state.GetCurrentTerm() && rf.state.GetRole() == Candidate {
		rf.state.SetRole(Follower)
	}
	// 重置选举超时时间
	rf.lastHeratBeat = time.Now()

	// step2
	if args.LastIncludeIndex <= rf.state.GetLastIncludeIndex() {
		// 快照不够新，忽略
		reply.Term = rf.state.GetCurrentTerm()
		return
	}
	if args.LastIncludeIndex <= rf.state.GetLastLogIndex() && args.LastIncludeTerm == rf.state.GetLogEntry(args.LastIncludeIndex).Term {
		// 对应case3
		rf.state.TruncateLogsTo(args.LastIncludeIndex)
	} else {
		// 对应case1 && 2
		rf.state.ClearLogs()
	}
	rf.state.SetSnapshotBytes(args.Data)
	rf.state.SetLastIncludeIndex(args.LastIncludeIndex)
	rf.state.SetLastIncludeTerm(args.LastIncludeTerm)

	// step3
	rf.persist()

	// step4
	// 通过applier进行状态机同步
	rf.pendingSnapshot = true
	rf.cond.Signal()

	reply.Term = rf.state.GetCurrentTerm()
}

// support function
func min_int(a int, b int) int {
	if a < b {
		return a
	}
	return b
}