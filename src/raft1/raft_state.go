package raft

import (
	"log"
	"sort"
)

type TermNumber int
type ServerID int
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type RaftState struct {
	// persisitent state on all servers
	currentTerm TermNumber
	votedFor    ServerID
	logs        []LogEntry
	lastIncludedIndex int // 快照中包含的最后一个log entry的index
	lastIncludeTerm TermNumber
	snapshot []byte
	// volatile state on all servers
	role Role
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

func (rs *RaftState) GetCurrentTerm() TermNumber {
	return rs.currentTerm
}

func (rs *RaftState) SetCurrentTerm(term TermNumber) {
	rs.currentTerm = term
}

func (rs *RaftState) GetVotedFor() ServerID {
	return rs.votedFor
}

func (rs *RaftState) SetVotedFor(candidateId ServerID) {
	rs.votedFor = candidateId
}

func (rs *RaftState) GetLogEntry(idx int) LogEntry {
	return rs.logs[idx - rs.lastIncludedIndex - 1]
}

func (rs *RaftState) GetAllLogEntry() []LogEntry {
	return rs.logs
}

func (rs *RaftState) SetLogEntry(logs []LogEntry) {
	rs.logs = logs
}

func (rs *RaftState) GetLogLength() int {
	return len(rs.logs)
}

// 从下标idx开始（包括idx）丢弃后面的日志
func (rs *RaftState) TruncateLogsFrom(idx int) {
	if idx - rs.lastIncludedIndex - 1 >= len(rs.logs) {
		return
	}

	rs.logs = rs.logs[:idx - rs.lastIncludedIndex - 1]
}

// 丢弃下标小于等于idx的日志
func (rs *RaftState) TruncateLogsTo(idx int) {
	if idx - rs.lastIncludedIndex - 1 >= len(rs.logs) {
		return
	}
	rs.logs = append([]LogEntry(nil), rs.logs[idx-rs.lastIncludedIndex:]...)
}

func (rs *RaftState) ClearLogs() {
	rs.logs = nil
}

func (rs *RaftState) AppendLogEntry(l LogEntry) {
	rs.logs = append(rs.logs, l)
}

func (rs *RaftState) GetLastLogIndex() int {
	return len(rs.logs) + rs.lastIncludedIndex
}

func (rs *RaftState) GetLastLogTerm() TermNumber {
	last_log_idx := rs.GetLastLogIndex()
	if last_log_idx <= rs.GetLastIncludeIndex() {
		return rs.GetLastIncludeTerm()
	}

	entry := rs.GetLogEntry(last_log_idx)

	return entry.GetTerm()
}

// 复制logs从idx下标开始的元素
func (rs *RaftState) CopyLogFrom(idx int) []LogEntry {
	if idx - rs.lastIncludedIndex - 1 >= len(rs.logs) {
		// idx 大于等于logs的长度，返回空切片
		return []LogEntry{}
	}
	entries := append([]LogEntry(nil), rs.logs[idx - rs.lastIncludedIndex - 1:]...)
	return entries
}


func (rs *RaftState) GetRole() Role {
	return rs.role
}

func (rs *RaftState) SetRole(role Role) {
	rs.role = role
}

func (rs *RaftState) GetCommitIndex() int {
	return rs.commitIndex
}

func (rs *RaftState) SetCommitIndex(idx int) {
	rs.commitIndex = idx
}

func (rs *RaftState) GetNextId(idx int) int {
	if idx >= len(rs.nextIndex) {
		log.Printf("peer idx(%v) >= peer number(%v)", idx, len(rs.nextIndex))
		panic("idx error")
	}

	return rs.nextIndex[idx]
}

func (rs *RaftState) SetNextId(idx int, value int) {
	if idx >= len(rs.nextIndex) {
		log.Printf("peer idx(%v) >= peer number(%v)", idx, len(rs.nextIndex))
		panic("idx error")
	}

	rs.nextIndex[idx] = value
}

func (rs *RaftState) GetMatchIndex(idx int) int {
	return rs.matchIndex[idx]
}

func (rs *RaftState) SetMatchIndex(idx int, value int) {
	rs.matchIndex[idx] = value
}

// 角色转换为leader后需要做的初始化操作
// idx为leader对应的下标
func (rs *RaftState) InitAsLeader(idx int) {
	// role
	rs.SetRole(Leader)
	
	// nextIndex
	for i, _ := range rs.nextIndex {
		rs.nextIndex[i] = rs.GetLastLogIndex() + 1
	}

	// matchIndex
	for i, _ := range rs.matchIndex {
		if i == idx {
			rs.matchIndex[i] = rs.GetLastLogIndex()
		} else {
			rs.matchIndex[i] = 0
		}
	}
}

// 根据matchIndex计算commitIndex
func (rs *RaftState) UpdateCommitIndex() {
	tmp_s := append([]int(nil), rs.matchIndex...)
	sort.Slice(tmp_s, func(i, j int) bool {
		return tmp_s[i] > tmp_s[j]
	})

	idx := len(tmp_s) / 2
	match_idx := tmp_s[idx]

	// 这里找到的log entry的term必须和leader的term一致，才能判定为已提交
	if rs.commitIndex < match_idx && rs.GetLogEntry(match_idx).Term == rs.GetCurrentTerm() {
		rs.commitIndex = match_idx
	}
}

func (rs *RaftState) GetLastApplied() int {
	return rs.lastApplied
}

func (rs *RaftState) SetLastApplied(idx int) {
	rs.lastApplied = idx
}

func (rs *RaftState) GetLastIncludeIndex() int {
	return rs.lastIncludedIndex
}

func (rs *RaftState) SetLastIncludeIndex(idx int) {
	rs.lastIncludedIndex = idx
}

func (rs *RaftState) GetLastIncludeTerm() TermNumber {
	return rs.lastIncludeTerm
}

func (rs *RaftState) SetLastIncludeTerm(term TermNumber) {
	rs.lastIncludeTerm = term
}

func (rs *RaftState) GetSnapshotBytes() []byte {
	return rs.snapshot
}

func (rs *RaftState) SetSnapshotBytes(bytes []byte) {
	rs.snapshot = bytes
}