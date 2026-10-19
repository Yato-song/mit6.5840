package raft

import "time"

const (
	replicationRetryInterval = 300 * time.Millisecond
	maxReplicationInflight   = 32
)

// replicationResult 保存一次复制请求的上下文和结果，供协调协程处理回复。
type replicationResult struct {
	id            uint64               // 此 peer 的请求编号，用于判断回复是否属于最新发出的请求
	term          TermNumber           // 构造请求时的 leader 任期，用于过滤旧任期的复制结果
	next          int                  // 构造请求时的 nextIndex，用于判断冲突回复是否仍对应当前探测位置
	last          int                  // 本次请求覆盖到的最后索引：日志末尾或快照边界，成功时用于推进 matchIndex
	snapshot      bool                 // true 表示 InstallSnapshot 请求，false 表示 AppendEntries 请求
	ok            bool                 // 底层 RPC 是否收到回复；true 不代表 AppendEntries 的 Success 为 true
	reply         AppendEntriesReply   // 日志复制的回复，仅在 snapshot=false 且 ok=true 时有效
	snapshotReply InstallSnapshotReply // 快照同步的回复，仅在 snapshot=true 且 ok=true 时有效
}

// replicator 为指定 peer 协调日志复制和快照同步，每个 peer 使用一个独立的协调协程。
//
// 设计目的：labrpc.Call 在节点断连后可能等待数秒才返回，即使节点已经恢复连接，
// 旧调用也不会立即结束。因此协调协程不直接等待网络调用，而是让独立 goroutine
// 执行 RPC，再通过 results channel 收集结果，避免旧请求阻塞后续复制尝试。
//
// 调度循环处理三类事件：
//   - trigger 通知：Start 写入新日志或周期性心跳触发复制；通知可以合并，发送时读取最新状态。
//   - retry 定时事件：存在未完成请求时，允许按 replicationRetryInterval 的节奏重新尝试。
//   - RPC 结果：回收一个 inflight 名额，并统一调用 handleReplicationResult 处理回复。
//     最新请求的有效回复若表明还有日志待复制或需要回退探测，可以立即发送下一次请求。
//
// 流量控制：已有请求在途时，普通触发受重试间隔限制；没有在途请求时可以直接发送。
// 每个 peer 最多允许 maxReplicationInflight 个未完成请求。重试不会取消旧 RPC，
// 也不会提前释放名额，只有收到该 RPC 的结果后才减少 inflight，防止无限创建请求。
//
// 每次发送前持有 rf.mu 检查 leader 身份，并根据最新 nextIndex 构造独立请求：
// nextIndex 落在快照覆盖范围内时发送 InstallSnapshot，否则发送 AppendEntries。
// 日志和快照数据在锁内复制，请求记录发送时的任期、起始位置和覆盖到的最后索引，
// 解锁后才启动网络调用。inflight、latest 和 lastSent 仅由本协调协程访问。
//
// 回复可能乱序到达，handleReplicationResult 在锁内先处理更高任期，再过滤角色或
// 请求任期已过期的结果。成功回复只能推进已确认进度；冲突回复只有仍对应当前探测
// 位置且未被已确认进度覆盖时才允许回退，避免旧失败回复破坏新成功回复的结果。
// RPC goroutine 只执行调用并上报结果，不直接修改 Raft 状态。
//
// 执行流程（对应下方代码的顺序）：
//  1. 初始化结果 channel、重试 ticker、在途请求计数 inflight、请求编号 latest
//     和上次发送时间 lastSent，然后进入循环。
//  2. 每轮清空 send 和 immediate，通过 select 等待触发通知、定时事件或 RPC 结果。
//     收到结果时先减少 inflight，再处理回复；处理结果决定是否需要继续复制，
//     只有需要继续复制且回复属于最新发出的请求时，才设置 immediate 跳过间隔限制。
//  3. 若本轮不需要发送，或在途请求已达到上限，则返回步骤 2。
//     若仍有在途请求、距离上次发送不足重试间隔且没有 immediate 标记，也返回步骤 2。
//     这里仅跳过本轮发送，不会取消任何已经发出的 RPC。
//  4. 获取 rf.mu，确认当前仍是 leader；否则解锁并返回步骤 2。
//     为本次请求分配编号，并读取当前任期和目标 peer 最新的 nextIndex。
//  5. 在锁内准备请求：若 nextIndex <= lastIncludedIndex，复制快照数据并构造
//     InstallSnapshot；否则计算前驱索引和任期，复制日志后缀并构造 AppendEntries。
//     同时记录本次请求覆盖到的最后索引，供回复处理时更新复制进度。
//  6. 释放 rf.mu，增加 inflight 并记录 lastSent，启动独立 goroutine 执行 RPC。
//     协调协程立即返回步骤 2，因此旧调用尚未返回时也能处理其他事件。
//  7. RPC goroutine 等待调用真正结束，将调用是否成功及回复写入 results 后退出。
//     协调协程在步骤 2 接收它；即使期间已经重试或发生任期变化，这条结果仍会被处理。
func (rf *Raft) replicator(peer int) {
	// 接收异步 RPC 结果，并定期检查是否需要重试。
	results := make(chan replicationResult, maxReplicationInflight)
	retry := time.NewTicker(replicationRetryInterval)
	defer retry.Stop()
	inflight := 0          // 尚未回收结果的请求数
	var latest uint64      // 最新发出的请求编号
	var lastSent time.Time // 上次发送时间，用于限制重试频率
	for {
		immediate := false
		send := false
		// 等待新工作、重试时机或已完成的请求。
		select {
		case <-rf.replicate[peer]:
			// Start 或心跳通知：尝试发送最新状态。
			send = true
		case <-retry.C:
			// 只重试尚未完成的工作；普通心跳由 trigger 触发。
			send = inflight > 0
		case result := <-results:
			// 调用真正结束才释放名额，并处理回复中的任期和复制进度。
			inflight--
			send = rf.handleReplicationResult(peer, result)
			// 最新请求若需要继续推进，允许跳过重试间隔。
			immediate = send && (result.id == latest)
		}
		// 无需发送或并发名额已满，继续等待事件。
		if !send || inflight >= maxReplicationInflight {
			continue
		}
		// 有请求在途时，避免普通通知触发过于密集的重复请求。
		if !immediate && time.Since(lastSent) < replicationRetryInterval && inflight > 0 {
			continue
		}

		// 持锁确认身份，并读取一致的任期、日志和快照状态。
		rf.mu.Lock()
		if rf.state.GetRole() != Leader {
			rf.mu.Unlock()
			continue
		}
		// 保存本次请求的上下文，供迟到或乱序回复校验使用。
		latest++
		next := rf.state.GetNextId(peer)
		result := replicationResult{id: latest, term: rf.state.GetCurrentTerm(), next: next}
		var args AppendEntriesArgs
		var snap InstallSnapshotArgs
		if next <= rf.state.GetLastIncludeIndex() {
			// 所需日志已被裁剪，改为发送快照，并复制快照字节。
			result.snapshot = true
			result.last = rf.state.GetLastIncludeIndex()
			snap = InstallSnapshotArgs{
				Term: result.term, LeaderID: ServerID(rf.me),
				LastIncludeIndex: result.last, LastIncludeTerm: rf.state.GetLastIncludeTerm(),
				Data: append([]byte(nil), rf.state.GetSnapshotBytes()...),
			}
		} else {
			// 日志仍保留：确定前驱任期，再复制 next 开始的日志后缀。
			prev := next - 1
			prevTerm := rf.state.GetLastIncludeTerm()
			if prev > rf.state.GetLastIncludeIndex() {
				prevTerm = rf.state.GetLogEntry(prev).Term
			}
			args = AppendEntriesArgs{
				Term: result.term, LeaderId: ServerID(rf.me),
				PrevLogIndex: prev, PrevLogTerm: prevTerm,
				Entries: rf.state.CopyLogFrom(next), LeaderCommit: rf.state.GetCommitIndex(),
			}
			result.last = prev + len(args.Entries) // 本次成功最多能确认到的位置
		}
		rf.mu.Unlock()
		// 占用名额并记录时间；网络调用期间不持有 Raft 锁。
		inflight++
		lastSent = time.Now()
		// 独立执行 RPC，协调协程继续处理事件，不等待这个调用。
		go func(r replicationResult, args AppendEntriesArgs, snap InstallSnapshotArgs) {
			if r.snapshot {
				r.ok = rf.sendInstallSnapshot(peer, &snap, &r.snapshotReply)
			} else {
				r.ok = rf.sendAppendEntries(peer, &args, &r.reply)
			}
			results <- r // 成功或失败都上报，由协调协程统一处理
		}(result, args, snap)
	}
}

// handleReplicationResult 统一处理日志或快照 RPC 的结果。
// 返回 true 表示还有日志待复制或探测位置已改变，协调协程可以尝试继续发送；
// 返回 false 表示本次结果不要求继续发送，不代表后续不再重试。
func (rf *Raft) handleReplicationResult(peer int, r replicationResult) bool {
	// 网络调用失败不能当作日志冲突，保留原复制进度。
	if !r.ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 从对应类型的回复中取出 follower 的任期。
	term := r.reply.Term
	if r.snapshot {
		term = r.snapshotReply.Term
	}
	// 即使请求已过期，更高任期也必须先处理：退回 follower 并持久化。
	if term > rf.state.GetCurrentTerm() {
		rf.state.SetCurrentTerm(term)
		rf.state.SetRole(Follower)
		rf.state.SetVotedFor(-1)
		rf.persist()
		return false
	}
	// 角色或任期已经变化，旧请求不能再修改当前的复制进度。
	if rf.state.GetRole() != Leader || r.term != rf.state.GetCurrentTerm() {
		return false
	}

	if r.snapshot || r.reply.Success {
		// 快照请求通过任期检查后视为成功；日志请求还需 Success=true。
		// 只推进到本次请求覆盖的位置，迟到的成功回复不能降低进度。
		if r.last > rf.state.GetMatchIndex(peer) {
			rf.state.SetMatchIndex(peer, r.last)
		}
		if rf.state.GetNextId(peer) < rf.state.GetMatchIndex(peer)+1 {
			rf.state.SetNextId(peer, rf.state.GetMatchIndex(peer)+1)
		}
		// 根据多数节点的匹配进度尝试提交，并唤醒 applier 检查新提交。
		rf.state.UpdateCommitIndex()
		rf.cond.Signal()
		// 本地还有未复制的日志时，通知协调协程继续发送。
		return rf.state.GetNextId(peer) <= rf.state.GetLastLogIndex()
	}

	// 以下处理日志冲突：探测位置已改变，或请求前驱已被确认匹配时，
	// 这个失败回复已经过时，不能用它回退 nextIndex。
	if rf.state.GetNextId(peer) != r.next || rf.state.GetMatchIndex(peer) >= r.next-1 {
		return false
	}
	next := r.next
	switch r.reply.ConflictType {
	case 1:
		// follower 日志较短，从它最后一条日志的下一位置重新探测。
		next = r.reply.MaxIndex + 1
	case 2:
		// 任期冲突：默认跳到 follower 提供的冲突任期起点。
		next = r.reply.ConflictTermFirstIndex
		// 若本地保留日志中也有该任期，则从其最后一条的下一位置探测。
		for i := rf.state.GetLastLogIndex(); i > rf.state.GetLastIncludeIndex(); i-- {
			if rf.state.GetLogEntry(i).Term == r.reply.ConflictTerm {
				next = i + 1
				break
			}
		}
	default:
		// 未识别的冲突信息不能用于调整进度。
		return false
	}
	// 无论冲突建议是什么，都不能退到已经确认匹配的位置之前。
	if next < rf.state.GetMatchIndex(peer)+1 {
		next = rf.state.GetMatchIndex(peer) + 1
	}
	rf.state.SetNextId(peer, next)
	// 只有探测位置发生变化，才要求据此继续发送。
	return next != r.next
}
