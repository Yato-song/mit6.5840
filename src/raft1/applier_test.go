package raft

import (
	"sync"
	"testing"
	"time"

	"6.5840/raftapi"
)

func TestApplierDelivery(t *testing.T) {
	rf := &Raft{
		applyCh: make(chan raftapi.ApplyMsg),
		state: RaftState{
			logs: []LogEntry{{}, {Command: "first"}, {Command: "second"}},
			commitIndex: 2,
		},
	}
	rf.cond = *sync.NewCond(&rf.mu)
	go rf.applier()

	for i, command := range []string{"first", "second"} {
		select {
		case msg := <-rf.applyCh:
			if !msg.CommandValid || msg.CommandIndex != i+1 || msg.Command != command {
				t.Fatalf("unexpected delivery: %+v", msg)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for committed entry")
		}

		// The application can call back into Raft before accepting another entry.
		unlocked := make(chan struct{})
		go func() {
			rf.mu.Lock()
			rf.mu.Unlock()
			close(unlocked)
		}()
		select {
		case <-unlocked:
		case <-time.After(time.Second):
			t.Fatal("applier holds Raft lock while application is not receiving")
		}
	}

	select {
	case msg := <-rf.applyCh:
		t.Fatalf("duplicate delivery: %+v", msg)
	case <-time.After(50 * time.Millisecond):
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state.GetLastApplied() != 2 {
		t.Fatalf("lastApplied = %d, want 2", rf.state.GetLastApplied())
	}
}
