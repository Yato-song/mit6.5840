package raft

type LogEntry struct {
	Term	TermNumber
	Command	interface{}
}

func (le *LogEntry) GetTerm() TermNumber {
	return le.Term
}

func (le *LogEntry) GetCommand() interface{} {
	return le.Command
}