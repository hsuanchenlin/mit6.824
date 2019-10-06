package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type State int

const (
	Candidate = iota
	Leader
	Follower
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
	ReceiverID   int
	ErrorCode    int
}

type LogEntry struct {
	LogTerm  int
	Command  interface{}
	LogIndex int
}
