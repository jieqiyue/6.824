package raft

type HeartBeatArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

type HeartBeatReply struct {
	// Your data here (3A, 3B).
	Term int
}

type SendLogArgs struct {
	Term        int
	CandidateId int
}

type SendLogReply struct {
	Term int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}
