package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"time"
)
import "labrpc"
import "math/rand"
import "sync/atomic"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor    int
	commitIndex int
	lastApplied int
	heartbeat   uint32
	state       State
	votedInTerm bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PreLogIndex  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A).

	reply.Success = true
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}
	if reply.Success {
		atomic.StoreUint32(&rf.heartbeat, 1)
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderID
	}
	rf.mu.Unlock()

	_, _ = DPrintf("AppendEntries receiver %d rf currentterm  %d \nsender %d term %d\nreply grant %t term %d" , rf.me, rf.currentTerm, args.LeaderID, args.Term, reply.Success, reply.Term)

}

//func min(a, b int) int{
//	if a < b {
//		return a
//	}
//	return b
//}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VotedGranted = false
	} else if args.Term == rf.currentTerm && !rf.votedInTerm{
		if rf.votedFor == -1 {
			reply.VotedGranted = true
			rf.votedFor = args.CandidateId
			rf.votedInTerm = true
		}
		if rf.votedFor == args.CandidateId {
			reply.VotedGranted = true
			rf.votedInTerm = true
		}
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VotedGranted = true
		rf.votedInTerm = true
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
	_, _ = DPrintf(" receiver %d rf currentterm  %d \nsender %d term %d\nreply grant %t term %d" , rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.VotedGranted, reply.Term)
	//_, _ = DPrintf("  " , )
	//_, _ = DPrintf(" reply grant %t term %d " , reply.VotedGranted, reply.Term)

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("server %d sender %d sender term %d",server, args.CandidateId, args.Term)
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.state = Candidate
	rf.votedInTerm = false
	rand.Seed(time.Now().UnixNano())
	DPrintf("state enum candidate %d leader %d follower %d", Candidate, Leader, Follower)
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				sec := rand.Intn(250) + 150
				time.Sleep(time.Duration(sec) * time.Millisecond)
				continue
			}
			rf.currentTerm += 1
			rf.votedFor = me
			rf.votedInTerm = false
			_, _ = DPrintf("candidate election id:%d term:%d", me, rf.currentTerm)
			rf.mu.Unlock()

			voteCnt := 0
			peerSize := len(peers)
			peerVoteCh := make(chan int, peerSize-1)
			for i := 0; i < peerSize; i++ {
				if i == me {
					continue
				}
				rf.mu.Lock()
				if rf.state != Candidate {
					rf.mu.Unlock()
					peerVoteCh <- 0
					break
				}
				currentTerm := rf.currentTerm
				rf.mu.Unlock()
				go func(ii, term int) {
					rq := RequestVoteArgs{CandidateId: me, Term: term}
					reply := RequestVoteReply{}
					rf.mu.Lock()
					if rf.state != Candidate {
						return
					}
					rf.mu.Unlock()
					response := rf.sendRequestVote(ii, &rq, &reply)
					if response {
						if reply.VotedGranted {
							peerVoteCh <- 1
						} else {
							peerVoteCh <- 0
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
							}
							rf.mu.Unlock()
						}
					} else {
						peerVoteCh <- -1
					}
					_, _ = DPrintf("requestvote sender %d currentterm %d reply granted %t term %d otherid %d rsp:%t", me, rq.Term, reply.VotedGranted, reply.Term, ii,response)
				}(i, currentTerm)
			}
			voterNum := peerSize
			for i := 0; i < peerSize-1; i++ {
				vote := <-peerVoteCh
				if vote >= 0 {
					voteCnt += vote
				} else {
					voterNum -= 1
				}

			}
			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			_, _ = DPrintf("counting vote id %d votes %d selected:%t voteCnt %d", rf.me, voteCnt, voteCnt>=(voterNum+1)/2, voteCnt)

			if voteCnt < (voterNum+1)/2 {
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()
				sec := rand.Intn(350) + 150
				time.Sleep(time.Duration(sec) * time.Millisecond)
				continue
			}
			_, _ = DPrintf("After counting vote id %d votes %d selected:%t voteCnt %d", rf.me, voteCnt, voteCnt>=(voterNum+1)/2, voteCnt)

			rf.mu.Lock()
			rf.state = Leader
			rf.mu.Unlock()
			atomic.StoreUint32(&rf.heartbeat, 1)
		}
	}()

	go func() {
		for {
			sec := rand.Intn(400) + 200
			time.Sleep(time.Duration(sec) * time.Millisecond)
			rf.mu.Lock()
			if atomic.LoadUint32(&rf.heartbeat) == 0 {
				if rf.state == Follower {
					_, _ = DPrintf("lose connect id %d Exleader %d",rf.me, rf.votedFor)
					rf.votedFor = -1
					rf.state = Candidate
				}
			}
			atomic.StoreUint32(&rf.heartbeat, 0)
			rf.mu.Unlock()

		}
	}()
	//sending heartbeat
	go func() {
		for {
			sec := 120
			time.Sleep(time.Duration(sec) * time.Millisecond)
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			peerSize := len(rf.peers)

			for i := 0; i < peerSize; i++ {
				if i == me {
					continue
				}
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					break
				}
				copyterm := rf.currentTerm
				rf.mu.Unlock()
				//_, _ = DPrintf("heartbeat outside sender %d receiver %d", me, i)
				go func(ii, term int) {
					reply := &AppendEntriesReply{}
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					args := &AppendEntriesArgs{LeaderID: me}
					args.Term = term
					rf.mu.Unlock()
					ok := rf.peers[ii].Call("Raft.AppendEntries", args, reply)
					if !ok{
						_, _ = DPrintf("sending heartbeat fail leader: %d receiver:%d state:%d", me, ii, rf.state)
					}

					if ok {
						_, _ = DPrintf("sending heartbeat success leader: %d receiver:%d state:%d", me, ii, rf.state)
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Candidate
							_, _ = DPrintf("from leader to follower id %d receiver %d state %d", me, ii, rf.state)
						}
						rf.mu.Unlock()
					} else {

					}
				}(i, copyterm)
			}
		}
	}()
	//TODO:send out cmd
	//go func() {
	//	msg := <-applyCh
	//
	//}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

//func (rf *Raft)election() {
//	rf.mu.Lock()
//	if rf.state == Leader {
//		rf.mu.Unlock()
//		return
//	}
//	rf.mu.Unlock()
//}