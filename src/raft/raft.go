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
const ElectionTimeout = time.Duration(350 * time.Millisecond)
const AppendEntriesInterval = time.Duration(100 * time.Millisecond) // sleep time between successive AppendEntries call

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	commitIndex   int
	lastApplied   int
	logIndex      int
	state         State
	votedInTerm   bool
	leaderId      int
	electionTimer *time.Timer
	log           []LogEntry
	applyCh       chan ApplyMsg
	notifyApplyCh chan struct{} // notify to apply
	nextIndex     []int
	matchIndex    []int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A).
	reply.Success = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//Rule1 in Fig 2
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.ConflictIndex = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower
	//Rule 2 in Fig 2
	if rf.logIndex <= args.PrevLogIndex {
		reply.Success = false
		conflictIndex := Min(rf.logIndex-1, args.PrevLogIndex)
		conflictTerm := rf.log[conflictIndex].LogTerm
		floor := rf.commitIndex
		for ; conflictIndex > floor && rf.log[conflictIndex-1].LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.ConflictIndex = conflictIndex
		return
	}
	//Rule 3 in Fig 2
	if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		reply.Success = false
		conflictIndex := Min(rf.logIndex-1, args.PrevLogIndex)
		conflictTerm := rf.log[conflictIndex].LogTerm
		floor := rf.commitIndex
		for ; conflictIndex > floor && rf.log[conflictIndex-1].LogTerm == conflictTerm; conflictIndex-- {
		}
		reply.ConflictIndex = conflictIndex
		return
	}
	reply.Success, reply.ConflictIndex = true, -1
	//Rule 3 in Fig 2
	i := 0
	for ; i < len(args.Entries); i++ {
		if i+args.PrevLogIndex+1 >= rf.logIndex {
			break
		}
		if rf.log[args.Entries[i].LogIndex].LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = args.PrevLogIndex + 1 + i
			rf.log = append(rf.log[:rf.logIndex])
			break
		}
	}
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex++
	}

	//Rule5 in Fig 2
	if args.LeaderCommit > rf.commitIndex {
		lenEntries := len(args.Entries)
		originCommitIndex := rf.commitIndex
		rf.commitIndex = Max(originCommitIndex, Min(args.LeaderCommit, args.PrevLogIndex+lenEntries))
		if rf.commitIndex > originCommitIndex {
			rf.notifyApplyCh <- struct{}{}
		}
	}
	if reply.Success {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.votedFor = args.LeaderID
		rf.resetElectionTimer(NewRandDuration(ElectionTimeout))
	}
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.ReceiverID = rf.me
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			reply.VotedGranted = true
			reply.Term = rf.currentTerm
			reply.ErrorCode = 1
			return
		}
	}
	if rf.currentTerm > args.Term || // valid candidate
		(rf.currentTerm == args.Term && rf.votedFor != -1) { // the server has voted in this term
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		if rf.state != Follower { // once server becomes follower, it has to reset electionTimer
			rf.resetElectionTimer(NewRandDuration(ElectionTimeout))
			rf.state = Follower
		}
	}

	rf.leaderId = -1 // other server trying to elect a new leader
	reply.Term = args.Term
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	if lastLogTerm > args.LastLogTerm || // the server has log with higher term
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // under same term, this server has longer index
		reply.VotedGranted = false
		return
	}
	reply.VotedGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimer(NewRandDuration(ElectionTimeout)) // granting vote to candidate, reset electionTimer
	reply.ErrorCode = 1
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
func (rf *Raft) sendRequestVoteChan(server int, args *RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		reply.VotedGranted = false
		reply.ErrorCode = -1
	}
	reply.ReceiverID = server
	replyCh <- reply
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
	//index := -1
	//term := -1
	//isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	} // append log only if server is leader
	index := rf.logIndex
	entry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	if offsetIndex := rf.logIndex; offsetIndex < len(rf.log) {
		rf.log[offsetIndex] = entry
	} else {
		rf.log = append(rf.log, entry)
	}
	rf.matchIndex[rf.me] = rf.logIndex
	rf.logIndex += 1
	//rf.persist()
	go rf.replicate()
	return index, rf.currentTerm, true
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.state = Candidate
	rf.votedInTerm = false
	rf.electionTimer = time.NewTimer(NewRandDuration(ElectionTimeout))
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.log = []LogEntry{{0, nil, 0}}
	rf.logIndex = 1
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				electionDuration := NewRandDuration(ElectionTimeout)
				rf.election()
				rf.mu.Lock()
				rf.resetElectionTimer(electionDuration)
				rf.mu.Unlock()
			}
		}
	}()
	go rf.apply()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
func (rf *Raft) tick() {
	timer := time.NewTimer(AppendEntriesInterval)
	for {
		select {
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}
func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.leaderId = -1
	rf.votedFor = rf.me
	rf.state = Candidate
	currentTerm, lastLogIndex, me := rf.currentTerm, rf.logIndex-1, rf.me
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	rf.mu.Unlock()
	peerSize := len(rf.peers)
	votes := 0
	replyCh := make(chan RequestVoteReply, peerSize-1)
	args := RequestVoteArgs{Term: currentTerm, CandidateId: me, LastLogIndex:lastLogIndex, LastLogTerm:lastLogTerm}
	electionDuration := NewRandDuration(ElectionTimeout)
	timer := time.After(electionDuration) // in case there's no quorum, this election should timeout
	for i := 0; i < peerSize; i++ {
		if i == rf.me {
			continue
		}
		go func(serverID int) {
			rf.sendRequestVoteChan(serverID, &args, replyCh)
		}(i)
	}
	candidatesNum := peerSize
	for {
		select {
		case reply := <-replyCh:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor, rf.leaderId = -1, -1
				rf.state = Follower
				rf.mu.Unlock()
				break
			}
			if reply.VotedGranted {
				votes++
			}
			if votes >= candidatesNum/2 {
				if rf.state == Candidate {
					rf.state = Leader
					rf.initIndex()
					go rf.tick()
				}
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		case <-timer: // election timeout
			return
		}
	}
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	// Always stop a electionTimer before reusing it. See https://golang.org/pkg/time/#Timer.Reset
	// We ignore the value return from Stop() because if Stop() return false, the value inside the channel has been drained out
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}

// only use in lab2A
func (rf *Raft) heartbeat() {

	peerSize := len(rf.peers)
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, LeaderCommit: rf.commitIndex}
	rf.mu.Unlock()
	for i := 0; i < peerSize; i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) replicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[rf.me] = rf.logIndex + 1
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower)
		}
	}
}

//triggered by checkiing nextIndex on leader
func (rf *Raft) sendLogEntry(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].LogTerm
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommit: rf.commitIndex, Entries: nil}
	if rf.nextIndex[server] < rf.logIndex {
		args.Entries = rf.getRangeEntry(prevLogIndex+1, rf.logIndex)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if reply.Success {
			if prevLogIndex+len(args.Entries) >= rf.nextIndex[server] {
				rf.nextIndex[server] = prevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = prevLogIndex + len(args.Entries)
			}
			toCommitIndex := prevLogIndex + len(args.Entries)
			if rf.canCommit(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				rf.notifyApplyCh <- struct{}{}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor, rf.leaderId = -1, -1
				rf.resetElectionTimer(NewRandDuration(ElectionTimeout))
			} else {
				//confilct
				rf.nextIndex[server] = Max(1,Min(reply.ConflictIndex, rf.logIndex))
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry

			if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getRangeEntry(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm, Command: entry.Command}
			}
		}
	}
}
func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	//from := rf.getOffsetIndex(fromInclusive)
	//to := rf.getOffsetIndex(toExclusive)
	return append([]LogEntry{}, rf.log[fromInclusive:toExclusive]...)
}

func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.log[index].LogTerm == rf.currentTerm{
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
}
