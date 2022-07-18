package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"Raft/labrpc"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

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

type RaftState uint8

const (
	Dead RaftState = iota
	Follower
	Candidate
	Leader
)
const NIDX int = -1

var StartTime time.Time = time.Now()

type LogType struct {
	Term    int
	Command interface{}
	// TODO? : more data in LogType
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	state              RaftState
	currentTerm        int
	voteFor            int
	log                []LogType
	commitIndex        int
	lastApplied        int
	nextIndex          []int
	matchIndex         []int
	applyCh            chan ApplyMsg
	commitIndexFlushCh chan int
	electionTimer      *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type voteCounter struct {
	mu  sync.Mutex
	val int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
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

// region RequestVote RPC
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandiateId   int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.printfLog("term changed %d -> %d", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.voteFor = NIDX
	}
	if rf.voteFor != NIDX && rf.voteFor != args.CandiateId {
		reply.VoteGranted = false
		return
	}
	lastEntry := rf.getLastLog()
	if args.LastLogTerm < lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex < rf.getLogLen()-1) { // candidate’s log is not so up-to-date as receiver’s log
		// the -1 is right?
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.voteFor = args.CandiateId
}

// endregion RequestVote RPC

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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.log = append(rf.log, LogType{})
	index = rf.getLogLen() - 1
	// Your code here (2B).

	return index, term, isLeader
}

// region AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogType
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NewNextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log := fmt.Sprintf("recieve a heartbeat from %d", args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		rf.printfLog(log)
	}()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		log += fmt.Sprintf("\nheart beat with a old term, sender's term: %d ,my term: %d ", args.Term, rf.currentTerm)
		return
	}
	if rf.currentTerm < args.Term {
		log += fmt.Sprintf("\nterm updated from %d to %d", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.voteFor = NIDX
	}
	rf.resetElectionTimeout()
	if rf.state != Follower {
		rf.becomeFollower()
	}
	if args.PrevLogIndex >= rf.getLogLen() {
		reply.NewNextIndex = rf.getLogLen()
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// rf.printLog(fmt.Sprintf())
		// term not match, send not success and the index of the first entry with term == rf.log[args.PrevLogIndex].Term
		newNextIndex := args.PrevLogIndex
		for ; rf.log[newNextIndex].Term == rf.log[args.PrevLogIndex].Term; newNextIndex-- {
		}
		newNextIndex += 1
		reply.NewNextIndex = newNextIndex
		reply.Success = false
		return
	}
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	reply.NewNextIndex = rf.getLogLen()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.getLogLen()-1)
		rf.commitIndexFlush()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// endregion AppendEntries RPC

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printfLog("server killed")
	rf.state = Dead
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

	// Your initialization code here (2A, 2B, 2C).rf.state = Follower
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = NIDX
	rf.log = make([]LogType, 1) // log begin at index 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.applyCh = applyCh
	rf.commitIndexFlushCh = make(chan int)
	rf.electionTimer = time.NewTimer(time.Duration(rand.Int63()%150+300) * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startWorking()
	rf.mu.Lock()
	rf.printfLog("server made")
	rf.mu.Unlock()
	return rf
}

// region state changing function (DONT LOCK!!!)
func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.printfLog("become follower")
	rf.resetElectionTimeout()
	// TODO: inline function to become follower
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.printfLog("become candidate")
	rf.voteFor = rf.me
	voteCnt := voteCounter{}
	voteCnt.val = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandiateId = rf.me
		args.LastLogIndex = rf.getLogLen()
		args.LastLogTerm = rf.getLastLog().Term
		// args.replyChan = replyChan
		go rf.RequestVoteThread(i, args, &voteCnt)
	}

	// TODO: inline function to become candidate
}

func (rf *Raft) becomeLeader() {
	// TODO: inline function to become leader
	rf.printfLog("become leader")
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	logLen := rf.getLogLen()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = logLen // TODO : may be wrong
	}
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.heartbeatThread()
}

// endregion state changing function
// region background threads
func (rf *Raft) startWorking() {
	go rf.electionTimingThread()
	go rf.applyThread()
}

func (rf *Raft) RequestVoteThread(peerIndex int, args *RequestVoteArgs, voteCnt *voteCounter) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peerIndex, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	rf.resetElectionTimeout()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		return
	}
	if reply.VoteGranted {
		if (rf.currentTerm == args.Term) && (rf.state == Candidate) {
			voteCnt.mu.Lock()
			voteCnt.val++
			if voteCnt.val > len(rf.peers)/2 {
				rf.becomeLeader()
			}
			voteCnt.mu.Unlock()
		}
	}
}

func (rf *Raft) AppendEntriesThread(peerIndex int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	rf.printfLog("send a AppendEntries request to %d,detail:\t%+v", peerIndex, *args)
	ok := rf.sendAppendEntries(peerIndex, args, reply)
	if !ok {
		return
	}
	rf.printfLog("recieve a AppendEntries reply from %d,detail:\t%+v", peerIndex, *reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	rf.nextIndex[peerIndex] = reply.NewNextIndex
	if reply.Success {
		rf.matchIndex[peerIndex] = reply.NewNextIndex - 1
		rf.leaderUpdateCommitIndex()
	}
}

func (rf *Raft) electionTimingThread() {
	for range rf.electionTimer.C {
		rf.mu.Lock()
		rf.resetElectionTimeout()
		if rf.state == Dead {
			return
		}
		if rf.state != Leader {
			rf.becomeCandidate()
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) applyThread() {
	for {
		rf.mu.Lock()
		if rf.state == Dead {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			nowApplyMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			rf.mu.Unlock()
			rf.applyCh <- nowApplyMsg
			continue
		}
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			<-rf.commitIndexFlushCh
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatThread() {
	heartBeatTicker := time.NewTicker(20 * time.Millisecond)
	for range heartBeatTicker.C {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log
			args.LeaderCommit = rf.commitIndex
			rf.printfLog("send a heartbeat to %d", i)
			go rf.AppendEntriesThread(i, args)
		}
		rf.mu.Unlock()
	}
}

// endregion background threads
// region tool functions
func minInt(lhs, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}
func (rf *Raft) getPeerNum() int {
	return len(rf.peers)
}
func (rf *Raft) getLogLen() int {
	return len(rf.log)
}
func (rf *Raft) getLastLog() LogType {
	return rf.log[len(rf.log)-1]
}
func (rf *Raft) commitIndexFlush() {
	rf.commitIndexFlushCh <- rf.commitIndex
}
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(time.Duration(rand.Int63()%150+300) * time.Millisecond)
}

func (rf *Raft) leaderUpdateCommitIndex() {
	tmp_list := make([]int, len(rf.matchIndex))
	copy(tmp_list, rf.matchIndex)
	sort.Ints(tmp_list)
	newCommitIndex := tmp_list[(rf.getPeerNum()-1)/2]
	if newCommitIndex > rf.commitIndex { // may be unnecessary
		rf.commitIndex = newCommitIndex
		rf.commitIndexFlush()
	}
}

func (rf *Raft) printfLog(format string, a ...interface{}) {
	if rf.state != Dead {
		PrintfLog("[%d] %s", rf.me, fmt.Sprintf(format, a...))
	}
}

func PrintfLog(format string, a ...interface{}) {
	io.WriteString(os.Stderr, fmt.Sprintf("{%12d} %s\n", time.Now().UnixMilli()-StartTime.UnixMilli(), fmt.Sprintf(format, a...)))
}

// endregion tool functions
