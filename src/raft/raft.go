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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm       int       //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor          int       //candidateId that received vote in current term (or null if none)
	lastActiveTime    time.Time //上次活跃时间 刷新时机有：收到心跳检测，给其他candidate投票
	lastBroadcastTime time.Time //上次leader广播的时间

	log  []LogEntry
	role int //0-leader 1-follower 2-candidate

	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine
	leaderId    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	term = rf.currentTerm
	if rf.role == 0 && rf.leaderId == rf.me {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s Term
	CandidateId  int //candidate requesting vote
	lastLogIndex int //index of candidate’s last log entry (§5.4)
	lastLogTerm  int //term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

const HeartBeatsTimeOut = 100 * time.Microsecond

type AppendEntriesArgs struct {
	Term         int         //leader’s term
	LeaderId     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new ones
	PrevLogTerm  int         //term of prevLogIndex entry
	Entries      []*LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	fmt.Printf("Raft[%d] handle RequestVote, candidateId: %d, currentTerm: %d, Term: %d, votedFor: %d\n",
		rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.votedFor)
	if args.Term < rf.currentTerm { //比较term，比我的小，拒绝投票
		return
	}
	if args.Term > rf.currentTerm { //比我的大，转为该term的follower继续投票
		rf.currentTerm = args.Term
		rf.role = 1
		rf.votedFor = -1
		rf.leaderId = -1
	}
	fmt.Printf("Raft[%d].votedFor: %d\n", rf.me, rf.votedFor)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.lastLogTerm < lastLogTerm || args.lastLogIndex < len(rf.log) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastActiveTime = time.Now()
	}
	fmt.Printf("Raft[%d] return RequestVote, candidateId: %d, VoteGranted: %v, rf.votedFor: %d\n",
		rf.me, args.CandidateId, reply.VoteGranted, rf.votedFor)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("Raft[%d] handle AppendEntries, Term: %d, currentTerm: %d, leaderId: %d\n",
		rf.me, args.Term, rf.currentTerm, rf.leaderId)
	defer fmt.Printf("Raft[%d] return AppendEntries, Term: %d, currentTerm: %d, leaderId: %d\n",
		rf.me, args.Term, rf.currentTerm, rf.leaderId)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	//log := rf.log[args.PrevLogIndex]
	//if log.Term != args.PrevLogIndex {
	//	return
	//}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = 1
		rf.leaderId = -1
		rf.votedFor = -1
	}

	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(1 * time.Millisecond)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			now := time.Now()
			timeout := time.Duration(150+rand.Int31n(150)) * time.Millisecond
			elapse := now.Sub(rf.lastActiveTime)
			if rf.role == 1 {
				if elapse > timeout { //时间超时，未收到心跳检测，convert to candidate
					rf.role = 2
					fmt.Printf("Raft[%d] convert to candidate\n", rf.me)
				}
			}
			if rf.role == 2 && elapse > timeout {
				//start election
				rf.currentTerm++               //Increment currentTerm
				rf.votedFor = rf.me            //Vote for self
				rf.lastActiveTime = time.Now() //Reset election timer
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					lastLogIndex: len(rf.log),
				}
				if len(rf.log) != 0 {
					args.lastLogTerm = rf.log[len(rf.log)-1].Term
				}
				rf.mu.Unlock()
				fmt.Printf("Raft[%d] RequestVote starts, lastLogIndex: %d, lastLogTerm: %d\n",
					rf.me, args.lastLogIndex, args.lastLogTerm)
				//rf.sendRequestVote(rf.me, args, reply)
				voteCount := 1 //收到的投票数
				respCount := 1 //收到的回应数
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					if peerId == rf.me {
						continue
					}
					go func(id int) {
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{
								peerId: id,
								resp:   &resp,
							}
						} else {
							voteResultChan <- &VoteResult{
								peerId: id,
								resp:   nil,
							}
						}
					}(peerId)
				}
				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						respCount += 1
						resp := voteResult.resp
						if resp != nil {
							if resp.VoteGranted {
								voteCount++
							}
							if resp.Term > maxTerm {
								maxTerm = resp.Term
							}
						}
						if respCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							fmt.Printf("Raft[%d] respCount: %d, voteCount: %d\n", rf.me, respCount, voteCount)
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					fmt.Printf("Raft[%d] RequestVote ends, respCount: %d, voteCount: %d, Role: %d ,maxTerm: %d currentTerm: %d\n",
						rf.me, respCount, voteCount, rf.role, maxTerm, rf.currentTerm)
				}()
				if rf.role != 2 {
					return
				}
				if maxTerm > rf.currentTerm {
					rf.currentTerm = maxTerm
					rf.role = 1
					rf.leaderId = -1
					rf.votedFor = -1
					return
				}
				if voteCount > len(rf.peers)/2 {
					rf.role = 0
					rf.leaderId = rf.me
					rf.lastBroadcastTime = time.Now()
					return
				}
			}
		}()
	}
}

func (rf *Raft) appendEntries() {
	for rf.killed() == false {
		func() {
			time.Sleep(1 * time.Millisecond)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			fmt.Printf("Raft[%d] appendEntries, role: %d\n", rf.me, rf.role)
			if rf.role != 0 {
				return
			}
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < HeartBeatsTimeOut {
				return
			}
			rf.lastBroadcastTime = time.Now()
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				go func(id int) {
					fmt.Printf("Raft[%d] AppendEntries starts, term: %d, peerId: %d\n", rf.me, args.Term, id)
					resp := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, &args, &resp); ok {
						if resp.Term > rf.currentTerm { // 变成follower
							rf.role = 1
							rf.leaderId = -1
							rf.currentTerm = resp.Term
							rf.votedFor = -1
						}
					}
				}(peerId)
			}
		}()
	}
}

type VoteResult struct {
	peerId int
	resp   *RequestVoteReply
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	rf.votedFor = -1
	rf.role = 1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.appendEntries()
	return rf
}
