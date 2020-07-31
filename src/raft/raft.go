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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

// ApplyMsg indicates a log entry was committed
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

// LogEntry is a struct for info about a single log entry
//
type LogEntry struct {
	Index   int // log index
	Term    int // the term of the leader when this log was stored
	Command interface{}
}

// server state types and consts
type serverState int

// Leader etc are server states
const (
	Leader serverState = iota
	Follower
	Candidate
)

// Raft is a Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote from this server in current term (else -1)
	log         []LogEntry // list of log entries
	commitIndex int        // index of highest log entry known to be committed
	lastApplied int        // index of highest log entry applied to state machine

	// leader specific state
	state              serverState
	nextIndex          []int // next log index to send to each server
	matchIndex         []int // for each server, index of highest log entry known to be replicated on that server
	inAgreementProcess bool  // we are currently in the process of agreeing... helps avoid duplicate Start calls

	// election timeout for follower
	electionTimeout time.Duration

	// for passing info about comitted messages to tester code
	applyCh chan ApplyMsg
}

// HeartbeatSendInterval is How often do we send hearbeats
const HeartbeatSendInterval = time.Duration(50) * time.Millisecond

// DefaultPollInterval is how often to poll for election timeout, and the election parameters
const DefaultPollInterval = time.Duration(50) * time.Millisecond

// MinElectionTimeout gives the lower bound on the randomly generated
// election timeout window in ms
const MinElectionTimeout = 500

// MaxElectionTimeout gives the upper bound on the randomly generated
// election timeout window in ms
const MaxElectionTimeout = 2500

// LogLevel state types and consts
type LogLevel int

// LogDebug etc are different levels we can log at
const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarning
	LogError
)

func (me LogLevel) String() string {
	return [...]string{"Debug", "Info", "Warning", "Error"}[me]
}

// SetLogLevel sets the level we log at
const (
	SetLogLevel LogLevel = LogDebug
)

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// fmt.Printf("State for server %v... currentTerm: %v; state: %v; isLeader: %v\n", rf.me, rf.currentTerm, rf.state, rf.state == Leader)
	return rf.currentTerm, (rf.state == Leader)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// we don't bother locking, because we only call
	// rf.persist from functions where we already hold the lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		rf.Log(LogError, "Error reading persistent state.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.Log(LogDebug, "Reading persistent state on restart:", "\nrf.currentTerm:", rf.currentTerm, "\nrf.votedFor:", rf.votedFor, "\nrf.log", rf.log)
	}
}

// RequestVoteArgs is the args structure for RequestVote RPC
//
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int // index of candidate’s last log entry
	LastLogTerm   int // term of candidate’s last log entry
}

// RequestVoteReply is the reply structure for RequestVote RPC
//
type RequestVoteReply struct {
	CurrentTerm int  // highest term known by receiver
	VoteGranted bool // did receiver vote for us or not
}

// LogUpToDate returns true if a log with LastLogIndex and LastLogTerm
// is at least as up to date as log
// else false
func LogUpToDate(lastIndex int, lastTerm int, log []LogEntry) bool {
	if len(log) == 0 {
		return true // any log is up to date with blank log
	}
	lastEntry := log[len(log)-1]
	return (lastTerm > lastEntry.Term) || (lastTerm == lastEntry.Term && lastIndex >= len(log))
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// request out of date with current leader, don't grant vote
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false

		// grant vote, if candidate is at right term and we haven't voted
		// for anyone else yet, and this server isn't the leader
		// and also check candidate log is at least as up to date as us
	} else if ((rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.state != Leader) ||
		args.CandidateTerm > rf.currentTerm &&
			LogUpToDate(args.LastLogIndex, args.LastLogTerm, rf.log) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true

		// deny vote, already voted for someone else in this term
	} else {
		reply.VoteGranted = false
	}

	rf.Log(LogDebug, "Received RequestVote from server", args.CandidateID, "term", args.CandidateTerm, "\n  - args.LastLogIndex", args.LastLogIndex, "\n  - args.lastLogTerm", args.LastLogTerm, "\n  - rf.log", rf.log, "\n  - rf.votedFor", rf.votedFor, "\n  - VoteGranted:", reply.VoteGranted)

	// update currentTerm and state if candidate has higher term
	// if we found out we're no longer a leader, restart the heartbeat timeout check
	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm
		if rf.state == Leader {
			rf.state = Follower
			go rf.HeartbeatTimeoutCheck()
		}
	}
	reply.CurrentTerm = rf.currentTerm

	// persist - we may have changed rf.currentTerm or rf.votedFor
	rf.persist()
	return
}

//
// code to send a RequestVote RPC to a server.
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
	rf.mu.Lock()
	args.CandidateID = rf.me
	args.CandidateTerm = rf.currentTerm

	// grab last log index and term - default to -1 if log is []
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	} else {
		args.LastLogIndex = -1
		args.LastLogTerm = -1
	}

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs is the args structure for AppendEntries RPC
//
type AppendEntriesArgs struct {
	LeaderTerm        int        // the current leader's term according to request
	LeaderID          int        // so follower can redirect clients
	PrevLogIndex      int        // index of log entry preceding the new ones
	PrevLogTerm       int        // the term of the previous log entry
	LogEntries        []LogEntry // empty for heartbeat, o/w log entries to store
	LeaderCommitIndex int        // the leader's commit index
}

// AppendEntriesReply is the reply structure for AppendEntries RPC
//
type AppendEntriesReply struct {
	CurrentTerm int  // the current term of the server that was hit (for leader to update if needed)
	Success     bool // true if the follower had an entry matching prevlogindex and prevlogterm
	Returned    bool // so we can check whether or not the function has returned (e.g. when gathering responses from a leader commit request)

	// these are all to assist with commit and rollback
	LogLength                 int
	ConflictingEntryTerm      int // the term of an entry that conflicts
	IndexFirstConflictingTerm int // the index of the first entry that has term ConflictingEntryTerm

}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if leader is out of date, do nothing and return
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		reply.Returned = true
		reply.CurrentTerm = rf.currentTerm
		return
	}

	// if leader not out of date, we want to
	// 1. reset election clock
	// 2. convert to follower status as we might have been
	// candidate or leader - start heartbeat check as well
	// 3. update our local term (leader term must be >= local)
	rf.electionTimeout = GetRandomElectionTimeout()
	if rf.state != Follower {
		rf.state = Follower
		go rf.HeartbeatTimeoutCheck()
	}
	rf.currentTerm = args.LeaderTerm
	reply.CurrentTerm = rf.currentTerm

	// leader's nextIndex is out of whack.
	// trim log back accordingly
	if args.PrevLogIndex >= 0 &&
		(len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		// prev log is incorrect, need to roll back
		reply.Success = false

		// need to store these so that we can do fast rollback
		reply.ConflictingEntryTerm = -1
		reply.IndexFirstConflictingTerm = -1
		if args.PrevLogIndex <= len(rf.log) {
			reply.ConflictingEntryTerm = rf.log[args.PrevLogIndex-1].Term
			reply.IndexFirstConflictingTerm = args.PrevLogIndex
			for reply.IndexFirstConflictingTerm-2 > 0 &&
				rf.log[reply.IndexFirstConflictingTerm-2].Term == rf.log[reply.IndexFirstConflictingTerm-1].Term {
				reply.IndexFirstConflictingTerm--
			}
		}

	} else {
		reply.Success = true

		// delete any conflicting log entries and append new ones
		for _, e := range args.LogEntries {
			// need to remove all entries from this point onwards inclusive
			if e.Index <= len(rf.log) && rf.log[e.Index-1].Term != e.Term {
				rf.log = rf.log[:e.Index-1]
			}

			// append current entry if it is going to be at the end of the log
			// if it won't be at the end of the log, it must already be there
			if e.Index > len(rf.log) {
				rf.log = append(rf.log, e)
			}
		}

		// update commit index and send updates to applyCh
		if args.LeaderCommitIndex > rf.commitIndex {

			// walk up through messages to min of leader commit idx,
			// and idx of last log entry added. also stop if we are past the
			// number of actual messages in our log
			// for each message, send acknowledgement to applyCh and
			// update this server's commit idx
			idx := rf.commitIndex + 1
			for idx <= args.LeaderCommitIndex &&
				(len(args.LogEntries) == 0 || idx <= args.LogEntries[len(args.LogEntries)-1].Index) &&
				idx <= len(rf.log) {
				// send acknowledgement to applyCh
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					Command:      rf.log[idx-1].Command,
				}

				// increment and update commit idx
				rf.commitIndex = idx
				idx++
			}
		}
	}

	// length of the log stored on this server - used for walking backwards,
	// and confirming commits
	reply.LogLength = len(rf.log)

	// so the caller knows we have finished
	reply.Returned = true

	// persist - we may have changed rf.currentTerm or rf.log
	rf.persist()

	rf.Log(LogDebug, "Received AppendEntries from server", args.LeaderID, "term", args.LeaderTerm, "\n  - args.LogEntries:", args.LogEntries, "\n  - args.LeaderCommitIndex", args.LeaderCommitIndex, "\n  - rf.log", rf.log, "\n  - rf.commitIndex", rf.commitIndex, "\n  - args.PrevLogIndex", args.PrevLogIndex, "\n  - args.PrevLogTerm", args.PrevLogTerm, "\n  - reply.LogLength", reply.LogLength, "\n  - reply.ConflictingEntryTerm", reply.ConflictingEntryTerm, "\n  - reply.IndexFirstConflictingTerm", reply.IndexFirstConflictingTerm, "\n  - success:", reply.Success)
	return
}

// function to call the AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()

	// these are always just grabbed from rf
	args.LeaderID = rf.me
	args.LeaderTerm = rf.currentTerm
	args.LeaderCommitIndex = rf.commitIndex

	// figure out prevLogIndex and prevLogTerm based on entries passed in
	// otherwise they are the commit index of the leader if we are sending no logs
	//    (so leader still finds out we're behind)
	// otherwise set defaults to -1
	if len(args.LogEntries) > 0 && args.LogEntries[0].Index != 1 {
		args.PrevLogIndex = args.LogEntries[0].Index - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	} else if len(args.LogEntries) == 0 && rf.commitIndex > 0 {
		args.PrevLogIndex = rf.commitIndex
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	} else {
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
	}

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start starts a raft server
//
// this happens when the service using Raft (e.g. a k/v server) wants to start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// server is not the leader, return false immediately
	if rf.state != Leader {
		return -1, -1, false
	}

	// heartbeat routine will pick this up and send appropriate requests
	entry := LogEntry{
		Index:   len(rf.log) + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.Log(LogDebug, "Start called, current log:", rf.log, "\n  - rf.matchIndex: ", rf.matchIndex)

	// persist - we may have changed rf.log
	rf.persist()

	return entry.Index, entry.Term, true
}

// Kill kills a raft server
//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// GetRandomElectionTimeout gets a random election timeout window
func GetRandomElectionTimeout() time.Duration {
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
}

// HeartbeatTimeoutCheck implements election timeout
// for a Raft server, by continually runs a check as to whether sending
// out RequestVote is needed due to heartbeat timeout
func (rf *Raft) HeartbeatTimeoutCheck() {
	// get heartbeat check start time
	lastHeartbeatCheck := time.Now()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.electionTimeout > 0 && rf.state == Follower {
			currentTime := time.Now()
			rf.electionTimeout -= (currentTime.Sub(lastHeartbeatCheck))
			lastHeartbeatCheck = currentTime
			rf.Log(LogDebug, "timeout remaining:", rf.electionTimeout)
		} else if rf.state == Follower {
			// election needs to occur
			// quit this function and run the election
			rf.Log(LogInfo, "timed out as follower, running election.")
			rf.mu.Unlock()
			go rf.RunElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(DefaultPollInterval)
	}
}

// HeartbeatAppendEntries is the leader routine for sending out
// AppendEntries requests to servers
//
// The contents of these are dictated by rf.nextIndex[] for each server
//
// The requests are send on a heartbeat interval
func (rf *Raft) HeartbeatAppendEntries() {
	// make server -> reply map
	replies := make(map[int]*AppendEntriesReply)
	for idx := range rf.peers {
		replies[idx] = &AppendEntriesReply{}
	}

	for !rf.killed() {
		rf.mu.Lock()

		// if we are no longer the leader
		if rf.state != Leader {
			rf.Log(LogDebug, "Discovered no longer the leader, stopping heartbeat")
			rf.mu.Unlock()
			return
		}
		// send out heartbeats concurrently if leader
		for idx := range rf.peers {
			if idx != rf.me {

				// successful request - update matchindex and nextindex accordingly
				if replies[idx].Success {
					if replies[idx].Success {
						rf.matchIndex[idx] = replies[idx].LogLength
						rf.nextIndex[idx] = replies[idx].LogLength + 1
					}

					// failed request - check for better term or decrease nextIndex
				} else if !replies[idx].Success && replies[idx].Returned {

					// we might have found out we shouldn't be the leader!
					if replies[idx].CurrentTerm > rf.currentTerm {
						rf.Log(LogDebug, "Detected server with higher term, stopping heartbeat and changing to follower.")
						rf.state = Follower
						rf.currentTerm = replies[idx].CurrentTerm

						// persist - updated current term
						rf.persist()

						go rf.HeartbeatTimeoutCheck()
						rf.mu.Unlock()
						return
					}

					// failure - we need to decrease next index
					// 1. case where follower has no entry at the place we thought
					//    => want to back up to start of follower log
					// 2. case where server has entry with different term NOT seen by leader
					//    => want to back up nextIndex to the start of the 'run' of entries with that term (i.e. IndexFirstConflictingTerm)
					// 3. case where server has entry with different term that HAS been seen by leader
					//    => want to back up to last entry leader has with that term
					if replies[idx].ConflictingEntryTerm == -1 {
						rf.nextIndex[idx] = replies[idx].LogLength + 1
					} else if rf.log[replies[idx].IndexFirstConflictingTerm-1].Term != replies[idx].ConflictingEntryTerm {
						rf.nextIndex[idx] = replies[idx].IndexFirstConflictingTerm
					} else {
						rf.nextIndex[idx] = replies[idx].IndexFirstConflictingTerm
						for rf.log[rf.nextIndex[idx]].Term == replies[idx].ConflictingEntryTerm {
							rf.nextIndex[idx]++
						}
						rf.nextIndex[idx]++ // want to go one after the last entry leader has with this term
					}
					rf.Log(LogDebug, "Failed to AppendEntries to server", idx, "- rolling back to idx", rf.nextIndex[idx])
				}

				// send a new append entries request to the server
				replies[idx] = &AppendEntriesReply{}
				entries := []LogEntry{}
				if rf.nextIndex[idx] <= len(rf.log) {
					entries = rf.log[rf.nextIndex[idx]-1:]
				}
				args := &AppendEntriesArgs{LogEntries: entries}
				go rf.sendAppendEntries(idx, args, replies[idx])
			}
		}

		// walk up through possible new commit indices
		// update commit index
		origIndex := rf.commitIndex
		newIdx := rf.commitIndex + 1
		for newIdx <= len(rf.log) {
			replicas := 1 // already replicated in our log
			for idx := range rf.peers {
				if idx != rf.me && rf.matchIndex[idx] >= newIdx {
					replicas++
				}
			}
			if replicas >= int(math.Ceil(float64(len(rf.peers))/2.0)) &&
				rf.log[newIdx-1].Term == rf.currentTerm {
				rf.commitIndex = newIdx
				rf.Log(LogInfo, "Command", rf.log[rf.commitIndex-1], "replicated on a majority of servers. Committed to index", rf.commitIndex)
			}
			newIdx++
		}

		// send messages to applyCh for every message that was committed
		for origIndex < rf.commitIndex {
			origIndex++
			rf.Log(LogDebug, "Sending applyCh confirmation for commit of ", rf.log[origIndex-1], "at index", origIndex)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: origIndex,
				Command:      rf.log[origIndex-1].Command,
			}
		}

		rf.mu.Unlock()
		time.Sleep(HeartbeatSendInterval)
	}
}

// RunElection turns a Raft server into a candidate
// and executes the election procedure
func (rf *Raft) RunElection() {
	// get election start time
	lastElectionCheck := time.Now()

	rf.mu.Lock()
	rf.currentTerm++
	// persist - updated current term
	rf.persist()
	rf.Log(LogInfo, "running as candidate")

	// set as candidate state and vote for ourselves,
	// also reset the timer
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.electionTimeout = GetRandomElectionTimeout()

	// for holding replies - we send out the requests concurrently
	peers := rf.peers
	replies := make([]*RequestVoteReply, len(peers))
	rf.mu.Unlock()

	// send out requests concurrently
	for idx := range rf.peers {
		if idx != rf.me {
			args := &RequestVoteArgs{}
			reply := &RequestVoteReply{}
			replies[idx] = reply
			rf.Log(LogDebug, "Sending RequestVote to server", idx)
			go rf.sendRequestVote(idx, args, reply)
		}
	}

	// while we still have time on the clock, poll
	// for election result
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Follower {
			rf.Log(LogInfo, "now a follower")
			// we must have received a heartbeat message from a new leader
			// stop the election
			rf.mu.Unlock()
			return
		} else if rf.electionTimeout > 0 {
			// election still running
			// do a vote count and update time remaining
			currentTime := time.Now()
			rf.electionTimeout -= (currentTime.Sub(lastElectionCheck))
			lastElectionCheck = currentTime
			votes := 1 //  we vote for ourselves automatically
			for idx := range rf.peers {
				if idx != rf.me && replies[idx].VoteGranted {
					votes++
				}
			}
			// majority vote achieved - set state as leader and
			// start sending heartbeats
			if votes >= int(math.Ceil(float64(len(rf.peers))/2.0)) {
				rf.Log(LogInfo, "elected leader", "\n  - rf.log:", rf.log, "\n  - rf.commitIndex", rf.commitIndex)
				rf.state = Leader
				// this volatile state is reinitialized on election
				for idx := range rf.peers {
					if idx != rf.me {
						rf.nextIndex[idx] = len(rf.log) + 1
						rf.matchIndex[idx] = 0
					}
				}
				rf.mu.Unlock()
				go rf.HeartbeatAppendEntries()
				return
			}
		} else {
			// no result - need to rerun election
			rf.Log(LogInfo, "timed out as candidate")
			rf.mu.Unlock()
			go rf.RunElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(DefaultPollInterval)
	}
}

// Make creates a raft server
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

	rf.mu.Lock()

	// default initial state for all servers
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// default initial state for leader
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// set election timeout randomly
	rf.electionTimeout = GetRandomElectionTimeout()

	// for passing info about commits
	rf.applyCh = applyCh

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start election timeout check - server can't be a leader when created
	go rf.HeartbeatTimeoutCheck()

	return rf
}

// Log wraps fmt.Printf
// in order to log only when an instance hasn't been killed
func (rf *Raft) Log(level LogLevel, a ...interface{}) {
	if !rf.killed() && level >= SetLogLevel {
		pc, _, ln, _ := runtime.Caller(1)
		rp := regexp.MustCompile(".+\\.([a-zA-Z]+)")
		funcName := rp.FindStringSubmatch(runtime.FuncForPC(pc).Name())[1]
		data := append([]interface{}{"[ Server", rf.me, "- term", rf.currentTerm, "]", "[", funcName, ln, "]"}, a...)
		fmt.Println(data...)
	}
}
