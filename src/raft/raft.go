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
	CommandTerm  int
}

// LogEntry is a struct for info about a single log entry
//
type LogEntry struct {
	Index   int // log index
	Term    int // the term of the leader when this log was stored
	Command interface{}
}

// peristent state to encode and decode
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// Snapshot is the persistent snapshot to encode and decode
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
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

	// from most recent snapshot, if any
	lastIncludedIndex int
	lastIncludedTerm  int

	// for doing no-op commits
	// used for kvraft only so as not to break lab 2 tests
	noops bool
}

// heartbeatSendInterval is How often do we send hearbeats
const heartbeatSendInterval = time.Duration(100) * time.Millisecond

// defaultPollInterval is how often to poll for election timeout, and the election parameters
const defaultPollInterval = time.Duration(50) * time.Millisecond

// minElectionTimeout gives the lower bound on the randomly generated
// election timeout window in ms
const minElectionTimeout = 500

// maxElectionTimeout gives the upper bound on the randomly generated
// election timeout window in ms
const maxElectionTimeout = 2500

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
	return [...]string{"DEBUG", "INFO", "WARNING", "ERROR"}[me]
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

// GetStateBytes gets raft's state as a bytes array
// lock is optional since we may be calling this within a critical section
// already
// this then allows the state data to be saved to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) GetStateBytes(lock bool) []byte {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(PersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})
	return w.Bytes()
}

// if the raft log has been trimmed, need to convert
// from target log index -> actual index in raft
// return -1 if log is empty or log with this index precedes the log
// if it exceeds the log, still return the expected index
// can only call this when lock is already acquired!
func (rf *Raft) getRaftLogIndex(idx int) int {
	if len(rf.log) == 0 {
		return -1
	}
	logIdx := idx - rf.log[0].Index
	if logIdx < 0 {
		return -1
	}
	return logIdx
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
	var s PersistentState
	if d.Decode(&s) != nil {
		rf.Log(LogError, "Error reading persistent state.")
	} else {
		rf.currentTerm = s.CurrentTerm
		rf.votedFor = s.VotedFor
		rf.log = s.Log
		rf.Log(LogDebug, "Read persistent state on restart:", "\nrf.currentTerm:", rf.currentTerm, "\nrf.votedFor:", rf.votedFor, "\nrf.log", rf.log)
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
// is at least as up to date as rf.log
// else false
func (rf *Raft) LogUpToDate(lastIndex int, lastTerm int) bool {
	// if empty log, check snapshot
	if len(rf.log) == 0 {
		return (lastTerm > rf.lastIncludedTerm) || (lastTerm == rf.lastIncludedTerm && lastIndex >= rf.lastIncludedIndex)
	}
	lastEntry := rf.log[len(rf.log)-1]
	return (lastTerm > lastEntry.Term) || (lastTerm == lastEntry.Term && lastIndex >= lastEntry.Index)
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
	} else if (((rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.state != Leader) || args.CandidateTerm > rf.currentTerm) &&
		rf.LogUpToDate(args.LastLogIndex, args.LastLogTerm) {
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
			go rf.heartbeatTimeoutCheck()
		}
	}
	reply.CurrentTerm = rf.currentTerm

	// persist - we may have changed rf.currentTerm or rf.votedFor
	data := rf.GetStateBytes(false)
	rf.persister.SaveRaftState(data)
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
	args.CandidateID = rf.me
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
	LastLogIndex              int
	ConflictingEntryTerm      int // the term of an entry that conflicts
	IndexFirstConflictingTerm int // the index of the first entry that has term ConflictingEntryTerm

	// so that leader can appropriately update MatchIndex
	NewLogsAdded int
}

// helper function - returns true if log contains an entry with PrevIndex, PrevTerm
// OR if the most recent snapshot LastIndex, LastTerm matches
// must only be called if lock is already held!
func (rf *Raft) correctPrevLogEntry(PrevLogIndex int, PrevLogTerm int) bool {
	// we have a snapshot where the last included log is this entry
	if PrevLogIndex == rf.lastIncludedIndex && PrevLogTerm == rf.lastIncludedTerm {
		return true
	}
	prevRaftLogIndex := rf.getRaftLogIndex(PrevLogIndex)
	// the leader nextIndex is ahead of us
	if prevRaftLogIndex >= len(rf.log) {
		return false
	}

	// NOTE:
	// if prevRaftLogIndex == -1 ... this should never happen?
	// We know length of rf.log > 0 (see where this function is called), so this
	// would only occur if leader nextIndex for this server preceded our snapshot;
	// but on leader election, nextIndex is set to the end of the leader log,
	// including all committed entries.
	// However, our snapshot includes AT MOST all committed entries,
	// so nextIndex should never precede it.
	if prevRaftLogIndex == -1 {
		rf.Log(LogError, "AppendEntries call has PrevLogIndex preceding our log!")
		return false
	}

	// we must have an entry at the given index (see above note for why
	// PrevLogIndex will never precede our snapshot), so just return a bool for whether
	// or not the term of this entry is correct
	return rf.log[prevRaftLogIndex].Term == PrevLogTerm

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
		go rf.heartbeatTimeoutCheck()
	}
	rf.currentTerm = args.LeaderTerm
	reply.CurrentTerm = rf.currentTerm

	// mismatch on prevlogindex/term - leader's nextIndex is out of whack.
	// will need to trim log back accordingly - set the necessary reply variables
	if args.PrevLogIndex > 0 && (len(rf.log) == 0 || !rf.correctPrevLogEntry(args.PrevLogIndex, args.PrevLogTerm)) {
		// prev log is incorrect, need to roll back
		reply.Success = false

		// need to correctly set these variables for fast rollback
		// in the case where we have a wrong log entry
		//
		// walk back to the first log that has the same term as our
		// conflicting log does
		//
		// if instead the leader nextIndex is just ahead of our log,
		// we use the highest indexed log to roll back instead
		reply.ConflictingEntryTerm = -1
		reply.IndexFirstConflictingTerm = -1
		prevRaftLogIndex := rf.getRaftLogIndex(args.PrevLogIndex)
		if len(rf.log) > 0 && prevRaftLogIndex < len(rf.log) {
			reply.ConflictingEntryTerm = rf.log[prevRaftLogIndex].Term
			reply.IndexFirstConflictingTerm = args.PrevLogIndex
			for reply.IndexFirstConflictingTerm-1 > rf.log[0].Index &&
				rf.log[rf.getRaftLogIndex(reply.IndexFirstConflictingTerm)-1].Term == rf.log[rf.getRaftLogIndex(reply.IndexFirstConflictingTerm)].Term {
				reply.IndexFirstConflictingTerm--
			}
		}

	} else {
		reply.Success = true

		// delete any conflicting log entries and append new ones
		for _, e := range args.LogEntries {
			// if a clash, need to remove all entries from this point onwards inclusive
			raftLogIdx := rf.getRaftLogIndex(e.Index)
			if raftLogIdx > 0 && raftLogIdx < len(rf.log) && rf.log[raftLogIdx].Term != e.Term {
				rf.log = rf.log[:raftLogIdx]
			}

			// append current entry if it is going to be at the end of the log, otherwise just overwrite
			if len(rf.log) == 0 || raftLogIdx >= len(rf.log) {
				rf.log = append(rf.log, e)
			} else {
				rf.log[raftLogIdx] = e
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
				(len(args.LogEntries) == 0 || idx <= args.LogEntries[len(args.LogEntries)-1].Index) {
				rf.Log(LogInfo, "Sending applyCh confirmation for commit of ", rf.log[rf.getRaftLogIndex(idx)], "at index", idx)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					CommandTerm:  rf.currentTerm,
					Command:      rf.log[rf.getRaftLogIndex(idx)].Command,
				}

				// increment and update commit idx
				rf.commitIndex = idx
				idx++
			}
		}

		// set new matchIndex
		reply.NewLogsAdded = len(args.LogEntries)
	}

	// length of the log stored on this server - used for walking backwards,
	// and confirming commits
	reply.LastLogIndex = 0
	if len(rf.log) > 0 {
		reply.LastLogIndex = rf.log[len(rf.log)-1].Index
	}

	// so the caller knows we have finished
	reply.Returned = true

	// persist - we may have changed rf.currentTerm or rf.log
	data := rf.GetStateBytes(false)
	rf.persister.SaveRaftState(data)

	rf.Log(LogDebug, "Received AppendEntries from server", args.LeaderID, "term", args.LeaderTerm, "\n  - args.LogEntries:", args.LogEntries, "\n  - args.LeaderCommitIndex", args.LeaderCommitIndex, "\n  - rf.log", rf.log, "\n  - rf.commitIndex", rf.commitIndex, "\n  - args.PrevLogIndex", args.PrevLogIndex, "\n  - args.PrevLogTerm", args.PrevLogTerm, "\n  - reply.LastLogIndex", reply.LastLogIndex, "\n  - reply.ConflictingEntryTerm", reply.ConflictingEntryTerm, "\n  - reply.IndexFirstConflictingTerm", reply.IndexFirstConflictingTerm, "\n  - success:", reply.Success)
	return
}

// function to call the AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	args.LeaderID = rf.me

	// figure out prevLogIndex based on entries passed in
	// otherwise they are the commit index of the leader if we are sending no logs
	//    (so leader still finds out we're behind)
	// otherwise defaults to 0
	if len(args.LogEntries) > 0 && args.LogEntries[0].Index != 1 {
		args.PrevLogIndex = args.LogEntries[0].Index - 1
	} else if len(args.LogEntries) == 0 && rf.commitIndex > 0 {
		args.PrevLogIndex = rf.commitIndex
	}

	// if we have a nonzero PrevLogIndex (i.e. the condition above just set it),
	// retrieve it either from our log or our snapshot
	if args.PrevLogIndex > 0 {
		raftLogIdx := rf.getRaftLogIndex(args.PrevLogIndex)
		if raftLogIdx == -1 {
			rf.Log(LogDebug, "AppendEntries retrieving PrevLogTerm from snapshot since index", args.PrevLogIndex, "not present in log")
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			args.PrevLogTerm = rf.log[raftLogIdx].Term
		}
	}

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

	// new index is set based on past snapshot and current log
	idx := 1
	if len(rf.log) == 0 && rf.lastIncludedIndex > 0 {
		idx = rf.lastIncludedIndex + 1
	} else if len(rf.log) > 0 {
		idx = rf.log[len(rf.log)-1].Index + 1
	}

	// heartbeat routine will pick this up and send appropriate requests
	entry := LogEntry{
		Index:   idx,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.Log(LogDebug, "Start called, current log:", rf.log, "\n  - rf.matchIndex: ", rf.matchIndex)

	// persist - we may have changed rf.log
	data := rf.GetStateBytes(false)
	rf.persister.SaveRaftState(data)

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
	return time.Duration(minElectionTimeout+rand.Intn(maxElectionTimeout-minElectionTimeout)) * time.Millisecond
}

// heartbeatTimeoutCheck implements election timeout
// for a Raft server, by continually runs a check as to whether sending
// out RequestVote is needed due to heartbeat timeout
func (rf *Raft) heartbeatTimeoutCheck() {
	// get heartbeat check start time
	lastHeartbeatCheck := time.Now()
	i := 0
	for !rf.killed() {
		rf.mu.Lock()
		if rf.electionTimeout > 0 && rf.state == Follower {
			currentTime := time.Now()
			rf.electionTimeout -= (currentTime.Sub(lastHeartbeatCheck))
			lastHeartbeatCheck = currentTime
			if i%10 == 0 { // decrease log density
				rf.Log(LogDebug, "timeout remaining:", rf.electionTimeout)
			}
		} else if rf.state == Follower {
			// election needs to occur
			// quit this function and run the election
			rf.Log(LogInfo, "timed out as follower, running election.")
			rf.mu.Unlock()
			go rf.runElection()
			return
		}
		rf.mu.Unlock()
		i++
		time.Sleep(defaultPollInterval)
	}
}

// heartbeatAppendEntries is the leader routine for sending out
// AppendEntries requests to servers
//
// The contents of these are dictated by rf.nextIndex[] for each server
//
// The requests are send on a heartbeat interval
func (rf *Raft) heartbeatAppendEntries() {
	// make server -> reply map
	replies := make(map[int]*AppendEntriesReply)
	for servIdx := range rf.peers {
		replies[servIdx] = &AppendEntriesReply{}
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
		for servIdx := range rf.peers {
			if servIdx != rf.me {

				// successful request - update matchindex and nextindex accordingly
				if replies[servIdx].Success {
					if replies[servIdx].Success {
						rf.matchIndex[servIdx] += replies[servIdx].NewLogsAdded
						rf.nextIndex[servIdx] = rf.matchIndex[servIdx] + 1
					}

					// failed request - check for better term or decrease nextIndex
				} else if !replies[servIdx].Success && replies[servIdx].Returned {

					// we might have found out we shouldn't be the leader!
					if replies[servIdx].CurrentTerm > rf.currentTerm {
						rf.Log(LogDebug, "Detected server with higher term, stopping heartbeat and changing to follower.")
						rf.state = Follower
						rf.currentTerm = replies[servIdx].CurrentTerm

						// persist - updated current term
						data := rf.GetStateBytes(false)
						rf.persister.SaveRaftState(data)

						go rf.heartbeatTimeoutCheck()
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
					//
					// Note for 2 and 3 ... if leader does not have the relevant log
					// entries, we need to call InstallSnapshot!
					//
					rf.Log(LogInfo, "Failed to AppendEntries to server", servIdx, "\n - IndexFirstConflictingTerm", replies[servIdx].IndexFirstConflictingTerm, "\n - ConflictingEntryTerm", replies[servIdx].ConflictingEntryTerm, "\n - LastLogIndex", replies[servIdx].LastLogIndex)
					needInstallSnapshot := false
					if replies[servIdx].ConflictingEntryTerm == -1 {
						// case 1 - follower has no entry at the given location
						rf.nextIndex[servIdx] = replies[servIdx].LastLogIndex + 1

						// if our log doesn't contain the entry after the end of the
						// follower's log, need to install snapshot
						if len(rf.log) == 0 || rf.nextIndex[servIdx] < rf.log[0].Index {
							needInstallSnapshot = true
						}
					} else {
						// if not case 1, need to check we have the logs at and beyond
						// IndexFirstConflictingTerm
						raftLogIdx := rf.getRaftLogIndex(replies[servIdx].IndexFirstConflictingTerm)
						if raftLogIdx == -1 {
							needInstallSnapshot = true
						} else {
							if rf.log[raftLogIdx].Term != replies[servIdx].ConflictingEntryTerm {
								// case 2 - follower has a term not seen by leader
								rf.Log(LogDebug, "Case 2: follower has a term not seen by leader")
								rf.nextIndex[servIdx] = replies[servIdx].IndexFirstConflictingTerm
							} else {
								// case 3 - follower has a term seen by leader
								// need to go to latest entry that leader has with this term
								rf.Log(LogDebug, "Case 3: follower has a term seen by leader, finding leader's latest entry with this term \n - rf.log[", rf.log)
								rf.nextIndex[servIdx] = replies[servIdx].IndexFirstConflictingTerm
								for rf.log[rf.getRaftLogIndex(rf.nextIndex[servIdx])].Term == replies[servIdx].ConflictingEntryTerm {
									rf.nextIndex[servIdx]++
								}
							}
						}
					}

					if needInstallSnapshot {
						rf.Log(LogWarning, "Failed to AppendEntries to server", servIdx, "- need to send InstallSnapshot!")
						// nextIndex becomes the next index after the snapshot we will install
						// notice that we will then immediately send an AppendEntries request to the server,
						// and it will fail until the snapshot is installed, and we will just keep
						// resetting nextIndex
						rf.nextIndex[servIdx] = rf.lastIncludedIndex + 1
						// TODO - need to actually send the InstallSnapshot RPC
					} else {
						rf.Log(LogWarning, "rf.nextIndex for server", servIdx, "rolled back to idx", rf.nextIndex[servIdx])
					}
				}

				// send a new append entries request to the server if the last one has finished
				replies[servIdx] = &AppendEntriesReply{}
				entries := []LogEntry{}
				if len(rf.log) > 0 {
					entries = rf.log[rf.getRaftLogIndex(rf.nextIndex[servIdx]):]
				}
				args := &AppendEntriesArgs{
					LeaderTerm:        rf.currentTerm,
					LeaderCommitIndex: rf.commitIndex,
					LogEntries:        entries,
				}
				go rf.sendAppendEntries(servIdx, args, replies[servIdx])
			}
		}

		// walk up through possible new commit indices
		// update commit index
		origIndex := rf.commitIndex
		newIdx := rf.commitIndex + 1
		for newIdx <= len(rf.log) {
			replicas := 1 // already replicated in our log
			for servIdx := range rf.peers {
				if servIdx != rf.me && rf.matchIndex[servIdx] >= newIdx {
					replicas++
				}
			}
			if replicas >= int(math.Ceil(float64(len(rf.peers))/2.0)) &&
				rf.log[rf.getRaftLogIndex(newIdx)].Term == rf.currentTerm {
				rf.commitIndex = newIdx
				rf.Log(LogInfo, "Entry ", rf.log[rf.getRaftLogIndex(rf.commitIndex)], "replicated on a majority of servers. Committed to index", rf.commitIndex)
			}
			newIdx++
		}

		// send messages to applyCh for every message that was committed
		for origIndex < rf.commitIndex {
			origIndex++
			rf.Log(LogInfo, "Sending applyCh confirmation for commit of ", rf.log[rf.getRaftLogIndex(origIndex)], "at index", origIndex)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: origIndex,
				CommandTerm:  rf.currentTerm,
				Command:      rf.log[rf.getRaftLogIndex(origIndex)].Command,
			}
		}

		rf.mu.Unlock()
		time.Sleep(heartbeatSendInterval)
	}
}

// runElection turns a Raft server into a candidate
// and executes the election procedure
func (rf *Raft) runElection() {
	// get election start time
	lastElectionCheck := time.Now()

	rf.mu.Lock()
	rf.currentTerm++
	// persist - updated current term
	data := rf.GetStateBytes(false)
	rf.persister.SaveRaftState(data)
	rf.Log(LogInfo, "running as candidate")

	// set as candidate state and vote for ourselves,
	// also reset the timer
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.electionTimeout = GetRandomElectionTimeout()

	// for holding replies - we send out the requests concurrently
	peers := rf.peers
	replies := make([]*RequestVoteReply, len(peers))

	// send out requests concurrently
	for servIdx := range rf.peers {
		if servIdx != rf.me {
			args := &RequestVoteArgs{
				CandidateTerm: rf.currentTerm,
			}
			reply := &RequestVoteReply{}
			replies[servIdx] = reply
			rf.Log(LogDebug, "Sending RequestVote to server", servIdx)

			// grab last log index and term - default to snapshot if log is []
			if len(rf.log) > 0 {
				args.LastLogIndex = len(rf.log)
				args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
			} else {
				args.LastLogIndex = rf.lastIncludedIndex
				args.LastLogTerm = rf.lastIncludedTerm
			}
			go rf.sendRequestVote(servIdx, args, reply)
		}
	}
	rf.mu.Unlock()

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
			for servIdx := range rf.peers {
				// need a successful vote AND need that our term hasn't increased (e.g. if
				// since the last loop, we voted for a server with a higher term)
				if servIdx != rf.me && replies[servIdx].VoteGranted && replies[servIdx].CurrentTerm == rf.currentTerm {
					votes++
				}
			}
			// majority vote achieved - set state as leader and
			// start sending heartbeats
			if votes >= int(math.Ceil(float64(len(rf.peers))/2.0)) {
				rf.Log(LogInfo, "elected leader", "\n  - rf.log:", rf.log, "\n  - rf.commitIndex", rf.commitIndex)
				rf.state = Leader

				// get next index of the log for rf.nextIndex
				nextIdx := rf.lastIncludedIndex + 1
				if len(rf.log) > 0 {
					nextIdx = rf.log[len(rf.log)-1].Index + 1
				}

				// this volatile state is reinitialized on election
				for servIdx := range rf.peers {
					if servIdx != rf.me {
						rf.nextIndex[servIdx] = nextIdx
						rf.matchIndex[servIdx] = 0
					}
				}

				// commit a "no-op" so that entries from previous terms eligible for
				// commitment will be found and committed
				if rf.noops {
					rf.log = append(rf.log, LogEntry{
						Index:   nextIdx,
						Term:    rf.currentTerm,
						Command: "no-op",
					})
				}

				rf.mu.Unlock()
				go rf.heartbeatAppendEntries()
				return
			}
		} else {
			// no result - need to rerun election
			rf.Log(LogInfo, "timed out as candidate")
			rf.mu.Unlock()
			go rf.runElection()
			return
		}
		rf.mu.Unlock()
		time.Sleep(defaultPollInterval)
	}
}

func (rf *Raft) watchForSnapshot() {
	for !rf.killed() {
		data := rf.persister.ReadSnapshot()
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var s Snapshot
		// only log error if we know we must have a snapshot present
		// (i.e. we aren't waiting for the first one)
		if e := d.Decode(&s); e != nil && rf.lastIncludedIndex != 0 {
			rf.Log(LogError, "Error reading persistent snapshot.\n - error", e)
		} else if s.LastIncludedIndex != rf.lastIncludedIndex {
			// decode the snapshot and load relevant info into raft state
			rf.Log(LogInfo, "New snapshot spotted", "\n - lastIncludedIndex", s.LastIncludedIndex, "\n - lastIncludedTerm", s.LastIncludedTerm)

			rf.mu.Lock()
			rf.lastIncludedIndex = s.LastIncludedIndex
			rf.lastIncludedTerm = s.LastIncludedTerm
			rf.mu.Unlock()
		}
		time.Sleep(defaultPollInterval)
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
	persister *Persister, applyCh chan ApplyMsg, options ...bool) *Raft {
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

	// special - we turn on no-ops only for kvraft
	// to help with the case where a new leader won't know to commit
	// entries from previous terms
	if len(options) > 0 {
		rf.noops = options[0]
	}

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start election timeout check - server can't be a leader when created
	go rf.heartbeatTimeoutCheck()

	// trim logs if a new snapshot appears
	go rf.watchForSnapshot()

	return rf
}

// Log wraps fmt.Printf
// in order to log only when an instance hasn't been killed
func (rf *Raft) Log(level LogLevel, a ...interface{}) {
	if !rf.killed() && level >= SetLogLevel {
		pc, _, ln, _ := runtime.Caller(1)
		rp := regexp.MustCompile(".+\\.([a-zA-Z]+)")
		funcName := rp.FindStringSubmatch(runtime.FuncForPC(pc).Name())[1]
		data := append([]interface{}{level, "[ Server", rf.me, "- term", rf.currentTerm, "]", "[", funcName, ln, "]"}, a...)
		fmt.Println(data...)
	}
}
