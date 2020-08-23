package kvraft

import (
	"bytes"
	"fmt"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

// how long to wait for an op to appear at an index
// (it may never appear due to a server crashing)
const waitForOpTimeout = time.Duration(5000) * time.Millisecond

// to give raft time to actually delete logs between polling times
const snapshotPollInterval = time.Duration(500) * time.Millisecond

// at what % of maxraftsize should we snapshot
const maxStateThreshold float64 = 0.85

// Op is the 'Command' stored in each raft log entry
type Op struct {
	OpType       string // "Append" / "Put" / "Get"
	Key          string // always present
	Value        string // only for "Append" / "Put"
	ClientID     string // which client sent this
	ClientSerial int    // client's serial number for this command
}

// KVAppliedOp stores information about an Op that has
// been applied to the KV state machine
// our RPC methods use this to determine client response
type KVAppliedOp struct {
	Index int
	Term  int    // need this for snapshotting
	KVOp  Op     // this is contained in RaftMsg already but this makes typing easier
	Value string // string if "Get", else empty
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// persister that kv.rf also contains
	persister *raft.Persister

	// actual app state is just a string -> string map
	store map[string]string

	// this caches responses in case of crash recovery
	// e.g. if we commit a message but fail before responding to client
	// then we don't want to recommit.
	// so we store a map of clientID -> (latest command serial, response)
	latestResponse map[string]KVAppliedOp

	// last index and term we've processed and applied to state
	// useful for snapshotting
	lastIndexApplied int
	lastTermApplied  int

	// just so we can quickly check if snapshotting is worth it
	lastIncludedIndex int
}

// Snapshot is the persistent snapshot to encode and decode
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	LatestResponse    map[string]KVAppliedOp // to avoid duplicates after snapshot installation
	Store             map[string]string
}

// WaitForAppliedOp is a helper function to continually loop
// and wait for an applied op with ClientID and ClientSerial
// to appear in the log at idx
// it then returns true if this op was at the index, or false if
// another op was at the index
//
// it will timeout after a certain duration and just return false
//
func (kv *KVServer) WaitForAppliedOp(idx int, clientID string, clientSerial int, timeout time.Duration) (bool, KVAppliedOp) {
	lastTimeoutCheck := time.Now()
	for {

		// check timeout
		currentTime := time.Now()
		timeout -= (currentTime.Sub(lastTimeoutCheck))
		lastTimeoutCheck = currentTime
		if timeout < 0 {
			kv.Log(LogInfo, "Timed out while waiting for op from clientID", clientID, "clientSerial", clientSerial, "to appear at idx", idx)
			return false, KVAppliedOp{}
		}

		kv.mu.Lock()
		resp, ok := kv.latestResponse[clientID]
		if ok && resp.KVOp.ClientSerial == clientSerial {
			kv.Log(LogInfo, "Saw expected op from clientID", clientID, "clientSerial", clientSerial, "appear at idx", idx, "\n - KVOp", resp.KVOp)
			kv.mu.Unlock()
			return true, resp
		} else if ok && resp.KVOp.ClientSerial != clientSerial && kv.latestResponse[clientID].Index >= idx {
			kv.Log(LogInfo, "Saw different op from clientID", clientID, "clientSerial", clientSerial, "appear at idx", "\n - KVOp", resp.KVOp)
			kv.mu.Unlock()
			return false, KVAppliedOp{}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Log(LogDebug, "Received [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key)

	// if resp is cached, serve it to avoid recommitting
	kv.mu.Lock()
	if resp, ok := kv.latestResponse[args.ClientID]; ok && resp.KVOp.ClientSerial == args.ClientSerial {
		reply.Err = resp.Err
		reply.Value = resp.Value

		kv.Log(LogInfo, "Request already served, using cache [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key, "\n - reply.Value", reply.Value, "\n - reply.Err", reply.Err)

		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// else send request and wait for response
	expectedIdx, _, isLeader := kv.rf.Start(Op{
		OpType:       "Get",
		Key:          args.Key,
		ClientID:     args.ClientID,
		ClientSerial: args.ClientSerial,
	})

	kv.Log(LogDebug, "Called kv.rf.Start on [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key, "\n - expectedIdx", expectedIdx, "\n - isLeader", isLeader)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// we keep reading info about committed ops until either
	// we see op is committed and tell client success,
	// or see something else got committed there and tell client
	// ErrWrongLeader (so it will retry)
	success, appliedOp := kv.WaitForAppliedOp(expectedIdx, args.ClientID, args.ClientSerial, waitForOpTimeout)
	if success {
		// we saw our operation. success!
		kv.mu.Lock()
		reply.Err = appliedOp.Err
		reply.Value = appliedOp.Value
		kv.mu.Unlock()
	} else {
		// some other op got committed there. failure
		reply.Err = ErrWrongLeader
	}

	kv.Log(LogInfo, "Responded to [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key, "\n - reply.Value:", reply.Value, "\n - reply.Err", reply.Err)

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Log(LogDebug, "Received [ Client", args.ClientID, "] [ Request", args.ClientSerial, "]", args.Op, args.Key, args.Value)

	// if resp is cached, serve it to avoid recommitting
	kv.mu.Lock()
	if resp, ok := kv.latestResponse[args.ClientID]; ok && resp.KVOp.ClientSerial == args.ClientSerial {
		reply.Err = resp.Err

		kv.Log(LogInfo, "Request already served, using cache [ Client", args.ClientID, "] [ Request", args.ClientSerial, "]", args.Op, args.Key, args.Value, "\n - reply.Err", reply.Err)

		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// else send request and wait for response
	expectedIdx, _, isLeader := kv.rf.Start(Op{
		OpType:       args.Op,
		Key:          args.Key,
		Value:        args.Value,
		ClientID:     args.ClientID,
		ClientSerial: args.ClientSerial,
	})

	kv.Log(LogDebug, "Called kv.rf.Start on [ Client", args.ClientID, "] [ Request", args.ClientSerial, "]", args.Op, args.Key, args.Value, "\n - expectedIdx", expectedIdx, "\n - isLeader", isLeader)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// we keep reading info about committed ops until either
	// we see op is committed and tell client success,
	// or see something else got committed there and tell client
	// ErrWrongLeader (so it will retry)
	success, appliedOp := kv.WaitForAppliedOp(expectedIdx, args.ClientID, args.ClientSerial, waitForOpTimeout)
	if success {
		// we saw our operation. success!
		kv.mu.Lock()
		reply.Err = appliedOp.Err
		kv.mu.Unlock()
	} else {
		// some other op got committed there. failure
		reply.Err = ErrWrongLeader
	}

	kv.Log(LogInfo, "Responded to [ Client", args.ClientID, "] [ Request", args.ClientSerial, "]", args.Op, args.Key, args.Value, "\n - reply.Err", reply.Err)

	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// ApplyOp is a helper function to
// apply an operation and return the applied operation
// don't lock - that's handled by the caller
func (kv *KVServer) ApplyOp(op Op, opIndex int, opTerm int) KVAppliedOp {
	err := Err(OK)
	val, keyOk := kv.store[op.Key]

	// only do the store modification if we haven't already
	// seen this request (can happen if a leader crashes after comitting
	// but before responding to client)
	if prevOp, ok := kv.latestResponse[op.ClientID]; !ok || prevOp.KVOp.ClientSerial != op.ClientSerial {
		if op.OpType == "Get" && !keyOk {
			// Get
			err = Err(ErrNoKey)
		} else if op.OpType == "Put" {
			// Put
			kv.store[op.Key] = op.Value
		} else if op.OpType == "Append" {
			// Append (may need to just Put)
			if !keyOk {
				kv.store[op.Key] = op.Value // should this be ErrNoKey?
			} else {
				kv.store[op.Key] += op.Value
			}
		}
		kv.Log(LogInfo, "Store updated:", kv.store)
	} else {
		kv.Log(LogDebug, "Skipping store update, detected duplicate command.")
	}

	// create the op, add the Value field if a Get
	appliedOp := KVAppliedOp{
		Term:  opTerm,
		Index: opIndex,
		KVOp:  op,
		Err:   err,
	}
	if op.OpType == "Get" {
		appliedOp.Value = val
	}
	kv.Log(LogDebug, "Applied op", appliedOp)

	// update tracking of latest op applied
	kv.lastIndexApplied = appliedOp.Index
	kv.lastTermApplied = appliedOp.Term
	return appliedOp
}

// ScanApplyCh is a long running goroutine that continuously
// monitors the relevant raft server's applyCh
//
// When it finds new committed logs it applies them to the kv server state
// and also puts them in the kv.latestResponse map so that RPCs can return
//
func (kv *KVServer) ScanApplyCh() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.Log(LogInfo, "Received commit on applyCh:", msg)

		// see what type of message is is and respond accordingly
		if op, ok := msg.Command.(Op); ok {
			// message is an applyable op. Apply it to kv state
			// ensure to cache response to prevent re-applications later on!
			appliedOp := kv.ApplyOp(op, msg.CommandIndex, msg.CommandTerm)
			kv.mu.Lock()
			kv.latestResponse[appliedOp.KVOp.ClientID] = appliedOp
			kv.mu.Unlock()
			kv.Log(LogDebug, "New request processed for client", appliedOp.KVOp.ClientID, "\n - appliedOp", appliedOp)
		} else if len(msg.Snapshot) > 0 {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var s Snapshot
			if e := d.Decode(&s); e != nil {
				kv.Log(LogError, "Saw an applyMsg with Snapshot but error reading it.\n - error", e)
			} else {
				kv.Log(LogInfo, "InstallSnapshot found on applyMsg Ch.\n - LastIncludedIndex", s.LastIncludedIndex, "\n - LastIncludedTerm", s.LastIncludedTerm, "\n - Store", s.Store, "\n - LatestResponse", s.LatestResponse)

				// only apply the update to state if it is a more recent snapshot
				kv.mu.Lock()
				if s.LastIncludedIndex > kv.lastIndexApplied {
					kv.Log(LogInfo, "InstallSnapshot more up to date. Applying store and discarding log.")
					kv.store = s.Store
					kv.latestResponse = s.LatestResponse
					stateBytes := kv.rf.TrimLog(s.LastIncludedIndex, s.LastIncludedTerm)
					kv.persister.SaveStateAndSnapshot(stateBytes, msg.Snapshot)
					kv.lastIncludedIndex = s.LastIncludedIndex
					kv.lastIndexApplied = kv.lastIncludedIndex
					kv.Log(LogDebug, "Finished installing snapshot.")
				} else {
					kv.Log(LogInfo, "InstallSnapshot describes a prefix of our log. Ignoring.")
				}
				kv.mu.Unlock()
			}
		} else {
			// must be a no-op
			kv.Log(LogDebug, "Saw a no-op - skipping state machine update and waiting for next message.")
		}
	}
}

func (kv *KVServer) snapshotWatch() {
	for !kv.killed() {

		// snapshot if we have exceeded state size AND
		// we have additional ops to commit
		if kv.maxraftstate > 0 &&
			float64(kv.persister.RaftStateSize()) > float64(kv.maxraftstate)*maxStateThreshold && kv.lastIndexApplied > kv.lastIncludedIndex {
			kv.Log(LogInfo, "maxraftstate size exceeded, need to snapshot.", "\n - kv.persister.RaftStateSize()", kv.persister.RaftStateSize(), "\n - kv.maxraftstate*maxStateThreshold", float64(kv.maxraftstate)*maxStateThreshold, "\n - kv.lastIncludedIndex", kv.lastIncludedIndex)

			// create snapshot bytes. we store the store, our latestResponse
			// cache (for consistent duplicate detection across snapshots)
			// and the lastIncludedIndex and lastIncludedTerm for our
			// snapshot (to keep raft appendEntries working for the next
			// entry it gets post snapshot)
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			snapshot := Snapshot{
				LastIncludedIndex: kv.lastIndexApplied,
				LastIncludedTerm:  kv.lastTermApplied,
				Store:             kv.store,
				LatestResponse:    kv.latestResponse,
			}
			e.Encode(snapshot)
			snapshotBytes := w.Bytes()

			// fetch our state and trim raft logs, then save both snapshot and state
			stateBytes := kv.rf.TrimLog(snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
			kv.persister.SaveStateAndSnapshot(stateBytes, snapshotBytes)
			kv.lastIncludedIndex = snapshot.LastIncludedIndex

			kv.Log(LogInfo, "Saved raft state and snapshot", "\n - lastIncludedIndex", snapshot.LastIncludedIndex, "\n - lastIncludedTerm", snapshot.LastIncludedTerm, "\n - kv.persister.RaftStateSize()", kv.persister.RaftStateSize())

			kv.mu.Unlock()
		}
		time.Sleep(snapshotPollInterval)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, true)

	// store our persister
	kv.persister = persister

	// initialize maps
	kv.latestResponse = make(map[string]KVAppliedOp)
	kv.store = make(map[string]string)

	// load snapshot if it exists
	snapshotBytes := kv.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshotBytes)
	d := labgob.NewDecoder(r)
	var s Snapshot
	if e := d.Decode(&s); e != nil {
		kv.Log(LogInfo, "Could not read a valid Snapshot on startup.\n - error", e)
	} else {
		kv.store = s.Store
		kv.latestResponse = s.LatestResponse
		stateBytes := kv.rf.TrimLog(s.LastIncludedIndex, s.LastIncludedTerm)
		kv.persister.SaveStateAndSnapshot(stateBytes, snapshotBytes)
		kv.lastIncludedIndex = s.LastIncludedIndex
		kv.lastIndexApplied = kv.lastIncludedIndex
	}

	kv.Log(LogDebug, "Server started.")

	// start watching applyCh only after replay is complete
	go kv.ScanApplyCh()

	// start snapshotting routine
	go kv.snapshotWatch()

	return kv
}

// Log wraps fmt.Printf
// in order to log only when an instance hasn't been killed
func (kv *KVServer) Log(level LogLevel, a ...interface{}) {
	if !kv.killed() && level >= SetLogLevel {
		pc, _, ln, _ := runtime.Caller(1)
		rp := regexp.MustCompile(".+\\.([a-zA-Z]+)")
		funcName := rp.FindStringSubmatch(runtime.FuncForPC(pc).Name())[1]
		data := append([]interface{}{level, "[ KV", kv.me, "]", "[", funcName, ln, "]"}, a...)
		fmt.Println(data...)
	}
}
