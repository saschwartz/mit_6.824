package kvraft

import (
	"fmt"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

type Op struct {
	OpType       string // "Append" / "Put" / "Get"
	Key          string // always present
	Value        string // only for "Append" / "Put"
	ClientID     string // which client sent this
	ClientSerial int    // client's serial number for this command
}

// information about an Op that has been applied to the KV
// state machine
// our RPC methods use this to determine client response
type KVAppliedOp struct {
	Index int
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

	// Your definitions here.

	// actual app state is just a string -> string map
	store map[string]string

	// log of committed ops
	// should mirror the underlying raft log
	// but also includes info of kv state effects
	appliedOpsLog []KVAppliedOp

	// this caches responses in case of crash recovery
	// e.g. if we commit a message but fail before responding to client
	// then we don't want to recommit.
	// so we store a map of clientID -> (latest command serial, response)
	latestResponse map[string]KVAppliedOp
}

// WaitForAppliedOp is a helper function to continually loop
// and wait for an applied op with ClientID and ClientSerial
// to appear in the log at idx
// it then returns true if this op was at the index, or false if
// another op was at the index
func (kv *KVServer) WaitForAppliedOp(idx int, clientID string, clientSerial int) bool {
	for {
		kv.mu.Lock()
		if len(kv.appliedOpsLog) >= idx {
			kv.mu.Unlock()
			return (kv.appliedOpsLog[idx-1].KVOp.ClientSerial == clientSerial &&
				kv.appliedOpsLog[idx-1].KVOp.ClientID == clientID)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.Log(LogInfo, "Received [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key)

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
	appliedOp := kv.WaitForAppliedOp(expectedIdx, args.ClientID, args.ClientSerial)
	if appliedOp {
		// we saw our operation. success!
		kv.mu.Lock()
		reply.Err = kv.appliedOpsLog[expectedIdx-1].Err
		reply.Value = kv.appliedOpsLog[expectedIdx-1].Value
		kv.mu.Unlock()
	} else {
		// some other op got committed there. failure
		reply.Err = ErrWrongLeader
	}

	kv.Log(LogInfo, "Responded to [ Client", args.ClientID, "] [ Request", args.ClientSerial, "] Get", args.Key, "\n - reply.Value:", reply.Value, "\n - reply.Err", reply.Err)

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Log(LogInfo, "Received [ Client", args.ClientID, "] [ Request", args.ClientSerial, "]", args.Op, args.Key, args.Value)

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
	appliedOp := kv.WaitForAppliedOp(expectedIdx, args.ClientID, args.ClientSerial)
	if appliedOp {
		// we saw our operation. success!
		kv.mu.Lock()
		reply.Err = kv.appliedOpsLog[expectedIdx-1].Err
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
func (kv *KVServer) ApplyOp(op Op) KVAppliedOp {
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
		Index: len(kv.appliedOpsLog) + 1,
		KVOp:  op,
		Err:   err,
	}
	if op.OpType == "Get" {
		appliedOp.Value = val
	}
	kv.Log(LogDebug, "Applied op", appliedOp)
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
		kv.Log(LogDebug, "Received commit on applyCh:", msg)

		op, ok := msg.Command.(Op)
		if !ok {
			// might find a no-op - just log it, add it to log and keep looping
			kv.Log(LogDebug, "Saw a no-op - skipping state machine update and waiting for next message.")
			kv.mu.Lock()
			kv.appliedOpsLog = append(kv.appliedOpsLog, KVAppliedOp{
				Index: len(kv.appliedOpsLog) + 1,
				KVOp:  Op{OpType: "no-op"},
			})
			kv.mu.Unlock()
			continue
		}

		// if a valid op, apply op to state and add to log
		// ensure to cache response to prevent re-applications later on!
		appliedOp := kv.ApplyOp(op)
		kv.mu.Lock()
		kv.appliedOpsLog = append(kv.appliedOpsLog, appliedOp)
		kv.latestResponse[appliedOp.KVOp.ClientID] = kv.appliedOpsLog[appliedOp.Index-1]
		kv.mu.Unlock()

		kv.Log(LogDebug, "Committed ops log extended:", kv.appliedOpsLog)
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// initialize maps
	kv.latestResponse = make(map[string]KVAppliedOp)
	kv.store = make(map[string]string)

	kv.Log(LogDebug, "Server started.")

	// start watching applyCh only after replay is complete
	go kv.ScanApplyCh()

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
