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

type CachedClientResponse struct {
	ClientSerial int
	OpType       string
	Value        string // string if "Get", else empty
	Err          Err
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

	// this caches responses in case of crash recovery
	// e.g. if we commit a message but fail before responding to client
	// then we don't want to recommit.
	// so we store a map of clientID -> (latest command serial, response)
	latestResponse map[string]*CachedClientResponse
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// if not cached, send request
	if resp, ok := kv.latestResponse[args.ClientID]; !ok || resp.ClientSerial != args.ClientSerial {
		_, _, isLeader := kv.rf.Start(Op{
			OpType:       "Get",
			Key:          args.Key,
			ClientID:     args.ClientID,
			ClientSerial: args.ClientSerial,
		})
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	}

	// then loop until it shows up on cache and return
	for {
		kv.mu.Lock()
		if resp, ok := kv.latestResponse[args.ClientID]; ok && resp.ClientSerial == args.ClientSerial {
			reply.Err = resp.Err
			reply.Value = resp.Value
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// if not cached, send request
	if resp, ok := kv.latestResponse[args.ClientID]; !ok || resp.ClientSerial != args.ClientSerial {
		_, _, isLeader := kv.rf.Start(Op{
			OpType:       args.Op,
			Key:          args.Key,
			Value:        args.Value,
			ClientID:     args.ClientID,
			ClientSerial: args.ClientSerial,
		})
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
	}

	// then loop until it shows up on cache and return
	for {
		kv.mu.Lock()
		if resp, ok := kv.latestResponse[args.ClientID]; ok && resp.ClientSerial == args.ClientSerial {
			reply.Err = resp.Err
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
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

		kv.mu.Lock()
		op, ok := msg.Command.(Op)
		err := Err(OK)

		// Apply our operation to kv state
		val, ok := kv.store[op.Key]
		if op.OpType == "Get" && !ok {
			// if a Get, fetch the key from store
			err = Err(ErrNoKey)
		} else if op.OpType == "Put" {
			// if a Put, update the key in the store
			kv.store[op.Key] = op.Value
		} else if op.OpType == "Append" {
			// if an Append, either append to key or Put
			if !ok {
				kv.store[op.Key] = op.Value
			} else {
				kv.store[op.Key] += op.Value
			}
		}

		// mark this message as processed in our client cache
		// this will also trigger our RPC to respond to the Clerk
		kv.latestResponse[op.ClientID] = &CachedClientResponse{
			ClientSerial: op.ClientSerial,
			OpType:       op.OpType,
			Err:          err,
		}
		if op.OpType == "Get" {
			kv.latestResponse[op.ClientID].Value = val
		}
		kv.Log(LogDebug, "kv.latestResponse:", kv.latestResponse)
		kv.mu.Unlock()
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
	kv.latestResponse = make(map[string]*CachedClientResponse)
	kv.store = make(map[string]string)

	kv.Log(LogInfo, "Server started.")

	// start watching applyCh
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
		data := append([]interface{}{level, "[ KV ", kv.me, "]", "[", funcName, ln, "]"}, a...)
		fmt.Println(data...)
	}
}
