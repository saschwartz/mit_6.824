package kvraft

import (
	"fmt"
	"math/rand"
	"regexp"
	"runtime"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	leaderIdx    int
	mu           sync.Mutex
	clientSerial int
	clientID     string
}

func randString(length int) string {
	alpha := "abcdefghijklmnopqrztuvwxyz"
	r := make([]byte, length)
	for i := range r {
		r[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(r)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// just pick 0 as the initial leader
	ck.leaderIdx = 0

	// first request has client serial 0, then we inc
	ck.clientSerial = 0

	// need a unique client id
	ck.clientID = randString(5)

	ck.Log(LogDebug, "Client started.")

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// lock around incrementing serial so no funny business
	// for concurrent client requests
	ck.mu.Lock()
	ck.clientSerial++
	args := GetArgs{Key: key, ClientID: ck.clientID, ClientSerial: ck.clientSerial}
	ck.mu.Unlock()

	reply := GetReply{}

	// keep trying operation until succeeds
	for {
		ck.Log(LogDebug, "Client sending [ leaderIdx", ck.leaderIdx, "] [ Request", ck.clientSerial, "] Get", key)

		ok := ck.servers[ck.leaderIdx].Call("KVServer.Get", &args, &reply)

		ck.Log(LogDebug, "RPC returned [ leaderIdx", ck.leaderIdx, "] [ Request", ck.clientSerial, "] Get", key, "\n - ok", ok, "\n - reply.Value", reply.Value, "\n - reply.Err", reply.Err)

		if ok && reply.Err != "ErrWrongLeader" {
			// all good, return value
			return reply.Value
		} else {
			// need to change which server we hit
			ck.mu.Lock()
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			ck.mu.Unlock()
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// lock around incrementing serial so no funny business
	// for concurrent client requests
	ck.mu.Lock()
	ck.clientSerial++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, ClientSerial: ck.clientSerial}
	ck.mu.Unlock()

	reply := PutAppendReply{}

	// keep trying operation until succeeds
	for {
		ck.Log(LogDebug, "Client sending [ leaderIdx", ck.leaderIdx, "] [ Request", ck.clientSerial, "]", op, key, value)

		ok := ck.servers[ck.leaderIdx].Call("KVServer.PutAppend", &args, &reply)

		ck.Log(LogDebug, "RPC returned [ leaderIdx", ck.leaderIdx, "] [ Request", ck.clientSerial, "]", op, key, value, "\n - ok", ok, "\n - reply.Err", reply.Err)

		if ok && reply.Err != "ErrWrongLeader" {
			// all good, return
			return
		} else {
			// need to change which server we hit
			ck.mu.Lock()
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			ck.mu.Unlock()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// Log wraps fmt.Printf
// in order to log only when an instance hasn't been killed
func (ck *Clerk) Log(level LogLevel, a ...interface{}) {
	if level >= SetLogLevel {
		pc, _, ln, _ := runtime.Caller(1)
		rp := regexp.MustCompile(".+\\.([a-zA-Z]+)")
		funcName := rp.FindStringSubmatch(runtime.FuncForPC(pc).Name())[1]
		data := append([]interface{}{level, "[ Clerk ", ck.clientID, "]", "[", funcName, ln, "]"}, a...)
		fmt.Println(data...)
	}
}
