package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderIdx int
	mu        sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// just pick 0 as the initial leader
	ck.leaderIdx = 0

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

	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}

	// keep trying operation until succeeds
	for {
		ok := ck.servers[ck.leaderIdx].Call("KVServer.Get", &args, &reply)
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
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}

	// keep trying operation until succeeds
	for {
		ok := ck.servers[ck.leaderIdx].Call("KVServer.PutAppend", &args, &reply)
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
