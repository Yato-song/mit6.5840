package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	req := &rpc.GetArgs{Key: key}
	resp := &rpc.GetReply{}
	// retry if doesn't receive a reply
	for {
		if ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", req, resp); ok {
			if resp.Err == rpc.ErrWrongLeader {
				// RPC可达 但非leader
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue
			}

			return resp.Value, resp.Version, resp.Err
		}
		// RPC不可达
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	req := &rpc.PutArgs{Key: key, Value: value, Version: version}
	// resp := &rpc.PutReply{}

	uncertain := false
	for {
		resp := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", req, resp)

		// RPC不可达
		if !ok {
			// uncertain = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// RPC可达 && Error leader
		if resp.Err == rpc.ErrWrongLeader {
			// ErrWrongLeader可能是raft已commit成功，但DoOp返回前发生了leader change，导致返回ErrWrongLeader
			uncertain = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if resp.Err == rpc.ErrVersion && uncertain {
			return rpc.ErrMaybe
		}
		return resp.Err
	}
}
