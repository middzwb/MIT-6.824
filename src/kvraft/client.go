package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "fmt"

///*********************************
// suppose client is single thread
// It's OK to assume that a client will make only one call into a Clerk at a time.
///*********************************

const (
    RetryCount = 5
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

    leader_id int // current leader server
    correct_servers []bool
    me int64 // client id
    next_req_id int64 // unique request id
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
	// You'll have to add code here.
    ck.leader_id = 0
    ck.me = nrand()
    ck.next_req_id = 0
    ck.correct_servers = make([]bool, len(servers))
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
    args := &GetArgs{
        Key: key,
        Id: ck.next_req_id,
        Client_id : ck.me,
    }
    tmp := ck.next_req_id
    ck.next_req_id += 1
    ck.dout("get req=%v", tmp)
    defer ck.dout("get succ")
    retry := 0
    for {
        reply := &GetReply{}
        if ok := ck.servers[ck.leader_id].Call("KVServer.Get", args, reply); !ok {
            retry += 1
            if retry == RetryCount {
                // maybe network partition
                // change other server
                ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
                retry = 0
                //ck.dout("retry is max ,change leader")
            }
            continue
        }
        switch reply.Err {
        case OK:
            return reply.Value
        case ErrNoKey:
            return ""
        case ErrWrongLeader:
            ck.revise_leader(reply.CurrId, reply.LeaderId)
        }
        //ck.dout("re-send get to %v->%v, req=%v, order=%v", reply.LeaderId, ck.leader_id, tmp, ck.correct_servers)
    }
	return ""
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
	// You will have to modify this function.
    args := &PutAppendArgs{
        Key: key,
        Value: value,
        Op: op,
        Id: ck.next_req_id,
        Client_id: ck.me,
    }
    tmp := ck.next_req_id
    ck.next_req_id += 1
    ck.dout("put %v", tmp)
    defer ck.dout("put succ")
    retry := 0
    for {
        reply := &PutAppendReply{}
        //ck.dout("send put to %v %v", ck.leader_id, tmp)
        if ok := ck.servers[ck.leader_id].Call("KVServer.PutAppend", args, reply); !ok {
            retry += 1
            if retry == RetryCount {
                // maybe network partition
                // change other server
                ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
                retry = 0
                //ck.dout("retry is max ,change leader")
            }
            continue
        }
        switch reply.Err {
        case OK:
            return
        case ErrNoKey:
            return
        case ErrWrongLeader:
            // revise server order
            ck.revise_leader(reply.CurrId, reply.LeaderId)
            //ck.dout("re-send put to %v->%v, req=%v, order=%v", reply.LeaderId, ck.leader_id, tmp, ck.correct_servers)
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) dout(format string, a ...interface{}) {
    tmp := fmt.Sprintf("[C/%v]", ck.me)
	if Debug & DebugClient == DebugClient {
		dout(tmp + format, a...)
	}
	return
}

func (ck *Clerk) revise_leader(s int, new_leader int) {
    if  s != ck.leader_id {
        ck.servers[s], ck.servers[ck.leader_id] = ck.servers[ck.leader_id], ck.servers[s]
        //ck.dout("change order %v->%v", ck.leader_id, s)
        ck.correct_servers[s] = true
    }
    if !ck.correct_servers[new_leader] {
        ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
    } else {
        ck.leader_id = new_leader
    }
}
