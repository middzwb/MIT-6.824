package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
    "fmt"
    "runtime"
    "time"
    "bytes"
)

const (
    DebugClient = 1 << iota
    DebugServer
)
const Debug = 0

func dout(format string, a ...interface{}) (n int, err error) {
    _ = fmt.Sprintf(" %v", runtime.NumGoroutine())
    log.Printf(format, a...)
	return
}

const SnapCheckInterval = 100 * time.Millisecond

type OpType int
const (
    OpGet OpType = iota + 1
    OpPut
    OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Key string
    Value string
    Type OpType
    Client_id int64
    Id int64
}
func (op *Op) str_id() string {
    return fmt.Sprintf("[%v:%v]", op.Client_id, op.Id)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

    kvtable map[string]string // store all kv
    op_conds map[int]map[int]*sync.Cond // [term][index] -> cond variable

    apply_mu sync.Mutex
    apply_cond *sync.Cond
    apply []raft.ApplyMsg
    applied map[int64]int64 // key: client id; val: max applied request id

    last_received_index int
    last_received_term int
    snapshot_cond *sync.Cond
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    // duplicate detect
    kv.mu.Lock()
    defer kv.mu.Unlock()
    handle_reply := func() {
        if v, ok := kv.kvtable[args.Key]; ok {
            reply.Value = v
            reply.Err = OK
        } else {
            reply.Err = ErrNoKey
        }
    }
    if v, ok := kv.applied[args.Client_id]; ok && v >= args.Id {
        kv.dout("duplicate op %v", args.str_id())
        handle_reply()
        return
    }
    op := Op{
        Key: args.Key,
        Type: OpGet,
        Id: args.Id,
        Client_id: args.Client_id,
    }
    // scenario that before wait, apply has arrive
    index, term, is_leader := kv.rf.Start(op)
    handle_error := func() {
        reply.Err = ErrWrongLeader
        reply.LeaderId = kv.rf.GetLeader()
        reply.CurrId = kv.me
    }
    if !is_leader {
        handle_error()
        //kv.dout("get %v new leader is %v ", op.str_id(), reply.LeaderId)
        return
    }

    kv.dout("get %v %v", index, args.str_id())
    cond := kv.generate_cond(term, index)
    cond.Wait()
    if v, ok := kv.applied[args.Client_id]; !ok || v < args.Id {
        kv.dout("error order %v < %v", kv.applied[args.Client_id], args.Id)
        handle_error()
        return
    }
    // reply
    handle_reply()
    kv.dout("get succ %v", index)
    return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if v, ok := kv.applied[args.Client_id]; ok && v >= args.Id {
        kv.dout("duplicate put %v", args.str_id())
        reply.Err = OK
        return
    }
    op := Op{
        Key: args.Key,
        Value: args.Value,
        Id: args.Id,
        Client_id: args.Client_id,
    }
    if args.Op == "Append" {
        op.Type = OpAppend
    } else {
        op.Type = OpPut
    }
    index, term, is_leader := kv.rf.Start(op)
    handle_error := func() {
        reply.Err = ErrWrongLeader
        reply.LeaderId = kv.rf.GetLeader()
        reply.CurrId = kv.me
    }
    if !is_leader {
        handle_error()
        //kv.dout("put %v new leader is %v ", op.str_id(), reply.LeaderId)
        return
    }

    kv.dout("put %v %v", index, args.str_id())
    cond := kv.generate_cond(term, index)
    cond.Wait()
    if v, ok := kv.applied[args.Client_id]; !ok || v < args.Id {
        kv.dout("error order %v < %v", kv.applied[args.Client_id], args.Id)
        handle_error()
        return
    }
    // reply
    reply.Err = OK
    kv.dout("put succ %v", index)
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

    // kill all wait
    kv.mu.Lock()
    for _, m := range kv.op_conds {
        for _, cond := range m {
            cond.Broadcast()
        }
    }
    kv.snapshot_cond.Broadcast()
    kv.mu.Unlock()

    // kill reply handle
    kv.apply_mu.Lock()
    kv.apply_cond.Broadcast()
    kv.apply_mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

    // initialize kvtable
    kv.kvtable = make(map[string]string)
    kv.op_conds = make(map[int]map[int]*sync.Cond)
    kv.apply_cond = sync.NewCond(&kv.apply_mu)
    kv.apply = make([]raft.ApplyMsg, 0)
    kv.applied = make(map[int64]int64)
    kv.last_received_index = -1
    kv.last_received_term = -1
    kv.snapshot_cond = sync.NewCond(&kv.mu)

    kv.decode_snapshot(kv.rf.ReadSnapshot())

    go kv.apply_entry()
    go kv.handle_apply_entry()
    if maxraftstate != -1 {
        go kv.snapshot_entry()
    }
	return kv
}

// my code
func (kv *KVServer) dout(format string, a ...interface{}) {
    tmp := fmt.Sprintf("[S_%v]", kv.me)
	if Debug & DebugServer == DebugServer {
		dout(tmp + format, a...)
	}
	return
}

func (kv *KVServer) apply_entry() {
    for {
        msg, ok := <-kv.applyCh

        if !ok {
            kv.dout("chan closed apply_entry exit")
            return
        }

        kv.apply_mu.Lock()
        kv.apply = append(kv.apply, msg)
        if len(kv.apply) == 1 {
            kv.apply_cond.Broadcast()
        }
        kv.apply_mu.Unlock()
        //// apply to state machine
        //op := msg.Command.(Op)
        //kv.mu.Lock()
        //kv.apply(&op)
        //// leader notify thread waiting for
        //if cond, ok := kv.op_conds[msg.CommandIndex]; ok {
        //    kv.dout("apply %v", msg.CommandIndex)
        //    cond.Broadcast()
        //}
        //kv.mu.Unlock()
    }
}

// idempotent, duplicated detect
func (kv *KVServer) apply_to_table(op *Op) {
    if v, ok := kv.applied[op.Client_id]; ok && v >= op.Id {
        kv.dout("duplicate apply %v", op.str_id())
        return
    }
    //kv.dout("apply %v [%v->%v]", op.Client_id, kv.applied[op.Client_id], op.Id)
    kv.applied[op.Client_id] = op.Id
    switch op.Type {
    case OpGet:
    case OpPut:
        //kv.dout("apply put %v", op)
        kv.kvtable[op.Key] = op.Value
    case OpAppend:
        kv.kvtable[op.Key] += op.Value
    }
}

func (kv *KVServer) handle_apply_entry() {
    for !kv.killed() {
        kv.apply_mu.Lock()
        if len(kv.apply) == 0 {
            kv.apply_cond.Wait()
        }
        tmp := kv.apply
        kv.apply = make([]raft.ApplyMsg, 0)
        kv.apply_mu.Unlock()

        kv.mu.Lock()
        // apply to state machine
        for i, _ := range tmp {
            msg := &tmp[i]
            if msg.CommandValid {
                // index is monotonic increase
                kv.assert(msg.CommandIndex > kv.last_received_index, "Error index %v>%v", msg.CommandIndex, kv.last_received_index)
                kv.last_received_term = msg.CommandTerm
                kv.last_received_index = msg.CommandIndex
                op := msg.Command.(Op)
                kv.apply_to_table(&op)

                if kv.maxraftstate <= kv.rf.RaftStateSize() {
                    kv.snapshot_cond.Broadcast()
                }
            } else {
                // maybe need retry to commit log entry
                if msg.Snapshot {
                    data := msg.Command.([]byte)
                    kv.decode_snapshot(data)
                }
            }
            term := tmp[i].CommandTerm
            index := tmp[i].CommandIndex
            if cond, ok := kv.op_conds[term][index]; ok {
                kv.dout("apply %v", index)
                cond.Broadcast()
            }

            // notify ex-term
            for i, v := range kv.op_conds {
                if i < term {
                    kv.dout("ex-term %v < %v", i, term)
                    for _, cond := range v {
                        cond.Broadcast()
                    }
                    // unlike c++, delete in range-for is safe
                    delete(kv.op_conds, i)
                }
            }
        }
        kv.mu.Unlock()
    }
}

func (kv *KVServer) generate_cond(term int, index int) *sync.Cond {
    cond := sync.NewCond(&kv.mu)
    if kv.op_conds[term] == nil {
        kv.op_conds[term] = make(map[int]*sync.Cond)
    }
    kv.op_conds[term][index] = cond
    return cond
}

func (kv *KVServer) snapshot_entry() {
    kv.mu.Lock()
    for !kv.killed() {
        kv.snapshot_cond.Wait()
        if kv.maxraftstate <= kv.rf.RaftStateSize() {
            kv.dout("start snapshot %v>%v", kv.rf.RaftStateSize(), kv.maxraftstate)
            // perform snapshot
            // block other operation
            w := new(bytes.Buffer)
            e := labgob.NewEncoder(w)
            e.Encode(kv.kvtable)
            e.Encode(kv.applied) // detect duplicate after restart
            e.Encode(kv.last_received_index)
            e.Encode(kv.last_received_term)
            index := kv.last_received_index
            term := kv.last_received_term
            kv.mu.Unlock()
            kv.rf.SaveStateAndSnapshot(w.Bytes(), index, term)
            kv.mu.Lock()
        }
        //time.Sleep(SnapCheckInterval)
    }
    kv.mu.Unlock()
}

func (kv *KVServer) decode_snapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
    kvtable := make(map[string]string)
    applied := make(map[int64]int64)
    index := 0
    term := 0
	if d.Decode(&kvtable) != nil ||
       d.Decode(&applied) != nil ||
       d.Decode(&index) != nil ||
       d.Decode(&term) != nil {
        kv.dout("readPersist error")
	} else {
        //kv.assert(index > kv.last_received_index, "Error decode index %v>%v", index, kv.last_received_index)
        if index <= kv.last_received_index {
            kv.dout("WARNING: old snapshot %v<%v discard", index, kv.last_received_index)
            return
        }
        kv.kvtable = kvtable
        kv.applied = applied
        kv.last_received_index = index
        kv.last_received_term = term
        kv.dout("read persist table size=%v, applied=%v, snapshot[%v:%v]", len(kvtable), len(applied), index, term)
	}
}

func (kv *KVServer) assert(condition bool, msg string, args ...interface{}) {
    if !condition {
        tmp := fmt.Sprintf("[S_%v]", kv.me)
        log.Fatalf(tmp + msg, args...)
    }
}
