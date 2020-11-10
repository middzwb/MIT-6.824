package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Client_id int64
    Id int64
}
func (args *PutAppendArgs) str_id() string {
    return fmt.Sprintf("[%v:%v]", args.Client_id, args.Id)
}

type PutAppendReply struct {
	Err Err
    LeaderId int
    CurrId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    Client_id int64
    Id int64
}
func (args *GetArgs) str_id() string {
    return fmt.Sprintf("[%v:%v]", args.Client_id, args.Id)
}

type GetReply struct {
	Err   Err
	Value string
    LeaderId int
    CurrId int
}
