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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "fmt"
import "sort"

import "bytes"
import "../labgob"



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
}

type LogEntry struct {
    Command interface{}
    Term int
    Index int
}

func (e LogEntry) String() string {
    return fmt.Sprintf("[%v:%v]", e.Index, e.Term)
}
func (e *LogEntry) str() string {
    return fmt.Sprintf("[%v:%v]", e.Index, e.Term)
}

type RaftState int
const (
    STATE_FOLLOWER RaftState = iota + 1
    STATE_CANDIDATE
    STATE_LEADER
)

//
// A Go object implementing a single Raft peer.
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

    curr_term int
    voted_for int
    log []LogEntry

    commit_index int // 
    last_applied int

    next_index []int // AppendEntries
    match_index []int // used for update commitIndex

    timeout time.Time // heartbeat timeout and election timeout
    delta time.Duration // sleep for timeout check
    received_vote int // received vote to candidate int current term
    state RaftState //
    cond *sync.Cond // notify heartbeat send thread
    majority int
    timeout_cond *sync.Cond // notify timeout check thread

    applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.curr_term
    isleader = rf.is_leader()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.curr_term)
    e.Encode(rf.voted_for)
    e.Encode(rf.log)
    rf.persister.SaveRaftState(w.Bytes())
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
    term := 0
    voted_for := 0
    log := make([]LogEntry, 0)
	if d.Decode(&term) != nil ||
	   d.Decode(&voted_for) != nil ||
       d.Decode(&log) != nil {
        rf.dout("readPersist error")
	} else {
        rf.curr_term = term
        rf.voted_for = voted_for
        rf.log = log
        rf.dout("read persist t=%v v=%v log_len=%v", term, voted_for, len(log))
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry // encode/decode
    LeaderCommit int // notify follower to commit log entry
}
type AppendEntriesReply struct {
    Term int
    Success bool

    RStartIndex int // if rejected, first index in reject term
    RTerm int
    RLen int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    // server receive RequestVote
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // msg may lost, so will receive Term == curr_term repeated
    if rf.curr_term > args.Term {
        reply.VoteGranted = false
        reply.Term = rf.curr_term
        rf.dout("reject Vote from [%v:%v]", args.CandidateId, args.Term)
        return
    }

    if rf.curr_term < args.Term {
        rf.to_follower(args.Term, args.CandidateId)
    }

    reply.Term = args.Term
    // consistency check
    last_index := -1
    last_term := -1
    if len(rf.log) > 0 {
        last_index = len(rf.log) - 1
        last_term = rf.log[last_index].Term
    }
    // curr term is more up-to-date than sender or
    // curr log index is more up-to-date than sender
    if (last_term > args.LastLogTerm) ||
        (last_term == args.LastLogTerm && last_index > args.LastLogIndex) {
        reply.VoteGranted = false
        rf.dout("election restriction from [%v] [%v:%v] < [%v:%v]", args.CandidateId, args.LastLogIndex, args.LastLogTerm,
            last_index, last_term)
    } else {
        if rf.voted_for == -1 || rf.voted_for == args.CandidateId { // reply maybe lost
            rf.voted_for = args.CandidateId
            reply.VoteGranted = true
            rf.persist()
            rf.dout("vote to [%v]", args.CandidateId)
            //rf.update_timeout()
        }
    }

    return
}

//
// example code to send a RequestVote RPC to a server.
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.is_leader() {
        isLeader = true
        term = rf.curr_term
        index = len(rf.log)

        rf.dout("start append [%v:%v]", index, term)
        // 1. construct log entry
        entry := &LogEntry{
            Command: command,
            Term: term,
            Index: index,
        }
        rf.log = append(rf.log, *entry)
        rf.match_index[rf.me] = index
        // persist to local
        rf.persist()
        // 2. concurrent append
        for i, _ := range rf.peers {
            if i != rf.me {
                go rf.handle_append_entries(rf.curr_term, i, entry)
            }
        }
        index += 1 // raft has define log index start from 1
    } else {
        isLeader = false
    }

	return index, term, isLeader
}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    rf.majority = len(peers) / 2 + 1
    rf.cond = sync.NewCond(&rf.mu)
    rf.timeout_cond = sync.NewCond(&rf.mu)
    rf.state = STATE_FOLLOWER
    rf.update_timeout()
    rf.applyCh = applyCh
    rf.commit_index = -1
    rf.next_index = make([]int, len(peers))
    rf.match_index = make([]int, len(peers))

    // receive msg from leader or candidate

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    for i := 0; i < len(rf.peers); i++ {
        rf.next_index[i] = len(rf.log)
        rf.match_index[i] = -1 // our first index is 0
    }
    go rf.timeout_check()

    go rf.heartbeat_entry()

	return rf
}

// my code
func (rf *Raft) timeout_check() {
    // election timeout 200 - 400ms
    rf.mu.Lock()
    for !rf.killed() {
        for !rf.is_leader() {
            if time.Now().After(rf.timeout) {
                // timeout, convert to candidate
                rf.to_candidate()
            }
            tmp := rf.delta
            rf.mu.Unlock()
            time.Sleep(tmp)
            if rf.killed() {
                return
            }
            rf.mu.Lock()
        }
        rf.timeout_cond.Wait()
    }
    rf.mu.Unlock()

}

func (rf *Raft) handle_send_request_vote(term int, last_index int, last_term int, s int) {
    args := &RequestVoteArgs{
        Term: term,
        CandidateId: rf.me,
        LastLogIndex: last_index,
        LastLogTerm: last_term,
    }
    var reply *RequestVoteReply

    for {
        if rf.killed() {
            rf.dout("quit killed rpc RequestVote")
            return
        }
        reply = &RequestVoteReply{}
        if ok := rf.sendRequestVote(s, args, reply); ok {
            break
        }
        rf.mu.Lock()
        // has converted to new state
        if rf.curr_term > args.Term {
            rf.mu.Unlock()
            return
        }
        rf.mu.Unlock()

        // msg lost
        continue
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.curr_term > args.Term {
        return
    }
    if reply.VoteGranted {
        rf.dout("received vote from [%v]", s)
        rf.received_vote++
        if rf.received_vote == rf.majority {
            // convert to leader, no need to persist
            rf.state = STATE_LEADER
            rf.cond.Broadcast()
            rf.dout("to leader term[%v] %v", rf.curr_term, rf.log)
            // initialize nextIndex and matchIndex
            for i := 0; i < len(rf.next_index); i++ {
                rf.next_index[i] = len(rf.log)
                rf.match_index[i] = -1
            }
        }
    } else if reply.Term > rf.curr_term {
        // update higher term from reply
        // convert to follower
        rf.to_follower(reply.Term, s)
    }
}

func (rf *Raft) to_candidate() {
    rf.curr_term++
    rf.voted_for = rf.me
    rf.received_vote = 1
    rf.state = STATE_CANDIDATE
    rf.update_timeout()
    rf.dout("to candidate term[%v]", rf.curr_term)

    last_index := -1
    last_term := -1
    if len(rf.log) > 0 {
        last_index = len(rf.log) - 1
        last_term = rf.log[last_index].Term
    }

    rf.persist()

    for i, _ := range rf.peers {
        if i != rf.me {
            go rf.handle_send_request_vote(rf.curr_term, last_index, last_term, i)
        }
    }
}

func (rf *Raft) heartbeat_entry() {
    rf.mu.Lock()
    for !rf.killed() {
        for rf.is_leader() {
            tmp := rf.curr_term
            rf.mu.Unlock()

            for i, _ := range rf.peers {
                if i != rf.me {
                    go rf.handle_append_entries(tmp, i, nil)
                }
            }
            time.Sleep(100 * time.Millisecond)
            if rf.killed() {
                return
            }
            rf.mu.Lock()
        }
        rf.cond.Wait()
    }
    rf.mu.Unlock()
}

func (rf *Raft) handle_append_entries(term int, s int, entry *LogEntry) {
    rf.mu.Lock()
    if term != rf.curr_term {
        rf.dout("%v -> %v discard AppendEntries", term, rf.curr_term)
        rf.mu.Unlock()
        return
    }
    if entry != nil && entry.Index != len(rf.log) - 1 {
        rf.dout("has greater entry, discard current")
        rf.mu.Unlock()
        return
    }
    args := &AppendEntriesArgs{
        Term: term,
        LeaderId: rf.me,
        LeaderCommit: rf.commit_index,
    }
    if entry != nil {
        if rf.next_index[s] > entry.Index {
            // discard log that has been replicated
            rf.dout("discard AppendEntries to [%v] nextIndex:%v > entry.Index:%v", s, rf.next_index[s], entry.Index)
            rf.mu.Unlock()
            return
        }
        args.PrevLogIndex = rf.next_index[s] - 1
        if args.PrevLogIndex >= 0 {
            args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
        }
        args.Entries = make([]LogEntry, 0, entry.Index - rf.next_index[s] + 1)
        for i := entry.Index; i > args.PrevLogIndex; i-- {
            args.Entries = append(args.Entries, rf.log[i])
        }
        rf.dout("send AppendEntries [%v->%v] to [%v]", args.Entries[len(args.Entries)-1], args.Entries[0], s)
    } else {
        args.PrevLogIndex = rf.next_index[s] - 1
        if args.PrevLogIndex >= 0 {
            args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
        }
    }
    rf.mu.Unlock()
    var reply *AppendEntriesReply
    for {
        if rf.killed() {
            rf.dout("quit killed rpc AppendEntries")
            return
        }
        // msg will retry, so reply need update, otherwile will get WARN:
        // "labgob warning: Decoding into a non-default variable/field Term may not work"
        reply = &AppendEntriesReply{}
        ok := rf.peers[s].Call("Raft.AppendEntries", args, reply)
        // leader has changed
        rf.mu.Lock()
        if rf.curr_term > args.Term {
            rf.mu.Unlock()
            return
        }
        if !ok {
            //rf.dout("msg lost, retry AppendEntries to [%v]", s)
        }
        rf.mu.Unlock()

        // msg lost, retry
        if !ok {
            continue
        }
        // handle reply
        // consistency check failed
        if reply.Term == args.Term && !reply.Success {
            rf.mu.Lock()
            // next_index maybe changed by other rpc, so we fill entries again rather than append previous log entry
            //args.Entries = append(args.Entries, *rf.log[args.PrevLogIndex])

            if entry != nil && rf.next_index[s] > entry.Index {
                // other thread has update nextIndex by append log entry
                rf.dout("handle_append_entries nextIndex greater than entryIndex %v->%v", rf.next_index[s], entry.Index)
                rf.mu.Unlock()
                return
            }
            if reply.RTerm == -1 {
                // peer s log is empty
                rf.next_index[s] = 0
            } else {
                // find last consistent log entry

                if rf.next_index[s] <= reply.RStartIndex {
                    // other thread has decrease nextIndex
                } else {
                    find := false
                    for i := reply.RStartIndex + reply.RLen - 1; i >= reply.RStartIndex; i-- {
                        if rf.log[i].Term == reply.RTerm {
                            // last consistent entry 
                            rf.dout("find consistent entry %v", rf.log[i].str())
                            rf.next_index[s] = i + 1
                            find = true
                            break
                        }
                    }
                    if !find {
                        rf.next_index[s] = reply.RStartIndex
                    }
                }
            }
            args.PrevLogIndex = rf.next_index[s] - 1
            if entry != nil {
                args.Entries = args.Entries[:0]
                for i := entry.Index; i > args.PrevLogIndex; i-- {
                    args.Entries = append(args.Entries, rf.log[i])
                }
            } else {
                // heartbeat check failed, should not send log entry,just update nextIndex
            }
            if args.PrevLogIndex >= 0 {
                args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
            }
            if args.Entries != nil {
                rf.dout("got reject from %v retry [%v->%v], [%v:%v]", s, args.Entries[len(args.Entries)-1], args.Entries[0],
                args.PrevLogIndex, args.PrevLogTerm)
            } else {
                rf.dout("got reject heartbeat from %v retry, [%v:%v]", s, args.PrevLogIndex, args.PrevLogTerm)
            }
            rf.mu.Unlock()
            continue
        }
        break
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.curr_term > args.Term {
        return
    }

    if reply.Term == args.Term {
        if entry != nil {
            // log entry has replicated successfully, update matchIndex
            if rf.match_index[s] < entry.Index {
                rf.match_index[s] = entry.Index
            }
            // update nextIndex
            if rf.next_index[s] < entry.Index + 1{
                rf.dout("update [%v]nextIndex %v -> %v", s, rf.next_index[s], entry.Index + 1)
                rf.next_index[s] = entry.Index + 1
            }
            // commit
            commit := rf.get_match()
            for i := rf.commit_index + 1; i <= commit; i++ {
                // reply to client and apply to state machine
                apply := ApplyMsg{
                    CommandValid: true,
                    Command: rf.log[i].Command,
                    CommandIndex: i + 1, // first is 1
                }
                rf.applyCh <- apply
            }
            if rf.commit_index < commit {
                rf.dout("commitIndex[%v] %v -> %v", s, rf.commit_index, commit)
                rf.commit_index = commit
            }
        }
    } else if reply.Term > rf.curr_term {
        rf.to_follower(reply.Term, s)
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.curr_term > args.Term {
        reply.Success = false
        reply.Term = rf.curr_term
        return
    } else {
        // update heartbeat timeout
        defer rf.update_timeout()

        reply.Term = args.Term
        rf.to_follower(args.Term, args.LeaderId)
        // consistency check
        if args.PrevLogIndex >= len(rf.log) {
            // PrevIndex not in my log
            log_len := len(rf.log)
            if log_len == 0 {
                reply.RStartIndex = -1
                reply.RTerm = -1
                reply.RLen = 0
            } else {
                reply.RTerm = rf.log[log_len-1].Term
                reply.RStartIndex = rf.find_first_index(reply.RTerm)
                reply.RLen = log_len - reply.RStartIndex
            }
            rf.dout("AppendEntries reject from [%v] (%v > %v) [%v:%v]", args.LeaderId, args.PrevLogIndex, log_len,
                reply.RStartIndex, reply.RTerm)
            reply.Success = false
        } else if args.PrevLogIndex != -1 &&
            rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
            // PrevIndex in my log
            reply.RTerm = rf.log[args.PrevLogIndex].Term
            reply.RStartIndex = rf.find_first_index(reply.RTerm)
            reply.RLen = args.PrevLogIndex - reply.RStartIndex + 1
            rf.dout("AppendEntries reject %v != [%v:%v] [%v:%v]", rf.log[args.PrevLogIndex], args.PrevLogIndex, args.PrevLogTerm,
                reply.RStartIndex, reply.RTerm)
            reply.Success = false
        } else {
            reply.Success = true
        }

        if !reply.Success {
            return
        }
        if len(args.Entries) != 0 {
            // append entries
            log_size := len(rf.log)
            en_size := len(args.Entries)
            start := -1
            for i := en_size - 1; i >= 0; i-- {
                en := &args.Entries[i]
                if en.Index < log_size && en.Term != rf.log[en.Index].Term {
                    // erase inconsistent log
                    rf.dout("AppendEntries inconsistent log %v != %v",
                    en.str(), rf.log[en.Index].str())
                    start = i
                    rf.log = rf.log[:en.Index]
                    break
                } else if en.Index >= log_size {
                    // append all entries
                    start = i
                    break
                }
            }
            if start >= 0 {
                tmp := "empty"
                if l := len(rf.log); l > 0 {
                    tmp = (&rf.log[l-1]).str()
                }
                rf.dout("AppendEntries %v -> [%v->%v]", tmp, args.Entries[start], args.Entries[0])
            }
            for ; start >= 0; start-- {
                en := &args.Entries[start]
                rf.log = append(rf.log, *en)
            }
            // persist log entry
            rf.persist()
        }
        // commit log
        if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term != args.Term {
            // don't commit log less than current term
            return
        }
        if rf.commit_index >= args.LeaderCommit {
            // is greater scnario will be performed?
        } else {
            last_commit := rf.commit_index
            rf.commit_index = args.LeaderCommit
            if len(rf.log) > 0 && rf.log[len(rf.log) - 1].Index < rf.commit_index {
                rf.commit_index = rf.log[len(rf.log) - 1].Index
            } else if len(rf.log) == 0 {
                rf.commit_index = -1
            }
            for i := last_commit + 1; i <= rf.commit_index; i++ {
                apply := ApplyMsg{
                    CommandValid: true,
                    Command: rf.log[i].Command,
                    CommandIndex: rf.log[i].Index + 1, // first is 1
                }
                rf.applyCh <- apply
            }
            if last_commit < rf.commit_index {
                rf.dout("follower commit %v -> %v", last_commit, rf.commit_index)
            }
        }
    }
}

func (rf *Raft) update_timeout() {
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    rf.delta = time.Duration(200 + r.Intn(300)) * time.Millisecond
    rf.timeout = time.Now().Add(rf.delta)
}

func (rf *Raft) is_leader() bool {
    return rf.state == STATE_LEADER
}

func (rf *Raft) to_follower(t int, s int) {
    if rf.state == STATE_FOLLOWER && rf.curr_term == t {
        return
    }
    if rf.is_leader() {
        rf.update_timeout()
    }
    rf.state = STATE_FOLLOWER
    rf.curr_term = t
    rf.voted_for = -1
    rf.received_vote = 0
    rf.timeout_cond.Broadcast()
    rf.dout("to follower term[%v] from [%v]", t, s)
    rf.persist()
}

func (rf *Raft) stateString() string {
    switch rf.state {
    case STATE_FOLLOWER:
        return "F"
    case STATE_LEADER:
        return "L"
    case STATE_CANDIDATE:
        return "C"
    default:
        return "unknown"
    }
}
func (rf *Raft) dout(format string, a ...interface{}) {
    tmp := fmt.Sprintf("[%v:%v:%v]", rf.me, rf.curr_term, rf.stateString())
    dprintf(tmp + format, a...)
}

type sortMatch []int
func (s sortMatch) Len() int { return len(s) }
func (s sortMatch) Less(i, j int) bool { return s[i] < s[j] }
func (s sortMatch) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (rf *Raft) get_match() int {
    tmp := make([]int, len(rf.match_index))
    for i, m := range rf.match_index {
        tmp[i] = m
    }
    // Top-K, we can use quick select algorithm that time complexity is O(n)
    sort.Sort(sortMatch(tmp))
    return tmp[rf.majority-1]
}

func (rf *Raft) find_first_index(term int) int {
    // log is sequential, use binary search
    ret := sort.Search(len(rf.log), func(i int) bool {return rf.log[i].Term >= term })
    return ret
}
