package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapInputArgs struct {

}
type MapInputReply struct {
    NReduce int
    Filename string
    Id int
    RealId int
}

type MapFinishArgs struct {
    Id int
    Name string
    Intermediate map[int]string
}
type MapFinishReply struct {

}
type ReduceInputArgs struct {
}
type ReduceInputReply struct {
    Id int
    Intermediate []string
}
type ReduceFinishArgs struct {
    Id int
}
type ReduceFinishReply struct {

}

type HeartbeatArgs struct {
    Type int // 0 => Map, 1 => Reduce
    Id int
}
type HeartbeatReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
