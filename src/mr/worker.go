package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "sync"
import "sort"
import "io/ioutil"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
    for {
        if !doMap(mapf) {
            break;
        }
    }

    for {
        if !doReduce(reducef) {
            break
        }
    }


	// uncomment to send the Example RPC to the master.
	// CallExample()

}

type ByKey []*string

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return *a[i] < *a[j] }

type Task interface {
    is_finish() bool
}
type MapState struct {
    mutex sync.Mutex
    state int32
}
func (s *MapState) is_finish() bool {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    return s.state == STATE_COMPLETED
    //return atomic.LoadInt32(&s.state) == STATE_COMPLETED
}
func (s *MapState) setFinished() {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    s.state = STATE_COMPLETED
}

func heartbeat(t int, id int, task Task) {
    args := &HeartbeatArgs{
        Type: t,
        Id: id,
    }
    reply := &HeartbeatReply{}
    for !task.is_finish() {
        if !call("Master.Heartbeat", args, reply) {
            log.Printf("rpc failed %v:%v", t, id)

        }
        time.Sleep(HEARTBEAT / 3)
    }
}

func doMap(mapf func(string, string) []KeyValue) bool {
    args := &MapInputArgs{}
    reply := &MapInputReply{}
    
    if !call("Master.GetMapInput", args, reply) {
        return false
    }
    if len(reply.Filename) == 0 {
        //log.Printf("all map has finished")
        return false
    }
    //log.Printf("map %v:%v %v %v\n", reply.RealId, reply.Id, reply.Filename, reply.NReduce)
    
    task := &MapState{
        state: STATE_PENDING,
    }
    go heartbeat(0, reply.RealId, task)
    // get file content
    file, err := os.Open(reply.Filename)
    if err != nil {
        log.Fatalf("cannot open %v", reply.Filename)
        return true
    }
    defer file.Close()
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v %v", reply.Filename, err)
        return true
    }

    all_kv := mapf(reply.Filename, string(content))

    // write to intermediate
    partition := make(map[int][]KeyValue) // TODO: cannot use []*KeyValue, Encode cannot encode
    for _, kv := range all_kv {
        r := ihash(kv.Key) % reply.NReduce
        partition[r] = append(partition[r], kv)
    }
    output := make(map[int]string, len(partition))
    for k, v := range partition {
        inter_name := fmt.Sprintf("mr-%v-%v", reply.Id, k)
        ifile, _ := os.Create(inter_name)
        defer ifile.Close()
        output[k] = inter_name

        enc := json.NewEncoder(ifile)
        for _, intermediate := range v {
            err = enc.Encode(&intermediate)
            if err != nil {
                log.Printf("encode error: %v\n", err)
                return true
            }
        }
    }

    // finish
    finishargs := &MapFinishArgs{
        Id: reply.RealId,
        Name: reply.Filename,
        Intermediate: output,
    }
    finishreply := &MapFinishReply{}
    if !call("Master.MapFinished", finishargs, finishreply) {
        log.Printf("map finish failed %v", reply.Filename)
    }
    //log.Printf("curr map task finish %v:%v\n", reply.Id, reply.Filename)
    task.setFinished()
    return true
}

func doReduce(reducef func(string, []string) string) bool {
    args := &ReduceInputArgs{}
    reply := &ReduceInputReply{}
    if !call("Master.GetReduceInput", args, reply) {
        log.Printf("rpc reduce input failed")
        return false
    }

    if reply.Id == -1 {
        //log.Printf("all reduce has finished")
        return false
    }

    task := &MapState{ // TODO:
        state: STATE_PENDING,
    }
    //log.Printf("rpc reduce input %v:%v\n", reply.Id, reply.Intermediate)
    go heartbeat(1, reply.Id, task)

    // read all intermediate file
    all_kv := make(map[string][]string)
    sorter := make([]*string, 0)
    for _, inter := range reply.Intermediate {
        ifile, _ := os.Open(inter)
        defer ifile.Close()

        dec := json.NewDecoder(ifile)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break
            }
            if _, ok := all_kv[kv.Key]; !ok {
                sorter = append(sorter, &kv.Key)
            }
            all_kv[kv.Key] = append(all_kv[kv.Key], kv.Value)
        }
    }
    sort.Sort(ByKey(sorter))

    // write to tempfile
    tmp, err := ioutil.TempFile("", "mr")
    if err != nil {
        log.Fatalf("create temp file failed: %v", err)
    }
    defer os.Remove(tmp.Name())
    for _, k := range sorter {
        o := reducef(*k, all_kv[*k])
        fmt.Fprintf(tmp, "%v %v\n", *k, o)
    }

    // rename
    oname := fmt.Sprintf("mr-out-%v", reply.Id)
    os.Rename(tmp.Name(), oname)

    // finish
    finishargs := &ReduceFinishArgs{
        Id: reply.Id,
    }
    finishreply := &ReduceFinishReply{}

    if !call("Master.ReduceFinished", finishargs, finishreply) {
        log.Printf("rpc reduce finished %v error", reply.Id)
    }
    task.setFinished()

    //log.Printf("curr reduce task finished %v \n", reply.Id)
    return true
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

    log.Printf("rpc call failed %v", err)
	return false
}
