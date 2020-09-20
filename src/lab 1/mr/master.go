package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

const (
    STATE_PENDING = 1 + iota
    STATE_IDLE
    STATE_COMPLETED
)

const (
    HEARTBEAT = 10 * time.Second
)

type MapTask struct {
    real_id int
    id int // simulate local machine
    name string
    state int
    ttl time.Time
}

type ReduceTask struct {
    id int
    state int
    input []string
    ttl time.Time
}

type Master struct {
	// Your definitions here.
    NReduce int
    mutex sync.Mutex

    map_task map[int]*MapTask
    idle_map_task []*MapTask
    map_finished bool
    next_map_id int
    finished_task int
    map_cond *sync.Cond

    reduce_task map[int]*ReduceTask
    idle_reduce_task []*ReduceTask
    reduce_finished bool
    reduce_cond *sync.Cond
    finished_reduce int

    listener net.Listener
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapInput(in *MapInputArgs, reply *MapInputReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for !m.map_finished {
        if len(m.idle_map_task) == 0 {
            m.map_cond.Wait()
            continue
        }
        task := m.idle_map_task[0]
        m.idle_map_task = m.idle_map_task[1:]

        reply.Filename = task.name
        reply.Id = task.id
        reply.RealId = task.real_id
        reply.NReduce = m.NReduce

        task.ttl = time.Now().Add(HEARTBEAT)
        task.state = STATE_PENDING
        return nil
    }
    return nil
}

func (m *Master) MapFinished(in *MapFinishArgs, reply *MapFinishReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    task := m.map_task[in.Id]
    if task.state == STATE_COMPLETED {
        log.Printf("map already finished %v:%v", in.Id, in.Name)
        return nil
    }
    task.state = STATE_COMPLETED
    m.finished_task++
    if len(m.map_task) == m.finished_task {
        m.map_finished = true
        m.map_cond.Broadcast()
        //log.Printf("all map task finished")
    }
    for k, inter := range in.Intermediate {
        rtask := m.reduce_task[k]
        rtask.input = append(rtask.input, inter)
    }
    return nil
}

func (m *Master) GetReduceInput(in *ReduceInputArgs, reply *ReduceInputReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for !m.reduce_finished {
        if len(m.idle_reduce_task) == 0 {
            m.reduce_cond.Wait()
            continue
        }
        task := m.idle_reduce_task[0]
        m.idle_reduce_task = m.idle_reduce_task[1:]

        reply.Id = task.id
        reply.Intermediate = task.input

        task.ttl = time.Now().Add(HEARTBEAT)
        task.state = STATE_PENDING
        return nil
    }
    reply.Id = -1
    return nil
}

func (m *Master) ReduceFinished(in *ReduceFinishArgs, reply *ReduceFinishReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    task := m.reduce_task[in.Id]
    if task.state == STATE_COMPLETED {
        log.Printf("reduce already finished %v", in.Id)
        return nil
    }

    task.state = STATE_COMPLETED
    m.finished_reduce++
    if len(m.reduce_task) == m.finished_reduce {
        m.reduce_finished = true
        m.reduce_cond.Broadcast()
    }
    return nil
}

func (m *Master) Heartbeat(in *HeartbeatArgs, reply *HeartbeatReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    if (in.Type == 0) {
        task := m.map_task[in.Id]
        task.ttl = time.Now().Add(HEARTBEAT)
        return nil
    } else if (in.Type == 1) {
        task := m.reduce_task[in.Id]
        task.ttl = time.Now().Add(HEARTBEAT)
    }
    return nil
}

func (m *Master) heartbeat() {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    for !m.map_finished {
        notify := false
        for _, task := range m.map_task {
            if task.state == STATE_PENDING && task.ttl.Before(time.Now()) {
                //log.Printf("map timeout %v:%v, name=%v, newid=%v", task.real_id, task.id, task.name, m.next_map_id)

                task.state = STATE_IDLE
                task.id = m.next_map_id
                m.next_map_id++

                m.idle_map_task = append(m.idle_map_task, task)
                notify = true
            }
        }
        if notify {
            m.map_cond.Broadcast()
        }
        m.mutex.Unlock()
        time.Sleep(HEARTBEAT)
        m.mutex.Lock()
    }

    for !m.reduce_finished {
        notify := false
        for _, task := range m.reduce_task {
            if task.state == STATE_PENDING && task.ttl.Before(time.Now()) {
                //log.Printf("reduce timeout id=%v", task.id)
                task.state = STATE_IDLE
                m.idle_reduce_task = append(m.idle_reduce_task, task)
                notify = true
            }
        }
        if notify {
            m.reduce_cond.Broadcast()
        }
        m.mutex.Unlock()
        time.Sleep(HEARTBEAT)
        m.mutex.Lock()
    }
    
    log.Printf("reduce finished")
}

func (m *Master) init(in []string, nreduce int) {
    m.map_task = make(map[int]*MapTask)
    m.idle_map_task = make([]*MapTask, 0, len(in))
    m.map_cond = sync.NewCond(&m.mutex)
    m.map_finished = false
    m.finished_task = 0
    m.next_map_id = 0

    m.reduce_task = make(map[int]*ReduceTask)
    m.idle_reduce_task = make([]*ReduceTask, 0, nreduce)
    m.reduce_cond = sync.NewCond(&m.mutex)
    m.reduce_finished = false
    m.finished_reduce = 0
    m.NReduce = nreduce

    for _, s := range in {
        task := &MapTask{
            real_id: m.next_map_id,
            id: m.next_map_id,
            name: s,
            state: STATE_IDLE,
        }
        m.map_task[task.id] = task
        m.idle_map_task = append(m.idle_map_task, task)
        m.next_map_id++
    }
    for i := 0; i < nreduce; i++ {
        task := &ReduceTask{
            id: i,
            input: make([]string, 0),
            state: STATE_IDLE,
        }
        m.reduce_task[i] = task
        m.idle_reduce_task = append(m.idle_reduce_task, task)
    }

    go m.heartbeat()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
    m.listener = l
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
    m.mutex.Lock()
    defer m.mutex.Unlock()
    ret = m.map_finished && m.reduce_finished

    defer func() {
        if ret {
            m.listener.Close()
        }
    }()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
    m.init(files, nReduce)


	m.server()
	return &m
}
