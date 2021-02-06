package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"sync"
)

type Master struct {
	nReduce        int
	nMap           int
	files          []string
	eachMapDone    []int // 0: not done 1: doing 2: done
	mapDone        bool
	mapAllocateTime []time.Time
	eachReduceDone []int // 0: not done 1: doing 2: done
	reduceDone     bool
	reduceAllocateTime []time.Time
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func checkAllTrue(statusSlice []int) bool {
	for _, each := range statusSlice {
		if each != 2 {
			return false
		}
	}
	return true
}
func (m *Master) TaskDone(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case "map":
		// fmt.Println("map done", args.TaskIndex)
		m.eachMapDone[args.TaskIndex] = 2
		m.mapDone = checkAllTrue(m.eachMapDone) 
		
	case "reduce":
		m.eachReduceDone[args.TaskIndex] = 2
		// fmt.Println("reduce done", args.TaskIndex)
		m.reduceDone = checkAllTrue(m.eachReduceDone)
	}
	return nil
}
func (m *Master) TaskAllocate(args *Args, reply *Reply) error {
	// fmt.Println("Got worker call", *args)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.mapDone {
		// allocate map to workers
		for i := 0; i < m.nMap; i++ {
			if m.eachMapDone[i] == 0 || 
				(m.eachMapDone[i] == 1 && time.Now().Sub(m.mapAllocateTime[i]).Seconds() >= 10.0) {
				// fmt.Println("allocate map task", i)
				m.eachMapDone[i] = 1
				m.mapAllocateTime[i] = time.Now()
				reply.Filename = m.files[i]
				reply.TaskType = "map"
				reply.TaskIndex = i
				reply.NMap = m.nMap
				reply.NReduce = m.nReduce
				reply.Done = false
				break
			}
		}
	} else {
		if !m.reduceDone {
			// allocate reduce to workers
			for i := 0; i < m.nReduce; i++ {
				if m.eachReduceDone[i] == 0 ||
					(m.eachReduceDone[i] == 1 && time.Now().Sub(m.reduceAllocateTime[i]).Seconds() >= 10.0) {
					// fmt.Println("allocate reduce task", i)
					m.eachReduceDone[i] = 1
					m.reduceAllocateTime[i] = time.Now()
					reply.TaskType = "reduce"
					reply.TaskIndex = i
					reply.NMap = m.nMap
					reply.Done = false
					break
				}
			}
		} else {
			// all done
			reply.Done = true
		}
	}
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
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// fmt.Println("map done?", m.mapDone, "reduce done?", m.reduceDone)
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := m.mapDone && m.reduceDone
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:        nReduce,
		nMap:           len(files),
		files:          files,
		eachMapDone:    make([]int, len(files)),
		mapDone:        false,
		mapAllocateTime: make([]time.Time, len(files)),
		eachReduceDone: make([]int, nReduce),
		reduceDone:     false,
		reduceAllocateTime: make([]time.Time, nReduce),
	}
	m.server()
	return &m
}
