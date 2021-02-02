package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	nReduce        int
	nMap           int
	files          []string
	eachMapDone    []bool
	mapDone        bool
	eachReduceDone []bool
	reduceDone     bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) TaskAllocate(args *Args, reply *Reply) error {
	if !m.mapDone {
		// allocate map to workers
		for i := 0; i < m.nMap; i++ {
			if m.eachMapDone[i] == false {
				fmt.Println("allocate map task", i)
				reply.filename = m.files[i]
				reply.taskType = "map"
				reply.nReduce = m.nReduce
				reply.done = false
			}
		}
	} else {
		if !m.reduceDone {
			// allocate reduce to workers
			for i := 0; i < m.nReduce; i++ {
				if m.eachReduceDone[i] == false {
					fmt.Println("allocate reduce task", i)
					reply.taskType = "reduce"
					reply.taskIndex = i
					reply.nMap = m.nMap
					reply.done = false
				}
			}
		} else {
			// all done
			reply.done = true
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
		eachMapDone:    make([]bool, len(files)),
		mapDone:        false,
		eachReduceDone: make([]bool, nReduce),
		reduceDone:     false,
	}
	m.server()
	return &m
}
