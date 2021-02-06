package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := Args{}
		reply, succeed := CallMaster(args, "Master.TaskAllocate")
		if reply.Done == true {
			// fmt.Println("All done -> exit")
			os.Exit(0)
		}
		if succeed {
			switch reply.TaskType {
			case "map":
				// fmt.Println("Got map task", reply.Filename)
				filename := reply.Filename
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))

				ofiles := make([]*os.File, reply.NReduce)
				encs := make([]*json.Encoder, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					ofiles[i], err = ioutil.TempFile("", "")
					if err != nil {
						log.Fatal(err)
					}
					encs[i] = json.NewEncoder(ofiles[i])
				}
				for _, kv := range kva {
					fileIndex := ihash(kv.Key) % reply.NReduce
					err := encs[fileIndex].Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				for i := 0; i < reply.NReduce; i++ {
					oname := "mr-" + strconv.Itoa(reply.TaskIndex) + "-" + strconv.Itoa(i)
					// fmt.Println("Map", ofiles[i].Name(), oname)
					err := os.Rename(ofiles[i].Name(), oname)
					if err != nil {
						log.Fatal(err)
					}
					ofiles[i].Close()
				}
				args := Args{
					TaskType: "map",
					TaskIndex: reply.TaskIndex,
				}
				_, succeed := CallMaster(args, "Master.TaskDone")
				if succeed != true {
					fmt.Println("Map task done failed")
				}
			case "reduce":
				// fmt.Println("Got reduce task", reply.TaskIndex)
				kva := []KeyValue{}
				for i := 0; i < reply.NMap; i++ {
					filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskIndex)
					file, err := os.Open(filename)
					defer file.Close()
					if err != nil {
						log.Fatal(err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				sort.Sort(ByKey(kva))
				tmpfile, err := ioutil.TempFile("", "")
				if err != nil {
					log.Fatal(err)
				}
				intermediate := kva
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				oname := "mr-out-" + strconv.Itoa(reply.TaskIndex)
				// fmt.Println("Reduce", tmpfile.Name(), oname)
				err = os.Rename(tmpfile.Name(), oname)
				if err != nil {
					log.Fatal(err)
				}
				tmpfile.Close()
				args := Args{
					TaskType: "reduce",
					TaskIndex: reply.TaskIndex,
				}
				_, succeed := CallMaster(args, "Master.TaskDone")
				if succeed != true {
					fmt.Println("reduce task done failed")
				}
			}
		} else {
			fmt.Println("worker call master failed")
			return
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMaster(args Args, rpcname string) (Reply, bool) {

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	succeed := call(rpcname, &args, &reply)

	return reply, succeed
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

	fmt.Println(err)
	return false
}
