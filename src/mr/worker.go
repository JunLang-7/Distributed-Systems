package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := MessageArgs{}
		reply := MessageReply{}
		if ok := call("Coordinator.RequestTask", &args, &reply); !ok {
			log.Fatalf("call Coordinator.RequestTask failed")
		}

		switch reply.TaskType {
		case MapTask:
			HandleMap(&reply, mapf)
		case ReduceTask:
			HandleReduce(&reply, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func HandleMap(reply *MessageReply, mapf func(string, string) []KeyValue) {
	// read the input file
	filename := reply.TaskFile
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	// pass it to Map
	kva := mapf(filename, string(content))
	// divide the intermediate keys into buckets for nReduce reduce tasks
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	for idx, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, idx)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot create temporary file: %v", oname)
		}
		// encode kv-pairs into file
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("encode error: %v", err)
			}
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}
	args := &MessageArgs{TaskID: reply.TaskID, TaskStatus: MapSuccess}
	if ok := call("Coordinator.ReportTask", &args, &MessageReply{}); !ok {
		log.Fatalf("call Coordinator.ReportTask failed!")
	}
}

func HandleReduce(reply *MessageReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	var filenames []string
	for taskID := 0; taskID < reply.NMap; taskID++ {
		filenames = append(filenames, fmt.Sprintf("mr-%d-%d", taskID, reply.TaskID))
	}
	// handle intermediate files from Map
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		// decode the k-v pairs from file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	// When a reduce worker has read all intermediate data,
	// it sorts it by the intermediate keys
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	// call Reduce on each distinct key in intermediate[],
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	args := &MessageArgs{TaskID: reply.TaskID, TaskStatus: ReduceSuccess}
	if ok := call("Coordinator.ReportTask", &args, &MessageReply{}); !ok {
		log.Fatalf("call Coordinator.ReportTask failed!")
	}
}
