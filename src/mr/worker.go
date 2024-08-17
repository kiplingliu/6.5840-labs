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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		myTask := Task{}
		debugPrintf(false, -1, "waiting for task\n")
		ok := call("Coordinator.AssignTask", &struct{}{}, &myTask)
		if !ok {
			return
		}
		if myTask.Type == MapType {
			debugPrintf(false, myTask.Id, "assigned map task\n")
			mapWorker(mapf, &myTask)
		} else { // Reduce
			debugPrintf(false, myTask.Id, "assigned reduce task\n")
			reduceWorker(reducef, &myTask)
		}

		debugPrintf(false, myTask.Id, "completed task\n")
		ok = call("Coordinator.CompleteTask", &myTask, &struct{}{})
		if !ok {
			return
		}
	}
}

func mapWorker(mapf func(string, string) []KeyValue, myTask *Task) {
	filename := myTask.MapInput
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	sort.Sort(ByKey(intermediate))

	ofiles := make([]*os.File, myTask.NReduce)
	myTask.MapOutputs = make([]string, myTask.NReduce)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		h := ihash(intermediate[i].Key) % myTask.NReduce
		oname := fmt.Sprintf("mr-%v-%v", myTask.Id, h)
		if ofiles[h] == nil {
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot create %v", oname)
			}
			ofiles[h] = ofile
			myTask.MapOutputs[h] = oname
		}
		enc := json.NewEncoder(ofiles[h])
		for _, kv := range intermediate[i:j] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", oname)
			}
		}
		i = j
	}
	for _, ofile := range ofiles {
		ofile.Close()
	}
}

func reduceWorker(reducef func(string, []string) string, myTask *Task) {
	intermediate := []KeyValue{}
	for _, filename := range myTask.ReduceInputs {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
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

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", myTask.Id-myTask.NMap)
	ofile, err := os.CreateTemp("", "mr-tmp-")
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
