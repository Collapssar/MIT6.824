package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by Key
type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := Args{MessageType: "request"}
		reply := Reply{}
		resp := call("Master.WorkerCallHandler", &args, &reply)
		if !resp {
			break
		}

		switch reply.Task.TaskType {
		case "Map":
			mapTask(mapf, reply.Task)
		case "Reduce":
			reduceTask(reducef, reply.Task)
		case "Wait":
			waitTask()
		}
	}

}

func mapTask(mapf func(string, string) []KeyValue, task Task)  {
	filename := task.MapFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("failed to open file %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("failed to read file %v", filename)
	}
	file.Close()
	intermediates := mapf(filename, string(content))
	buffer := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediates {
		id := ihash(kv.Key) % task.NReduce
		buffer[id] = append(buffer[id], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		interFilename := intermediateFilename(task.TaskID, i)
		storeIntermediateFile(buffer[i], interFilename)
		call("Master.SendFilename", &InterFile{ReduceID: i, Filename: interFilename}, &Reply{})
	}

	defer taskCompleted(task)
}
func intermediateFilename(NMap int, NReduce int) string {
	return fmt.Sprintf("mr-%v-%v", NMap, NReduce)
}

func storeIntermediateFile(intermediate []KeyValue, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v\n", filename)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, kv := range intermediate {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode")
		}
	}
}

func loadIntermediateFile(filename string) []KeyValue {
	var kvs []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("reduce cannot open %v", filename)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func reduceTask(reducef func(string, []string) string, task Task) {
	reply := ReplyFile{}
	call("Master.GetFilename", &Args{Task: task}, &reply)
	files := reply.Files
	for _, filename := range files {
		task.Intermediates = append(task.Intermediates, loadIntermediateFile(filename)...)
	}
	sort.Sort(ByKey(task.Intermediates))
	oname := fmt.Sprintf("mr-out-%v", task.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}

	i := 0
	for i < len(task.Intermediates){
		j := i + 1
		for j < len(task.Intermediates) && task.Intermediates[j].Key == task.Intermediates[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, task.Intermediates[k].Value)
		}
		output := reducef(task.Intermediates[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", task.Intermediates[i].Key, output)

		i = j
	}
	ofile.Close()
	defer taskCompleted(task)
}

func waitTask() {
	time.Sleep(time.Second)
}

func taskCompleted(task Task) {
	args := Args{MessageType: "completed", Task: task}
	reply := Reply{}
	//fmt.Printf("%v %v has been finish\n", task.TaskType, task.TaskNum)
	call("Master.WorkerCallHandler", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpcTest.DialHTTP("tcp", "127.0.0.1"+":1234")
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
