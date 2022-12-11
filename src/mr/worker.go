package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

// Len 重写len,swap,less才能排序
func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

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

	// Your worker implementation here.
	var flag = true
	for flag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, &task)
			//fmt.Println(&task)
			callDone(&task)
		case WaitingTask:
			time.Sleep(time.Second * 5)
			fmt.Println("All tasks are working, please waiting...")
		case ReduceTask:
			DoReduceTask(reducef, &task)
			callDone(&task)
		case ExitTask:
			time.Sleep(time.Second * 5)
			flag = false
		}
	}

}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	outputFileNum := task.TaskId
	var intermediate []KeyValue
	intermediate = Sort(task.TmpFileLists)
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j

	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", outputFileNum)
	os.Rename(tempFile.Name(), fn)
}

func Sort(files []string) []KeyValue {
	var kva []KeyValue
	for _, file := range files {
		ofile, _ := os.Open(file)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(SortedKey(kva))
	return kva
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	file, err := os.Open(task.Filename)
	//fmt.Println(reply.Filename)
	var intermediate []KeyValue
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	intermediate = mapf(task.Filename, string(content))
	//fmt.Println(intermediate)
	nReduce := task.NReduce
	KV := make([][]KeyValue, nReduce)
	for _, kv := range intermediate { //KV[ihash(kv.Key)%nReduce] --> {{Key : Value},{Key : Value}...}
		KV[ihash(kv.Key)%nReduce] = append(KV[ihash(kv.Key)%nReduce], kv)
	}
	for i := 0; i < nReduce; i++ { //保存intermediate到文件中
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range KV[i] {
			if err := enc.Encode(&kv); err != nil {
				break
			}
		}
		ofile.Close()
	}
}

func callDone(task *Task) Task {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkTaskDone", &args, &reply)
	if ok {
		//fmt.Println("call success")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func GetTask() Task {
	args := Args{}
	task := Task{}
	ok := call("Coordinator.PollTask", &args, &task)
	if ok {
		//fmt.Println("call success")
	} else {
		fmt.Printf("call failed!\n")
	}
	return task
}

// uncomment to send the Example RPC to the coordinator.
// CallExample()

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
