package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	TaskId        int
	MapChannel    chan *Task
	ReduceChannel chan *Task
	taskPhase     TaskPhase //工作阶段
	taskTable     TaskTable
	files         []string
}

const (
	MapPhase    TaskPhase = iota //map阶段
	ReducePhase                  //reduce阶段
	ExitPhase                    //完成
)

type TaskTable map[int]*Task

var m sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *Args, task *Task) error {
	if len(c.MapChannel) > 0 {
		*task = *<-c.MapChannel
		fmt.Println(task)
		if !c.taskTable.ChangeState(task) {
			fmt.Printf(" %v is working\n", task)
		}
	}
	fmt.Println("PollTask success")
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:         files,
		MapChannel:    make(chan *Task, len(files)),
		ReduceChannel: make(chan *Task, nReduce),
		taskPhase:     MapPhase,
		taskTable:     make(map[int]*Task, len(files)+nReduce),
	}
	c.makeMapTasks(files)
	// Your code here.
	c.server()
	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		task := Task{
			TaskId:    c.generateTaskId(),
			Filename:  file,
			TaskType:  MapTask,
			TaskState: Waiting,
		}
		c.taskTable.SetTask(&task)
		c.MapChannel <- &task
		//fmt.Println(<-c.MapChannel)
	}
}

func (t TaskTable) SetTask(task *Task) bool { //设置TaskTable中的Task信息
	id := task.TaskId
	data, _ := t[id]
	if data != nil {
		fmt.Printf("TaskTable contains task %v\n", data)
		return false
	} else {
		t[id] = task
	}
	return true
}

func (t TaskTable) ChangeState(task *Task) bool {
	id := task.TaskId
	data, ok := t[id]
	if !ok || data.TaskState != Waiting {
		return false
	}
	data.TaskState = Working
	return true
}

func (c *Coordinator) generateTaskId() int {
	id := c.TaskId
	c.TaskId++
	return id
}
