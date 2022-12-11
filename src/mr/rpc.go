package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct {
}

type TaskPhase int
type TaskType int
type TaskState int

type Task struct {
	TaskId       int
	Filename     string
	TaskType     TaskType  //MapTask-0 ReduceTask-1 WaitingTask-2 ExitTask-3
	TaskState    TaskState //Working-0 Waiting-1 Done-2
	NReduce      int
	TmpFileLists []string
}

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

const (
	Working TaskState = iota //正在工作
	Waiting                  //等待任务进行
	Done                     //工作结束
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
