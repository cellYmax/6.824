package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	TaskId         int
	MapChannel     chan *Task
	ReduceChannel  chan *Task
	taskPhase      TaskPhase //工作阶段
	taskInfoHolder TaskInfoHolder
	files          []string
	NReduce        int
}

const (
	MapPhase    TaskPhase = iota //map阶段
	ReducePhase                  //reduce阶段
	ExitPhase                    //完成
)

type TaskInfo struct {
	TaskState TaskState
	startTime time.Time
	TaskPtr   *Task
}

type TaskInfoHolder struct {
	taskInfoTable map[int]*TaskInfo
}

var m sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *Args, task *Task) error {
	m.Lock()
	defer m.Unlock()
	switch c.taskPhase {
	case MapPhase:
		{
			if len(c.MapChannel) > 0 {
				*task = *<-c.MapChannel
				if !c.taskInfoHolder.State2Working(task.TaskId) {
					fmt.Printf(" %v is working\n", task.TaskId)
				}
			} else {
				task.TaskType = WaitingTask
				if c.taskInfoHolder.CheckTasks() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceChannel) > 0 {
				*task = *<-c.ReduceChannel
				//fmt.Println(task)
				if !c.taskInfoHolder.State2Working(task.TaskId) {
					fmt.Printf(" %v is working\n", task.TaskId)
				}
				//fmt.Println("PollTask success")
			} else {
				task.TaskType = WaitingTask
				if c.taskInfoHolder.CheckTasks() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ExitPhase:
		task.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) toNextPhase() {
	switch c.taskPhase {
	case MapPhase:
		c.makeReduceTasks()
		c.taskPhase = ReducePhase
	case ReducePhase:
		c.taskPhase = ExitPhase
	}
}

func (c *Coordinator) MarkTaskDone(args *Task, task *Task) error {
	m.Lock()
	defer m.Unlock()
	switch args.TaskType {
	case MapTask:
		info, ok := c.taskInfoHolder.taskInfoTable[args.TaskId]
		if ok && info.TaskState == Working {
			info.TaskState = Done
			//fmt.Printf("%v mark finished\n", info)
		}
	case ReduceTask:
		info, ok := c.taskInfoHolder.taskInfoTable[args.TaskId]
		if ok && info.TaskState == Working {
			info.TaskState = Done
			//fmt.Printf("%v mark finished\n", info)
		}
	default:
		panic("task type undefined")
	}

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
	m.Lock()
	defer m.Unlock()
	ret := false
	// Your code here.
	if c.taskPhase == ExitPhase {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		MapChannel:     make(chan *Task, len(files)),
		ReduceChannel:  make(chan *Task, nReduce),
		taskPhase:      MapPhase,
		taskInfoHolder: TaskInfoHolder{taskInfoTable: make(map[int]*TaskInfo, len(files)+nReduce)},
		NReduce:        nReduce,
	}

	c.makeMapTasks(files)
	// Your code here.

	c.server()
	go c.CrashDetector()
	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		m.Lock()
		if c.taskPhase == ExitPhase {
			m.Unlock()
			break
		}
		for _, info := range c.taskInfoHolder.taskInfoTable {
			if info.TaskState == Working && time.Since(info.startTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", info.TaskPtr.TaskId, time.Since(info.startTime))
				switch info.TaskPtr.TaskType {
				case MapTask:
					{
						c.MapChannel <- info.TaskPtr
						info.TaskState = Waiting
						fmt.Println(info.TaskPtr)
					}
				case ReduceTask:
					{
						c.ReduceChannel <- info.TaskPtr
						info.TaskState = Waiting
						fmt.Println(info.TaskPtr)
					}
				}
			}
		}
		m.Unlock()
	}
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		task := Task{
			TaskId:   c.generateTaskId(),
			Filename: file,
			TaskType: MapTask,
			NReduce:  c.NReduce,
		}
		taskInfo := TaskInfo{
			TaskState: Waiting,
			TaskPtr:   &task,
		}
		c.taskInfoHolder.SetTaskInfo(&taskInfo)
		//fmt.Printf("makeMapTask:%v\n", task)
		c.MapChannel <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		task := Task{
			TaskId:       c.generateTaskId(),
			TaskType:     ReduceTask,
			NReduce:      c.NReduce,
			TmpFileLists: getReduceFile(i),
		}
		taskInfo := TaskInfo{
			TaskState: Waiting,
			TaskPtr:   &task,
		}
		c.taskInfoHolder.SetTaskInfo(&taskInfo)
		c.ReduceChannel <- &task
	}
}

func getReduceFile(i int) []string {
	var s []string
	dir, _ := os.Getwd()
	files, _ := os.ReadDir(dir)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			s = append(s, file.Name())
		}
	}
	return s
}

func (t *TaskInfoHolder) SetTaskInfo(taskInfo *TaskInfo) bool { //设置TaskTable中的Task信息
	id := taskInfo.TaskPtr.TaskId
	info, _ := t.taskInfoTable[id]
	if info != nil {
		fmt.Printf("TaskInfoHolder has contained task %v\n", id)
		return false
	} else {
		t.taskInfoTable[id] = taskInfo
		//fmt.Printf("TaskInfoHolder contains task %v\n", id)
	}
	return true
}

func (t *TaskInfoHolder) State2Working(id int) bool {
	info, ok := t.taskInfoTable[id]
	if !ok || info.TaskState != Waiting {
		return false
	}
	info.startTime = time.Now()
	info.TaskState = Working
	//fmt.Println(info)
	return true
}

func (t *TaskInfoHolder) CheckTasks() bool {
	var (
		MapDoneNum      = 0
		MapUnDoneNum    = 0
		ReduceDoneNum   = 0
		ReduceUnDoneNum = 0
	)
	for _, taskInfo := range t.taskInfoTable {
		//fmt.Printf("checkTask: %v\n", taskInfo)
		if taskInfo.TaskPtr.TaskType == MapTask {
			if taskInfo.TaskState == Done {
				MapDoneNum++
			} else {
				MapUnDoneNum++
			}
		} else {
			if taskInfo.TaskState == Done {
				ReduceDoneNum++
			} else {
				ReduceUnDoneNum++
			}
		}
	}
	//fmt.Println("mapDoneNum:" + strconv.Itoa(MapDoneNum) + "MapUnDoneNum:" + strconv.Itoa(MapUnDoneNum))
	return MapDoneNum > 0 && MapUnDoneNum == 0 || ReduceDoneNum > 0 && ReduceUnDoneNum == 0
}

func (c *Coordinator) generateTaskId() int {
	id := c.TaskId
	c.TaskId++
	return id
}
