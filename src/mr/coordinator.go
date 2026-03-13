package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TIMEOUT time.Duration = 10 * time.Second

type TaskState int

const (
	UnAssigned TaskState = iota
	Assigned
	Completed
	Failed
)

type TaskInfo struct {
	TaskState TaskState
	TaskFile  string
	TimeStamp time.Time
}
type Coordinator struct {
	NMap               int
	NReduce            int
	MapTasks           []TaskInfo
	ReduceTasks        []TaskInfo
	AllMapCompleted    bool
	AllReduceCompleted bool
	Mu                 sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *MessageArgs, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if !c.AllMapCompleted {
		mapCompleteCount := 0
		for i, task := range c.MapTasks {
			if task.TaskState == UnAssigned || task.TaskState == Failed || (task.TaskState == Assigned && time.Since(task.TimeStamp) > TIMEOUT) {
				c.MapTasks[i].TaskState = Assigned
				c.MapTasks[i].TimeStamp = time.Now()
				reply.TaskID = i
				reply.TaskType = MapTask
				reply.TaskFile = task.TaskFile
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				return nil
			} else if task.TaskState == Completed {
				mapCompleteCount++
			}
		}
		if mapCompleteCount == len(c.MapTasks) {
			c.AllMapCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}
	if !c.AllReduceCompleted {
		reduceCompleteCount := 0
		for i, task := range c.ReduceTasks {
			if task.TaskState == UnAssigned || task.TaskState == Failed || (task.TaskState == Assigned && time.Since(task.TimeStamp) > TIMEOUT) {
				c.ReduceTasks[i].TaskState = Assigned
				c.ReduceTasks[i].TimeStamp = time.Now()
				reply.TaskID = i
				reply.TaskType = ReduceTask
				reply.NMap = c.NMap
				reply.NReduce = c.NReduce
				return nil
			} else if task.TaskState == Completed {
				reduceCompleteCount++
			}
		}
		if reduceCompleteCount == len(c.ReduceTasks) {
			c.AllReduceCompleted = true
		} else {
			reply.TaskType = Wait
			return nil
		}
	}

	// all tasks completed
	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) ReportTask(args *MessageArgs, reply *MessageReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch args.TaskStatus {
	case MapSuccess:
		c.MapTasks[args.TaskID].TaskState = Completed
	case MapFailure:
		c.MapTasks[args.TaskID].TaskState = Failed
	case ReduceSuccess:
		c.ReduceTasks[args.TaskID].TaskState = Completed
	case ReduceFailure:
		c.ReduceTasks[args.TaskID].TaskState = Failed
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for _, task := range c.MapTasks {
		if task.TaskState != Completed {
			return false
		}
	}
	for _, task := range c.ReduceTasks {
		if task.TaskState != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:        len(files),
		NReduce:     nReduce,
		MapTasks:    make([]TaskInfo, len(files)),
		ReduceTasks: make([]TaskInfo, nReduce),
	}

	for i := range files {
		c.MapTasks[i] = TaskInfo{
			TaskState: UnAssigned,
			TaskFile:  files[i],
			TimeStamp: time.Now(),
		}
	}

	c.server(sockname)
	return &c
}
