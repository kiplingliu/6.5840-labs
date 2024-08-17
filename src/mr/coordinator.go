// Note: crash test can still fail if it takes too long (although maybe a bug?)

package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapType    = iota
	ReduceType = iota
)

const (
	idle       = iota
	inProgress = iota
	completed  = iota
)

type Coordinator struct {
	nMap    int
	nReduce int

	tasks                []Task
	tasksCompleted       int
	assignTaskSignal     sync.Cond
	taskCompletedSignals []chan bool

	tasksLock sync.Mutex
}

type Task struct {
	NMap    int
	NReduce int

	Id     int
	Type   int
	status int

	MapInput   string
	MapOutputs []string

	ReduceInputs []string
	ReduceOutput string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func debugPrintf(fromServer bool, id int, format string, args ...interface{}) {
	_, debug := os.LookupEnv("DEBUG")
	if !debug {
		return
	}
	var source string
	if fromServer {
		source = "SERVER"
	} else {
		source = "CLIENT"
	}
	tmp := append([]interface{}{source, os.Getpid(), id}, args...)
	fmt.Printf("%v %v: task %v: "+format, tmp...)
}

func (c *Coordinator) waitTaskOrReset(id int) {
	select {
	case <-c.taskCompletedSignals[id]:
		debugPrintf(true, id, "completion signal received\n")
		return
	case <-time.After(10 * time.Second):
		c.tasksLock.Lock()
		debugPrintf(true, id, "timeout\n")
		c.tasks[id].status = idle
		c.assignTaskSignal.Broadcast()
		c.tasksLock.Unlock()
	}
}

// Requirements:
//  1. Should eventually make progress (unless all tasks are complete,
//     in which case it is allowed to infinitely block)
//  2. Once a task is assigned, there should be no need to signal the worker
//     to switch tasks, even if a task dies.
//  3. Due to 2, all map tasks must complete before a reduce task may be
//     assigned (e.g. given 2 workers and 2 map tasks, suppose one map
//     completes and the other worker dies. If the remaining worker is
//     assigned a reduce task, it will not be able to make forward progress
//     because it is waiting on the other map task. Therefore, it will need
//     to be reassigned.)
func (c *Coordinator) AssignTask(args *struct{}, task *Task) error {
	c.tasksLock.Lock()

	// If idle map task available, return w/o blocking
	for id := 0; id < c.nMap; id++ {
		if c.tasks[id].status == idle {
			c.tasks[id].status = inProgress
			debugPrintf(true, id, "found idle map task w/o blocking\n")
			*task = c.tasks[id]
			c.tasksLock.Unlock()
			go c.waitTaskOrReset(id)
			return nil
		}
	}

	idleMapId := -1
	idleReduceId := -1
	for {
		for id := 0; id < c.nMap; id++ {
			if c.tasks[id].status == idle {
				idleMapId = id
			}
		}
		for id := c.nMap; id < len(c.tasks); id++ {
			if c.tasks[id].status == idle {
				idleReduceId = id
			}
		}

		// assignTaskSignal signals on two events:
		// 1. A task dies
		// 2. All map tasks complete
		// These events could change the set of *assignable* idle tasks
		// so they must trigger a recheck.
		if idleMapId != -1 || (c.tasksCompleted >= c.nMap && idleReduceId != -1) {
			break
		}

		debugPrintf(true, -1, "no assignable idle tasks\n")
		idleMapId = -1
		idleReduceId = -1
		c.assignTaskSignal.Wait()
	}
	if idleMapId != -1 {
		id := idleMapId
		debugPrintf(true, id, "found idle map task after blocking (map task died)\n")
		c.tasks[id].status = inProgress
		*task = c.tasks[id]
		c.tasksLock.Unlock()
		go c.waitTaskOrReset(id)
		return nil
	}

	if c.tasksCompleted >= c.nMap && idleReduceId != -1 {
		id := idleReduceId
		debugPrintf(true, id, "found idle reduce task\n")
		c.tasks[id].status = inProgress
		*task = c.tasks[id]
		c.tasksLock.Unlock()
		go c.waitTaskOrReset(id)
		return nil
	}

	debugPrintf(true, -1, "BAD: shouldn't be here\n")
	c.tasksLock.Unlock()
	return errors.New("BAD")
}

// Purpose of the function:
// 1. Maintain consistency between tasksCompleted and status of tasks
// 2. If map task, register each of the map outputs as a reduce input
// 3. If all map tasks completed, signal AssignTask
func (c *Coordinator) CompleteTask(task *Task, reply *struct{}) error {
	c.tasksLock.Lock()
	debugPrintf(true, task.Id, "start CompleteTask\n")

	if c.tasks[task.Id].status == completed {
		debugPrintf(true, task.Id, "end CompleteTask: already completed\n")
		c.tasksLock.Unlock()
		return nil
	}

	go func() {
		// If this is not inside a goroutine, it can block.
		// e.g. a task times out, then returns before it's
		// reassigned. Then, there's no waitTaskOrReset to
		// wait on it. Also, since a lock is held, other things
		// can't proceed.
		c.taskCompletedSignals[task.Id] <- true
	}()
	debugPrintf(true, task.Id, "after channel\n")
	if task.Type == MapType {
		for i, o := range task.MapOutputs {
			if o != "" {
				tmp := &c.tasks[i+c.nMap].ReduceInputs
				*tmp = append(*tmp, o)
			}
		}
	}
	c.tasks[task.Id].status = completed
	c.tasksCompleted++
	if c.tasksCompleted == c.nMap {
		c.assignTaskSignal.Broadcast()
	}

	debugPrintf(true, task.Id, "end CompleteTask\n")
	c.tasksLock.Unlock()
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
	// Your code here.
	c.tasksLock.Lock()
	if c.tasksCompleted == len(c.tasks) {
		c.tasksLock.Unlock()
		return true
	}
	c.tasksLock.Unlock()

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.tasks = make([]Task, c.nMap+c.nReduce)
	c.assignTaskSignal = *sync.NewCond(&c.tasksLock)
	c.taskCompletedSignals = make([]chan bool, len(c.tasks))
	for i := 0; i < len(c.tasks); i++ {
		c.taskCompletedSignals[i] = make(chan bool)
	}
	for i := 0; i < c.nMap; i++ {
		c.tasks[i] = Task{
			NMap:       c.nMap,
			NReduce:    c.nReduce,
			Id:         i,
			Type:       MapType,
			MapInput:   files[i],
			MapOutputs: []string{},
			status:     idle,
		}
	}
	for i := c.nMap; i < len(c.tasks); i++ {
		c.tasks[i] = Task{
			NMap:         c.nMap,
			NReduce:      c.nReduce,
			Id:           i,
			Type:         ReduceType,
			status:       idle,
			ReduceInputs: []string{},
		}
	}

	c.server()
	return &c
}
