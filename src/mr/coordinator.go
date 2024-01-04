package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// a map that stores the status of each map task
	// 0: not started
	// 1: in progress
	// 2: completed
	mapStatus map[string]int

	// a map that stores the status of each reduce task
	// 0: not started
	// 1: in progress
	// 2: completed
	reduceStatus map[int]int

	mapCompleted int

	TaskNum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// respond with the file name of an as-yet-unstarted map task

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *ExampleReply) error {
	if c.mapCompleted < c.TaskNum {
		for fileName, status := range c.mapStatus {
			if status == 0 {
				reply.taskType = "map"
				reply.fileName = fileName
				reply.status = 0
				c.mapStatus[fileName] = 1
				return nil
			}
		}
	} else {
		for reduceID, status := range c.reduceStatus {
			if status == 0 {
				reply.taskType = "reduce"
				reply.fileName = ""
				reply.status = 0
				c.reduceStatus[reduceID] = 1
				return nil
			}
		}
	}
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
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
