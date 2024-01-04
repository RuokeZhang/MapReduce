package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	ReduceNum int

	ReduceCompleted int
	MapCompleted    int
	MapNum          int
	mapStatus       []int
	reduceStatus    []int
	MapAssigned     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// respond with the file name of an as-yet-unstarted map task

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *ExampleReply) error {
	if c.MapAssigned < c.MapNum {
		for i := 0; i < c.MapNum; i++ {
			//start a map task
			if c.mapStatus[i] == 0 {
				reply.TaskType = "map"
				reply.Status = 1
				c.mapStatus[i] = 1
				c.MapAssigned++
				break
			}
		}
	} else if c.MapCompleted == c.MapNum && c.ReduceCompleted < c.ReduceNum {
		for i := 0; i < c.ReduceNum; i++ {
			//start a reduce task
			if c.reduceStatus[i] == 0 {
				reply.TaskType = "reduce"
				reply.Status = 1
				c.reduceStatus[i] = 1
				break
			}
		}
	} else if c.MapCompleted == c.MapNum && c.ReduceCompleted == c.ReduceNum {
		// 所有任务已完成
		reply.TaskType = "done"
		reply.Status = 1
	} else {
		// 所有Map任务已分配，但尚未全部完成
		reply.TaskType = "wait"
		reply.Status = 1
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
	c.MapCompleted = 0
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.ReduceCompleted = 0
	c.server()
	return &c
}
