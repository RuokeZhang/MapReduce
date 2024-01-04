package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu              sync.Mutex
	ReduceNum       int
	ReduceCompleted int
	MapCompleted    int
	MapNum          int
	MapStatus       []int
	ReduceStatus    []int
	MapAssigned     int
	fileNames       []string
	reduceAssigned  int
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// respond with the file name of an as-yet-unstarted map task

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *ExampleReply) error {
	reply.MapNumber = c.MapNum
	c.mu.Lock() // 在检查和修改共享变量之前加锁
	defer c.mu.Unlock()
	if c.MapAssigned < c.MapNum {
		for i := 0; i < c.MapNum; i++ {
			if c.MapStatus[i] == 0 {
				// 分配Map任务
				reply.TaskType = "map"
				reply.FileName = c.fileNames[i]
				reply.MapID = i
				reply.Status = 1
				c.MapStatus[i] = 1 // 任务状态设置为进行中
				c.MapAssigned++

				// 启动一个goroutine来监控任务超时
				go c.monitorMapTask(i)
				break
			}
		}
	} else if c.MapAssigned == c.MapNum && c.MapCompleted < c.MapNum {
		// 所有Map任务已分配，但尚未全部完成
		reply.TaskType = "wait"
		reply.Status = 1

	} else if c.MapCompleted == c.MapNum && c.ReduceCompleted < c.ReduceNum {
		for i := 0; i < c.ReduceNum; i++ {
			//start a reduce task
			if c.ReduceStatus[i] == 0 {
				reply.TaskType = "reduce"
				reply.Status = 1
				reply.ReduceID = i
				c.ReduceStatus[i] = 1
				c.reduceAssigned++
				// 启动一个goroutine来监控任务超时
				go c.monitorReduceTask(i)
				break
			}
		}
	} else if c.MapCompleted == c.MapNum && c.ReduceCompleted == c.ReduceNum {
		// 所有任务已完成
		reply.TaskType = "done"
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

	if c.MapCompleted == c.MapNum && c.ReduceCompleted == c.ReduceNum {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.fileNames = files
	c.MapCompleted = 0
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.ReduceCompleted = 0
	c.MapStatus = make([]int, len(files))
	c.ReduceStatus = make([]int, nReduce)
	c.server()
	return &c
}

func (c *Coordinator) monitorMapTask(taskIndex int) {
	time.Sleep(10 * time.Second) // 等待10秒
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapStatus[taskIndex] == 1 { // 如果任务状态仍然是进行中
		c.MapStatus[taskIndex] = 0 // 重置任务状态
		c.MapAssigned--
	}
}

func (c *Coordinator) monitorReduceTask(taskIndex int) {
	time.Sleep(10 * time.Second) // 等待10秒
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReduceStatus[taskIndex] == 1 { // 如果任务状态仍然是进行中
		c.ReduceStatus[taskIndex] = 0 // 重置任务状态
		c.reduceAssigned--
	}
}

func (c *Coordinator) HandleMapReport(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MapCompleted++
	c.MapStatus[args.MapID] = 2 // 任务状态设置为已完成
	fmt.Print("map's id: ", args.MapID, "\n")
	fmt.Print("MapCompleted: ", c.MapCompleted, "\n")
	return nil
}

func (c *Coordinator) HandleReduceReport(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ReduceCompleted++
	c.ReduceStatus[args.ReduceID] = 2 // 任务状态设置为已完成
	return nil
}
