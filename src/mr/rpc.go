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
	TaskType  string
	FileName  string
	Status    int
	MapID     int
	ReduceID  int
	MapNumber int
	ReduceNum int
}

// Add your RPC definitions here.
type ReportArgs struct {
	MapID    int
	ReduceID int
	TaskType string
}

type ReportReply struct {
	Err error
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
