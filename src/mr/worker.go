package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	for {
		// 向协调者请求任务
		task := CallForTask()

		if task.TaskType == "map" {
			// 执行Map任务
			Map(task.FileName, mapf, task.MapID)
			// 可能需要通知协调者Map任务完成

		} else if task.TaskType == "reduce" {
			// 执行Reduce任务
			Reduce(task.MapNumber, task.ReduceID, reducef)
			// 可能需要通知协调者Reduce任务完成

		} else if task.TaskType == "wait" {
			// 如果没有更多的Map任务，但Reduce任务还未开始
			time.Sleep(time.Second)
			continue

		} else if task.TaskType == "exit" {
			// 如果所有任务都完成了，退出循环
			break
		}

		// 等待10s再请求下一个任务
		time.Sleep(time.Second * 10)
	}
}

func Reduce(mapNumber int, reduceID int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < mapNumber; i++ {
		// 读取中间文件
		filename := fmt.Sprintf("mr-%d-%d", i, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break // 文件结束
				}
				log.Fatalf("cannot decode kv from file %v, error: %v", filename, err)
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + fmt.Sprintf("%d", reduceID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// read that file and call the application Mapf function
// write the intermediate key/value pairs to local disk
// repeat until all files have been processed
func Map(fileName string, mapf func(string, string) []KeyValue, mapID int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	buckets := make([][]KeyValue, 10)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}

	for _, kv := range kva {
		bucketID := ihash(kv.Key) % 10
		buckets[bucketID] = append(buckets[bucketID], kv)
	}

	for i := 0; i < 10; i++ {
		tempFileName := fmt.Sprintf("mr-%d-%d", mapID, i)
		tempFile, err := ioutil.TempFile("", tempFileName+"-")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal(err)
			}
		}
		if err := tempFile.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.Rename(tempFile.Name(), tempFileName); err != nil {
			log.Fatal(err)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallForTask() ExampleReply {

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
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.fileName %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
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
