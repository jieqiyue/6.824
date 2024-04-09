package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	jobFinish []bool // 弃用

	// map job
	mapJobFinish bool
	mapJob       []Job
	// reduce job
	reduceJobFinish bool
	reduceJob       []Job
	// lock
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetJob(args *ExampleArgs, reply *ExampleReply) error {
	// 这里先获取map job，如果map job全部完成之后，就顺着在同一个RPC里面返回reduce job
	if !c.mapJobFinish {
		for i, job := range c.mapJob {
			c.lock.Lock()
			if job.JobState != UnFinish {
				continue
			}

			reply.MapJobFileName = job.OperateFileName
			reply.JobType = job.JobType
			fmt.Print("master getJob function, replay a maptask, filename is " + reply.MapJobFileName)
			job.JobState = Doing
			c.lock.Unlock()

			// 创建一个定时器，在10秒钟之后触发，以此判断该任务是否完成
			timer := time.NewTimer(5 * time.Second)
			go func(t *time.Timer) {
				for {
					<-t.C

				}
			}(timer)

			c.lock.Unlock()
		}

		// 如果走到这里，证明没有找到map job
		c.mapJobFinish = true
		fmt.Println("master GetJob, all map job finish.")
	}

	return nil
}

func (c *Coordinator) ReportJobResult(args *JobFinishArgs, reply *JobFinishReply) error {

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

	// 初始化Coordinator
	c.nReduce = nReduce
	c.files = files

	c.mapJobFinish = false
	c.reduceJobFinish = false
	c.mapJob = make([]Job, 0)
	c.reduceJob = make([]Job, 0)

	for i, fileName := range files {
		tempJob := Job{
			JobType:         MapTask,
			OperateFileName: fileName,
			JobId:           i,
			JobState:        UnFinish,
		}

		c.mapJob = append(c.mapJob, tempJob)
	}

	for i := 0; i < nReduce; i++ {
		tempJob := Job{
			JobType:  ReduceTask,
			JobId:    i,
			JobState: UnFinish,
		}

		c.reduceJob = append(c.reduceJob, tempJob)
	}

	c.server()
	return &c
}
