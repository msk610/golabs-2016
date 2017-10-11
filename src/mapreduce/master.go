package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	allJobs := make(chan *DoJobArgs)
	file := mr.file
	reduces := mr.nReduce
	maps := mr.nMap

	// function to run a job
	runJob := func(worker string, job *DoJobArgs) {
		var res DoJobReply
		status := call(worker, "Worker.DoJob", job, &res)
		if status {
			mr.idleChannel <- worker
		} else {
			allJobs <- job
		}
	}

	// function to get next address
	nextAddr := func() string{
		// worker addr
		var addr string
		select {
		// register channel
	case addr = <- mr.registerChannel:
			// update the map
			mr.Workers[addr] = &WorkerInfo{addr}
		case addr = <- mr.idleChannel:
		}
		return addr
	}

	// thread to run jobs from allJobs channel
	go func() {
		for aJob := range allJobs{
			addr := nextAddr()
			go func(job *DoJobArgs) {
				runJob(addr, job)
			}(aJob)
		}
	}()

	// function to setup reduce jobs
	setupReduce := func() {
		for r := 0; r < reduces; r++{
			reduceJob := &DoJobArgs{file, Reduce, r, maps}
			allJobs <- reduceJob
		}
	}

	// function to setup map jobs
	setupMap := func() {
		for m := 0; m < maps; m++{
			mapJob := &DoJobArgs{file, Map, m, reduces}
			allJobs <- mapJob
		}
	}
	// setup maps
	go setupMap()
	// setup reduces
	go setupReduce()

	close(allJobs)

	return mr.KillWorkers()
}
