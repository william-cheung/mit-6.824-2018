package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	idleChan := make(chan string)
	taskChan := make(chan int)
	doneChan := make(chan int)

	getIdleWorker := func() string {
		var worker string
		select {
		case worker = <-registerChan:
		case worker = <-idleChan:
		}
		return worker
	}

	doTask := func(worker string, task int) {
		var args DoTaskArgs
		args.JobName = jobName
		args.File = mapFiles[task]
		args.Phase = phase
		args.TaskNumber = task
		args.NumOtherPhase = n_other

		ok := call(worker, "Worker.DoTask", args, nil)
		if ok {
			doneChan <- 1
			idleChan <- worker
		} else {
			fmt.Printf("schedule: RPC %s Worker.DoTask error\n", worker)
			taskChan <- task
		}
	}

	go func() {
		for t := range taskChan {
			worker := getIdleWorker()
			task := t
			go func() {
				doTask(worker, task)
			}()
		}
	}()

	go func() {
		for t := 0; t < ntasks; t++ {
			taskChan <- t
		}
	}()

	for t := 0; t < ntasks; t++ {
		<-doneChan
	}

	close(taskChan)

	fmt.Printf("Schedule: %v done\n", phase)
}
