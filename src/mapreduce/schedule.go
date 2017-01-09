package mapreduce

import "fmt"
import "sync/atomic"
import "time"

func addTaskToChannel(task_max int,c chan int){
	for i := 0; i < task_max; i++ {
		c <- i
	}
}
func workerRun(	mr *Master,
				phase jobPhase,
				task_c chan int,
				task_finished *int64,
				worker_name string,
				nios int){
	var doTaskArgs DoTaskArgs
	doTaskArgs.JobName = mr.jobName
	doTaskArgs.Phase = phase
	doTaskArgs.NumOtherPhase = nios
	for task := range task_c {
		doTaskArgs.TaskNumber = task
		doTaskArgs.File = mr.files[task]
		ok := call(worker_name, "Worker.DoTask", doTaskArgs, new(struct{}))	
		if ok == true {
			atomic.AddInt64(task_finished,1)
		}else{
			task_c <- task
		}
	}
}
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	task_c := make(chan int)
	go addTaskToChannel(ntasks,task_c)
	var task_finished int64
	mr.Lock()
	for _,worker := range mr.workers {
		go workerRun(mr,phase,task_c,&task_finished,worker,nios)
	}
	mr.Unlock()
	for atomic.LoadInt64(&task_finished) < int64(ntasks){
		select {
			case new_worker := <- mr.registerChannel:
				go workerRun(mr,phase,task_c,&task_finished,new_worker,nios)
			default:
				time.Sleep(100 * time.Millisecond)	
		}
	}
	close(task_c)
	
	fmt.Printf("Schedule: %v phase done\n", phase)
}
