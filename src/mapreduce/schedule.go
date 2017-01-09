package mapreduce

import "fmt"
import "sync/atomic"

func addTaskToChannel(task_max int,c chan int,task_map *map[int]bool){
	for i := 0; i < task_max; i++ {
		c <- i
		(*task_map)[i] = false
	}
}
func (mr *Master)workerRun(
				phase jobPhase,
				task_c chan int,
				task_finished_c chan int,
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
			task_finished_c <- task
		}else{
			task_c <- task
			break
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
	task_map := make(map[int]bool)
	task_c := make(chan int)
	finished_c := make(chan int)

	go addTaskToChannel(ntasks,task_c,&task_map)
	var task_finished int64
	mr.Lock()
	for _,worker := range mr.workers {
		go mr.workerRun(phase,task_c,finished_c,worker,nios)
	}
	mr.Unlock()
	for atomic.LoadInt64(&task_finished) < int64(ntasks){
		select {
			case new_worker := <- mr.registerChannel:
				go mr.workerRun(phase,task_c,finished_c,new_worker,nios)
			case finished_task := <- finished_c:
				if task_map[finished_task] == false{
					task_map[finished_task] = true
					task_finished++
				}
		}
	}	
	fmt.Printf("Schedule: %v phase done\n", phase)
}
