package mapreduce

import (
	"fmt"
	"sync"
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
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		taskArgs := DoTaskArgs{jobName, mapFiles[i], phase, i, nOther}
		fmt.Printf("dotaskargs结构体%T\n", taskArgs)
		go func(taskArgs DoTaskArgs, registerChan chan string) {
			defer wg.Done()
			for {
				worker := <-registerChan
				m := call(worker, "Worker.DoTask", taskArgs, nil)
				if m {
					// 如果不把registerChan <- worker包在协程里的话，
					// 最后一个task执行完，就会堵塞在registerChan <- worker这一步
					// 因为这是一个无缓冲同步的通道，没有了读取，就会堵塞。
					go func() {
						registerChan <- worker
					}()
					break
				}
			}
		}(taskArgs, registerChan)
	}
	wg.Wait()
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
