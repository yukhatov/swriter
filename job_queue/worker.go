package job_queue

// Job holds the attributes needed to perform unit of work.

type Job struct {
	Name      string
	Payload   interface{}
	Processor func(interface{})
}

// NewWorker creates takes a numeric id and a channel w/ worker pool.
func NewWorker(id int, workerPool chan chan Job) Worker {
	return Worker{
		id:         id,
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		quitChan:   make(chan bool),
	}
}

type Worker struct {
	id         int
	jobQueue   chan Job
	workerPool chan chan Job
	quitChan   chan bool
}

func (w Worker) startWorkerWithSinglePayload() {
	go func() {
		for {
			// Add my jobQueue to the worker pool.
			w.workerPool <- w.jobQueue

			select {
			case job := <-w.jobQueue:
				job.Processor(job.Payload)
			case <-w.quitChan:
				return
			}
		}
	}()
}

func (w Worker) startWorkerWithMultiplePayloads(numberOfPayloadsAtSameTime int) {
	go func() {
		var lastJobReceived Job
		var jobsPayloadList []interface{}
		jobsPayloadList = make([]interface{}, numberOfPayloadsAtSameTime)
		var elementsInList int
		for {
			// Add my jobQueue to the worker pool.
			w.workerPool <- w.jobQueue

			select {
			case job := <-w.jobQueue:
				if job.Name == "send_stats" {
					if elementsInList > 0 {
						lastJobReceived.Processor(jobsPayloadList[0:elementsInList])
						jobsPayloadList = make([]interface{}, numberOfPayloadsAtSameTime)
						elementsInList = 0
					}
					break
				}

				lastJobReceived = job
				if numberOfPayloadsAtSameTime > elementsInList {
					jobsPayloadList[elementsInList] = job.Payload
					elementsInList++
				}

				if numberOfPayloadsAtSameTime == elementsInList {
					job.Processor(jobsPayloadList)
					jobsPayloadList = make([]interface{}, numberOfPayloadsAtSameTime)
					elementsInList = 0
				}

			case <-w.quitChan:
				if elementsInList > 0 {
					lastJobReceived.Processor(jobsPayloadList[0:elementsInList])
				}
				return
			}
		}
	}()
}

func (w Worker) stop() {
	go func() {
		w.quitChan <- true
	}()
}
