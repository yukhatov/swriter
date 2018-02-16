package job_queue

type Dispatcher struct {
	workerPool chan chan Job
	maxWorkers int
	jobQueue   chan Job
	workers    []*Worker
}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(jobQueue chan Job, maxWorkers int) *Dispatcher {
	workerPool := make(chan chan Job, maxWorkers)
	workers := make([]*Worker, maxWorkers)

	return &Dispatcher{
		jobQueue:   jobQueue,
		maxWorkers: maxWorkers,
		workerPool: workerPool,
		workers:    workers,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(i+1, d.workerPool)
		worker.startWorkerWithSinglePayload()
		d.workers[i] = &worker
	}

	go d.dispatch()
}

func (d *Dispatcher) RunWorkersWithMultiplePayloads(numberOfPayloadsAtSameTime int) {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(i+1, d.workerPool)
		worker.startWorkerWithMultiplePayloads(numberOfPayloadsAtSameTime)
		d.workers[i] = &worker
	}

	go d.dispatch()
}

func (d *Dispatcher) Stop() {
	for _, worker := range d.workers {
		worker.stop()
	}
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			go func() {
				workerJobQueue := <-d.workerPool
				workerJobQueue <- job
			}()
		}
	}
}
