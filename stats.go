package wpn

type Stats struct {
	WorkerCount int
	BusyWorkers int
	QueueLength int

	ProcessedJobs int
	FailedJobs    int
	RetriedJobs   int

	MissRate float64
}
