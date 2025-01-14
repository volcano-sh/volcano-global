package workload

type Workload interface {
	GetQueueName() string
	GetPriorityClassName() string
}
