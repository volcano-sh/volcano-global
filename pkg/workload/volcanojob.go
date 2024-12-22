package workload

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

type VolcanoJobWorkload struct {
	resource *v1alpha1.Job
}

var _ Workload = &VolcanoJobWorkload{}

func NewVolcanoJobWorkload(obj *unstructured.Unstructured) (Workload, error) {
	var vcjob v1alpha1.Job
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &vcjob)
	if err != nil {
		klog.Errorf("Failed to convert to volcano job workload: %v", err)
		return nil, err
	}
	return &VolcanoJobWorkload{resource: &vcjob}, nil
}

func (vcjob *VolcanoJobWorkload) GetQueueName() string {
	return vcjob.resource.Spec.Queue
}

func (vcjob *VolcanoJobWorkload) GetPriorityClassName() string {
	return vcjob.resource.Spec.PriorityClassName
}
