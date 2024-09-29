/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"

	"volcano.sh/volcano-global/pkg/utils"
)

// CreateDefaultQueue Create the default queue.
func CreateDefaultQueue(volcanoClient volcanoclientset.Interface, queueName string) {
	err := retry.OnError(wait.Backoff{
		Steps:    60,
		Duration: time.Second,
		Factor:   1,
		Jitter:   0.1,
	}, func(err error) bool {
		return !apierrors.IsAlreadyExists(err)
	}, func() error {
		_, err := volcanoClient.SchedulingV1beta1().Queues().Create(context.TODO(), &schedulingv1beta1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name: queueName,
			},
			Spec: schedulingv1beta1.QueueSpec{
				Reclaimable: utils.ToPointer(true),
				Weight:      1,
			},
		}, metav1.CreateOptions{})
		return err
	})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		panic(fmt.Errorf("failed create default queue, with err: %v", err))
	}
}
