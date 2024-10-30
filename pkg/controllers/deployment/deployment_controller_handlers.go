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

package deployment

import (
	"context"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/controllers/util"
)

func (dc *deploymentController) addDeploymentHandler(obj interface{}) {
	deployment := obj.(*v1.Deployment)

	dc.queue.Add(types.NamespacedName{
		Name:      deployment.Name,
		Namespace: deployment.Namespace,
	})
}

func (dc *deploymentController) createPodGroupForDeployment(deployment *v1.Deployment) error {
	podGroupName := generatePodGroupName(deployment)

	if _, err := dc.podGroupLister.PodGroups(deployment.Namespace).Get(podGroupName); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get PodGroup for Deployment <%s/%s>, err: %v", deployment.Namespace, deployment.Name, err)
			return err
		}

		podGroup := &schedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            podGroupName,
				Namespace:       deployment.Namespace,
				OwnerReferences: newDeploymentPodGroupOwnerReferences(deployment),
			},
			Spec: schedulingv1beta1.PodGroupSpec{
				MinMember: 1,
				// Get the queue name from the annotation, it may be empty.
				Queue:             deployment.Annotations[scheduling.QueueNameAnnotationKey],
				PriorityClassName: deployment.Spec.Template.Spec.PriorityClassName,
				MinResources:      util.GetPodQuotaUsage(&corev1.Pod{Spec: deployment.Spec.Template.Spec}),
			},
			Status: schedulingv1beta1.PodGroupStatus{
				Phase: schedulingv1beta1.PodGroupPending,
			},
		}

		if _, err = dc.vcClient.SchedulingV1beta1().PodGroups(podGroup.Namespace).Create(context.TODO(), podGroup, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				klog.Errorf("Failed to create PodGroup <%s/%s> for Deployment <%s/%s>, err: %v",
					deployment.Namespace, podGroupName, deployment.Namespace, deployment.Name, err)
				return err
			}
		}
	} else {
		klog.Errorf("PodGroup for Deployment <%s/%s> is exists, skip to create PodGroup.", deployment.Namespace, deployment.Name)
		return nil
	}

	return dc.updatePodPodGroupAnnotation(deployment, podGroupName)
}

func (dc *deploymentController) updatePodPodGroupAnnotation(deployment *v1.Deployment, podGroupName string) error {
	if deployment.Annotations == nil {
		deployment.Annotations = map[string]string{}
	}

	if deployment.Annotations[scheduling.KubeGroupNameAnnotationKey] == "" {
		deployment.Annotations[scheduling.KubeGroupNameAnnotationKey] = podGroupName
		if _, err := dc.kubeClient.AppsV1().Deployments(deployment.Namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{}); err != nil {
			klog.Errorf("Failed to update Deployment <%s/%s>, err: %v", deployment.Namespace, deployment.Name, err)
			return err
		}
	} else if deployment.Annotations[scheduling.KubeGroupNameAnnotationKey] != podGroupName {
		klog.Errorf("Deployment <%s/%s> PodGroup annotations %s values is not %s, but %s",
			deployment.Namespace, deployment.Name, scheduling.KubeGroupNameAnnotationKey, podGroupName, deployment.Annotations[scheduling.KubeGroupNameAnnotationKey])
	}
	return nil
}

func newDeploymentPodGroupOwnerReferences(deployment *v1.Deployment) []metav1.OwnerReference {
	return []metav1.OwnerReference{*metav1.NewControllerRef(deployment, v1.SchemeGroupVersion.WithKind("Deployment"))}
}
