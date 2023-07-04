package controller

import (
	"context"
	"fmt"
	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"strconv"
)

// runGodisWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runGodisWorker(ctx context.Context) {
	for c.processNextGodisItem(ctx) {
	}
}

// processNextGodisItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextGodisItem(ctx context.Context) bool {
	obj, shutdown := c.godisQueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.godisQueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.godisQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		// Run the syncHandler, passing it the namespace/name string of the
		// Godis resource to be synced.
		if err := c.godisSyncHandler(ctx, key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.godisQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		c.godisQueue.Forget(obj)
		logger.Info("Successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// godisSyncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Godis resource
// with the current status of the resource.
func (c *Controller) godisSyncHandler(ctx context.Context, key string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "kind", "Godis", "resourceName", key)

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	godis, err := c.godisLister.Godises(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("godis cluster '%s' in work queue no longer exists", key))

			// delete
			err = c.kubeClient.CoreV1().Services(namespace).Delete(context.TODO(), name+"-endpoint", metav1.DeleteOptions{})
			if !errors.IsNotFound(err) {
				utilruntime.HandleError(fmt.Errorf("error delete service '%s'", name+"-endpoint"))
				return err
			}

			err = c.kubeClient.AppsV1().ReplicaSets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
			if !errors.IsNotFound(err) {
				utilruntime.HandleError(fmt.Errorf("error delete replicaSet '%s'", name))
				return err
			}

			return nil
		}
		return err
	}

	logger.Info("detect godis event", "name", godis.Name)

	_, err = c.kubeClient.CoreV1().Services(namespace).Get(context.TODO(), name+"-endpoint", metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = c.kubeClient.CoreV1().Services(namespace).Create(context.TODO(), newService(godis), metav1.CreateOptions{})
	}
	if err != nil {
		return err
	}

	_, err = c.kubeClient.AppsV1().ReplicaSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = c.kubeClient.AppsV1().ReplicaSets(namespace).Create(context.TODO(), newReplicaSet(godis), metav1.CreateOptions{})
	}
	if err != nil {
		return err
	}

	return nil
}

func podLabels(godis *godisapis.Godis) map[string]string {
	clusterName, _, _ := splitGodisNameID(godis.Name)
	return map[string]string{
		"cluster-name": clusterName,
		"node-name":    godis.Name,
	}
}

//	func godisPodName(godis *godisapis.Godis, id int) string {
//		return fmt.Sprintf("%s-%d", godis.Name, id)
//	}
func newService(godis *godisapis.Godis) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      godis.Name + "-endpoint",
			Namespace: godis.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(godis, godisapis.SchemeGroupVersion.WithKind("Godis")),
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: "client",
					Port: 6379,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 6379,
					},
				},
				{
					Name: "peer",
					Port: 6300,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 6300,
					},
				},
			},
			Selector: podLabels(godis),
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
}

func newReplicaSet(godis *godisapis.Godis) *appsv1.ReplicaSet {
	replicas := int32(1)
	minReadySec := int32(3)
	clusterName, id, _ := splitGodisNameID(godis.Name)
	labels := podLabels(godis)

	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      godis.Name,
			Namespace: godis.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(godis, godisapis.SchemeGroupVersion.WithKind("Godis")),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas:        &replicas,
			MinReadySeconds: minReadySec,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(godis, godisapis.SchemeGroupVersion.WithKind("Godis")),
					},
				},
				Spec: corev1.PodSpec{
					Hostname:  godis.Name,
					Subdomain: clusterName,
					//Volumes: []corev1.Volume{
					//	{
					//		Name:         "",
					//		VolumeSource: corev1.VolumeSource{},
					//	},
					//},
					Containers: []corev1.Container{
						{
							Name:  "godis",
							Image: "kbzjung359/godis",
							Args:  []string{"cluster"},
							Env: []corev1.EnvVar{
								{
									Name:  "CLUSTER_ID",
									Value: strconv.Itoa(id),
								},
								{
									Name: "CLUSTER_INITIAL-CLUSTER",
									ValueFrom: &corev1.EnvVarSource{
										ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: clusterName + "-config",
											},
											Key: "initial-cluster",
										},
									},
								},
								{
									Name:  "CLUSTER_WALDIR",
									Value: "/tmp/godis/wal",
								},
								{
									Name:  "CLUSTER_SNAPDIR",
									Value: "/tmp/godis/snap",
								},
								{
									Name:  "CLUSTER_JOIN",
									Value: strconv.FormatBool(!godis.Spec.Preferred),
								},
							},
						},
					},
				},
			},
		},
	}
}
