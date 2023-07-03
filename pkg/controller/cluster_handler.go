package controller

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
)

// runClusterWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runClusterWorker(ctx context.Context) {
	for c.processNextClusterItem(ctx) {
	}
}

// processNextClusterItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextClusterItem(ctx context.Context) bool {
	obj, shutdown := c.clusterQueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.clusterQueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.clusterQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		// Run the syncHandler, passing it the namespace/name string of the
		// Godis resource to be synced.
		if err := c.clusterSyncHandler(ctx, key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.clusterQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		c.clusterQueue.Forget(obj)
		logger.Info("Successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// clusterSyncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Godis resource
// with the current status of the resource.
func (c *Controller) clusterSyncHandler(ctx context.Context, key string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "kind", "GodisCluster", "resourceName", key)

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	cluster, err := c.clusterLister.GodisClusters(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("godis cluster '%s' in work queue no longer exists", key))
			// delete
			// TODO delete logic
			return nil
		}
		return err
	}

	logger.Info("detect cluster event", "name", cluster.Name, "replicas", *cluster.Spec.Replicas)

	initialize := false
	configName := configMapName(cluster)
	clusterConfig, err := c.kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, configName, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("error get cluster configMap '%s'", configName))
			return err
		}

		logger.Info("create new godis cluster config", "name", configName)
		initialize = true

		clusterConfig = newConfigMap(cluster)
		clusterConfig.Data["initial-cluster"] = clusterPeers(cluster, int(*cluster.Spec.Replicas))
		_, err = c.kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, clusterConfig, metav1.CreateOptions{})
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error create cluster configMap '%s'", configName))
			return err
		}
	}

	selector := clusterSelector(cluster)
	godises, err := c.godisClient.KumkeehyunV1().Godises(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error list godises with selector '%s'", selector.String()))
		return err
	}

	// TODO: initialize 조건 다시 구해야 함, 생성 도중 실패했을 때 다음 작업에서 초기화인지 스케일아웃인지 구분해야 함
	if initialize {
		for id := 1; id <= int(*cluster.Spec.Replicas); id++ {
			_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Create(ctx, newGodis(cluster, id, true), metav1.CreateOptions{})
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("error create godis '%s'", godisName(cluster, id)))
				return err
			}
		}

		err = c.updateClusterStatus(cluster, int(*cluster.Spec.Replicas))
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error update godis cluster status '%s'", cluster.Name))
			return err
		}
		return nil
	}

	// TODO: scale out/in 조건 다시 구해야 함
	if int(*cluster.Spec.Replicas) > len(godises.Items) {
		// scale out
		newID := len(godises.Items) + 1
		configCopy := clusterConfig.DeepCopy()
		configCopy.Data["initial-cluster"] = clusterPeers(cluster, len(godises.Items)+1)
		_, err = c.kubeClient.CoreV1().ConfigMaps(namespace).Update(ctx, configCopy, metav1.UpdateOptions{})
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error update cluster configMap '%s'", configName))
			return err
		}

		_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Create(ctx, newGodis(cluster, newID, false), metav1.CreateOptions{})
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error create godis '%s'", godisName(cluster, newID)))
			return err
		}

		err = c.updateClusterStatus(cluster, len(godises.Items)+1)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error update godis cluster status '%s'", cluster.Name))
			return err
		}

		return fmt.Errorf("scale out step by step. new: %d", newID)
	} else if int(*cluster.Spec.Replicas) < len(godises.Items) {
		// scale in
		deletedID := len(godises.Items)
		err = c.godisClient.KumkeehyunV1().Godises(namespace).Delete(ctx, godisName(cluster, deletedID), metav1.DeleteOptions{})
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error delete godis '%s'", godisName(cluster, deletedID)))
			return err
		}

		err = c.updateClusterStatus(cluster, len(godises.Items)-1)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error update godis cluster status '%s'", cluster.Name))
			return err
		}

		return fmt.Errorf("scale in step by step. delete: %d", deletedID)
	}

	return nil
}

func clusterSelector(cluster *godisapis.GodisCluster) labels.Selector {
	clusterReq, _ := labels.NewRequirement("cluster-name", selection.Equals, []string{cluster.Name})
	return labels.NewSelector().Add(*clusterReq)
}

func (c *Controller) updateClusterStatus(cluster *godisapis.GodisCluster, replicas int) error {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Replicas = int32(replicas)

	_, err := c.godisClient.KumkeehyunV1().GodisClusters(cluster.Namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
	return err
}

func configMapName(cluster *godisapis.GodisCluster) string {
	return cluster.Name + "-config"
}

func godisName(cluster *godisapis.GodisCluster, id int) string {
	return fmt.Sprintf("%s-%d", cluster.Name, id)
}

func splitGodisNameID(godisName string) (name string, id int, err error) {
	parts := strings.Split(godisName, "-")
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("unexpected godisName format: %s", godisName)
	}
	id, err = strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", 0, fmt.Errorf("unexpected godisName format: %s", godisName)
	}
	return strings.Join(parts[:len(parts)-1], "-"), id, nil
}

func godisServiceName(cluster *godisapis.GodisCluster, id int) string {
	return fmt.Sprintf("%s-%d-endpoint", cluster.Name, id)
}

func godisServiceFQDN(cluster *godisapis.GodisCluster, id int) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", godisServiceName(cluster, id), cluster.Namespace)
}

func clusterPeers(cluster *godisapis.GodisCluster, replicas int) string {
	peerURL := func(id int, host string) string {
		return fmt.Sprintf("%d@http://%s:6300", id, host)
	}
	peers := make([]string, 0, replicas)
	for id := 1; id <= replicas; id++ {
		peers = append(peers, peerURL(id, godisServiceFQDN(cluster, id)))
	}
	return strings.Join(peers, ",")
}

func newConfigMap(cluster *godisapis.GodisCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(cluster),
			Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, godisapis.SchemeGroupVersion.WithKind("GodisCluster")),
			},
		},
		Immutable: nil,
		Data:      map[string]string{},
	}
}

func newGodis(cluster *godisapis.GodisCluster, id int, initial bool) *godisapis.Godis {
	godisLabels := map[string]string{
		"cluster-name": cluster.Name,
	}
	return &godisapis.Godis{
		ObjectMeta: metav1.ObjectMeta{
			Name:      godisName(cluster, id),
			Namespace: cluster.Namespace,
			Labels:    godisLabels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, godisapis.SchemeGroupVersion.WithKind("GodisCluster")),
			},
		},
		Spec: godisapis.GodisSpec{
			Preferred: initial,
		},
	}
}
