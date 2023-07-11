package controller

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"strconv"
	"strings"
	"time"

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
		if err := c.syncCluster(ctx, key); err != nil {
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

// syncCluster compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Godis resource
// with the current status of the resource.
func (c *Controller) syncCluster(ctx context.Context, key string) error {
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
			return nil
		}
		return err
	}

	godises, err := c.godisLister.Godises(namespace).List(godisSelector(cluster))
	if err != nil {
		return err
	}

	clusterConfig, err := c.kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName(cluster.Name), metav1.GetOptions{})
	configNotExists := err != nil && errors.IsNotFound(err)

	if requiresInitialize(cluster, configNotExists) {
		logger.Info("initialize cluster")
		cluster, err = c.initializeCluster(cluster)
	} else if requiresScaleOut(cluster, godises) {
		logger.Info("scale out cluster")
		cluster, err = c.scaleOutCluster(cluster, godises, clusterConfig)
	} else if requiresScaleIn(cluster, godises) {
		logger.Info("scale in cluster")
		cluster, err = c.scaleInCluster(cluster, godises, clusterConfig)
	}
	if err != nil {
		return err
	}
	if cluster.Spec.Replicas != nil && *cluster.Spec.Replicas != cluster.Status.Replicas {
		logger.Info("remained job for scaling")
		c.clusterQueue.AddAfter(key, 5*time.Second)
	}

	return nil
}

func requiresInitialize(cluster *godisapis.GodisCluster, configNotExists bool) bool {
	return configNotExists || (cluster.Status.Status == "Initializing")
}

func (c *Controller) initializeCluster(cluster *godisapis.GodisCluster) (*godisapis.GodisCluster, error) {
	namespace := cluster.Namespace
	var updateStatusErr error

	// update status to initializing
	if cluster.Status.Status != "Initializing" {
		clusterCopy := cluster.DeepCopy()
		clusterCopy.Status.Status = "Initializing"
		initialReplicas := int(*cluster.Spec.Replicas)
		clusterCopy.Status.InitialReplicas = &initialReplicas
		clusterCopy.Status.NextID = initialReplicas + 1

		cluster, updateStatusErr = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
		if updateStatusErr != nil {
			return nil, updateStatusErr
		}
	}

	// create new configMap
	_, createCfgErr := c.kubeClient.CoreV1().ConfigMaps(namespace).Create(context.TODO(), newConfigMapForInit(cluster, *cluster.Status.InitialReplicas), metav1.CreateOptions{})
	if createCfgErr != nil && !errors.IsAlreadyExists(createCfgErr) {
		return nil, createCfgErr
	}

	// create godises
	for id := 1; id <= *cluster.Status.InitialReplicas; id++ {
		_, createGodisErr := c.godisClient.KumkeehyunV1().Godises(namespace).Create(context.TODO(), newGodis(cluster, id, true), metav1.CreateOptions{})
		if createGodisErr != nil && !errors.IsAlreadyExists(createGodisErr) {
			return nil, createGodisErr
		}
	}

	// update status to running
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = int32(*cluster.Status.InitialReplicas)
	clusterCopy.Status.InitialReplicas = nil
	return c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
}

func requiresScaleOut(cluster *godisapis.GodisCluster, godises []*godisapis.Godis) bool {
	return (cluster.Status.Status == "Running" && int(*cluster.Spec.Replicas) > len(godises)) ||
		(cluster.Status.Status == "Scaling" && cluster.Status.ScaleOutID != nil)
}

func (c *Controller) scaleOutCluster(cluster *godisapis.GodisCluster, godises []*godisapis.Godis, config *corev1.ConfigMap) (*godisapis.GodisCluster, error) {
	namespace := cluster.Namespace
	var updateStatusErr error
	var newID int

	// update status to scaling
	if cluster.Status.Status != "Scaling" {
		clusterCopy := cluster.DeepCopy()
		clusterCopy.Status.Status = "Scaling"
		newID = cluster.Status.NextID
		clusterCopy.Status.ScaleOutID = &newID
		clusterCopy.Status.NextID = newID + 1
		cluster, updateStatusErr = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
		if updateStatusErr != nil {
			return nil, updateStatusErr
		}
	} else {
		newID = *cluster.Status.ScaleOutID
	}

	// request forget in config and not in godisList
	deletedIDs := detectDeletedIDs(config, godises, newID)
	for _, deletedID := range deletedIDs {
		forgetCmdErr := c.clusterClient.Forget(godises, deletedID)
		if forgetCmdErr != nil {
			return nil, forgetCmdErr
		}
	}

	// update config
	_, updateCfgErr := c.kubeClient.CoreV1().ConfigMaps(namespace).Update(context.TODO(), newConfigMapForJoin(config, cluster, godises, newID), metav1.UpdateOptions{})
	if updateCfgErr != nil {
		return nil, updateCfgErr
	}

	// request meet command
	meetCmdErr := c.clusterClient.Meet(godises, newID)
	if meetCmdErr != nil {
		return nil, meetCmdErr
	}

	// create godis
	_, createGodisErr := c.godisClient.KumkeehyunV1().Godises(namespace).Create(context.TODO(), newGodis(cluster, newID, false), metav1.CreateOptions{})
	if createGodisErr != nil && !errors.IsAlreadyExists(createGodisErr) {
		return nil, createGodisErr
	}

	// update status to running
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = cluster.Status.Replicas + 1
	clusterCopy.Status.ScaleOutID = nil
	return c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
}

func detectDeletedIDs(config *corev1.ConfigMap, godises []*godisapis.Godis, newID int) []int {
	peers := strings.Split(config.Data["initial-cluster"], ",")
	peerIDs := make(map[int]struct{})
	for _, peer := range peers {
		peerID, _ := strconv.Atoi(strings.Split(peer, "@")[0])
		peerIDs[peerID] = struct{}{}
	}

	for _, godis := range godises {
		_, id, _ := splitGodisNameID(godis.Name)
		delete(peerIDs, id)
	}

	deletedIDs := make([]int, 0)
	for id := range peerIDs {
		if id != newID {
			deletedIDs = append(deletedIDs, id)
		}
	}
	return deletedIDs
}

func requiresScaleIn(cluster *godisapis.GodisCluster, godisList []*godisapis.Godis) bool {
	return (cluster.Status.Status == "Running" && int(*cluster.Spec.Replicas) < len(godisList)) ||
		(cluster.Status.Status == "Scaling" && cluster.Status.ScaleInID != nil)
}

func (c *Controller) scaleInCluster(cluster *godisapis.GodisCluster, godises []*godisapis.Godis, config *corev1.ConfigMap) (*godisapis.GodisCluster, error) {
	namespace := cluster.Namespace
	var updateStatusErr error
	var deletedID int

	// update status to scaling
	if cluster.Status.Status != "Scaling" {
		clusterCopy := cluster.DeepCopy()
		clusterCopy.Status.Status = "Scaling"
		deletedID = selectDeletedID(godises)
		clusterCopy.Status.ScaleInID = &deletedID
		cluster, updateStatusErr = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
		if updateStatusErr != nil {
			return nil, updateStatusErr
		}
	}
	deletedID = *cluster.Status.ScaleInID

	// update config
	_, updateCfgErr := c.kubeClient.CoreV1().ConfigMaps(namespace).Update(context.TODO(), newConfigMapForRemove(config, cluster, godises, deletedID), metav1.UpdateOptions{})
	if updateCfgErr != nil {
		return nil, updateCfgErr
	}

	// delete godis
	deleteGodisErr := c.godisClient.KumkeehyunV1().Godises(namespace).Delete(context.TODO(), godisName(cluster.Name, deletedID), metav1.DeleteOptions{})
	if deleteGodisErr != nil && !errors.IsNotFound(deleteGodisErr) {
		return nil, deleteGodisErr
	}

	// request forget command
	forgetCmdErr := c.clusterClient.Forget(godises, deletedID)
	if forgetCmdErr != nil {
		return nil, forgetCmdErr
	}

	// update status to running
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = cluster.Status.Replicas - 1
	clusterCopy.Status.ScaleInID = nil
	return c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
}

func selectDeletedID(godises []*godisapis.Godis) int {
	// TODO: follower 선택하는 로직
	deleted := godises[len(godises)-1]
	_, deletedID, _ := splitGodisNameID(deleted.Name)
	return deletedID
}

func godisSelector(cluster *godisapis.GodisCluster) labels.Selector {
	clusterReq, _ := labels.NewRequirement("cluster-name", selection.Equals, []string{cluster.Name})
	return labels.NewSelector().Add(*clusterReq)
}

func clusterPeers(cluster *godisapis.GodisCluster, ids []int) string {
	initialPeer := func(id int) string {
		return fmt.Sprintf("%d@%s", id, godisPeerURL(cluster.Namespace, godisName(cluster.Name, id)))
	}
	peers := make([]string, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, initialPeer(id))
	}
	return strings.Join(peers, ",")
}

func newConfigMapForInit(cluster *godisapis.GodisCluster, replicas int) *corev1.ConfigMap {
	ids := make([]int, 0, replicas)
	for id := 1; id <= replicas; id++ {
		ids = append(ids, id)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(cluster.Name),
			Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, godisapis.SchemeGroupVersion.WithKind("GodisCluster")),
			},
		},
		Data: map[string]string{
			"initial-cluster": clusterPeers(cluster, ids),
		},
	}
}

func newConfigMapForJoin(oldCfg *corev1.ConfigMap, cluster *godisapis.GodisCluster, godises []*godisapis.Godis, newID int) *corev1.ConfigMap {
	ids := make([]int, 0)
	for _, godis := range godises {
		_, id, _ := splitGodisNameID(godis.Name)
		if id != newID {
			ids = append(ids, id)
		}
	}
	ids = append(ids, newID)

	newCfg := oldCfg.DeepCopy()
	newCfg.Data["initial-cluster"] = clusterPeers(cluster, ids)
	return newCfg
}

func newConfigMapForRemove(oldCfg *corev1.ConfigMap, cluster *godisapis.GodisCluster, godises []*godisapis.Godis, deletedID int) *corev1.ConfigMap {
	ids := make([]int, 0)
	for _, godis := range godises {
		_, id, _ := splitGodisNameID(godis.Name)
		if id != deletedID {
			ids = append(ids, id)
		}
	}

	newCfg := oldCfg.DeepCopy()
	newCfg.Data["initial-cluster"] = clusterPeers(cluster, ids)
	return newCfg
}

func newGodis(cluster *godisapis.GodisCluster, id int, initial bool) *godisapis.Godis {
	godisLabels := map[string]string{
		"cluster-name": cluster.Name,
	}
	return &godisapis.Godis{
		ObjectMeta: metav1.ObjectMeta{
			Name:      godisName(cluster.Name, id),
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
