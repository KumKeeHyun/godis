package controller

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/client"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"net"
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
			return nil
		}
		return err
	}

	clusterConfig, err := c.kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName(cluster), metav1.GetOptions{})
	if errors.IsNotFound(err) || cluster.Status.Status == "Initializing" {
		logger.Info("initialize new cluster", "name", cluster.Name)
		cluster, clusterConfig, err = c.initializeCluster(cluster, clusterConfig)

		if err != nil {
			return err
		}
		if cluster.Spec.Replicas != nil && *cluster.Spec.Replicas != cluster.Status.Replicas {
			// for requeuing
			return fmt.Errorf("remain events")
		}
		return nil
	}

	godisList, err := c.godisClient.KumkeehyunV1().Godises(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: godisSelector(cluster).String(),
	})
	if err != nil {
		return err
	}

	if requiresScaleOut(cluster, godisList) {
		logger.Info("scale out cluster", "name", cluster.Name)
		err = c.ScaleOutCluster(cluster, clusterConfig, godisList)
	} else if requiresScaleIn(cluster, godisList) {
		logger.Info("scale in cluster", "name", cluster.Name)
		err = c.ScaleInCluster(cluster, clusterConfig, godisList)
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) initializeCluster(cluster *godisapis.GodisCluster, clusterConfig *corev1.ConfigMap) (*godisapis.GodisCluster, *corev1.ConfigMap, error) {
	var err error
	namespace := cluster.Namespace

	// update status to initializing
	if cluster.Status.Status != "Initializing" {
		clusterCopy := cluster.DeepCopy()
		clusterCopy.Status.Status = "Initializing"
		initialReplicas := int(*cluster.Spec.Replicas)
		clusterCopy.Status.InitialReplicas = &initialReplicas
		cluster, err = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
		if err != nil {
			return nil, nil, err
		}
	}

	// create new configMap
	clusterConfig, err = c.kubeClient.CoreV1().ConfigMaps(namespace).Create(context.TODO(), newConfigMapForInit(cluster, *cluster.Status.InitialReplicas), metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, nil, err
	}

	// create godises
	for id := 1; id <= *cluster.Status.InitialReplicas; id++ {
		_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Get(context.TODO(), godisName(cluster, id), metav1.GetOptions{})
		if errors.IsNotFound(err) {
			_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Create(context.TODO(), newGodis(cluster, id, true), metav1.CreateOptions{})
		}
		if err != nil {
			return nil, nil, err
		}
	}

	// update status to running
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = int32(*cluster.Status.InitialReplicas)
	clusterCopy.Status.InitialReplicas = nil
	cluster, err = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
	return cluster, clusterConfig, err
}

func requiresScaleOut(cluster *godisapis.GodisCluster, godisList *godisapis.GodisList) bool {
	return (cluster.Status.Status == "Running" && int(*cluster.Spec.Replicas) > len(godisList.Items)) ||
		(cluster.Status.Status == "Scaling" && cluster.Status.ScaleOutID != nil)
}

func (c *Controller) ScaleOutCluster(cluster *godisapis.GodisCluster, clusterConfig *corev1.ConfigMap, godisList *godisapis.GodisList) error {
	var err error
	var newID int
	namespace := cluster.Namespace

	// update status to scaling
	if cluster.Status.Status != "Scaling" {
		clusterCopy := cluster.DeepCopy()
		clusterCopy.Status.Status = "Scaling"
		newID, _ = strconv.Atoi(clusterConfig.Data["next-id"])
		clusterCopy.Status.ScaleOutID = &newID
		cluster, err = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	newID = *cluster.Status.ScaleOutID

	// request forget in config and not in godisList

	// update config
	clusterConfig, err = c.kubeClient.CoreV1().ConfigMaps(namespace).Update(context.TODO(), newConfigMapForJoin(cluster, clusterConfig, godisList, newID), metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	// request meet command

	err = sendMeetCommand(cluster, newID, godisList)
	if err != nil {
		return err
	}

	// create godis
	_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Get(context.TODO(), godisName(cluster, newID), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = c.godisClient.KumkeehyunV1().Godises(namespace).Create(context.TODO(), newGodis(cluster, newID, true), metav1.CreateOptions{})
	}
	if err != nil {
		return err
	}

	// update status to running
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = int32(len(strings.Split(clusterConfig.Data["initial-cluster"], ",")))
	clusterCopy.Status.ScaleOutID = nil
	_, err = c.godisClient.KumkeehyunV1().GodisClusters(namespace).UpdateStatus(context.TODO(), clusterCopy, metav1.UpdateOptions{})

	return err
}

func requiresScaleIn(cluster *godisapis.GodisCluster, godisList *godisapis.GodisList) bool {
	return (cluster.Status.Status == "Running" && int(*cluster.Spec.Replicas) < len(godisList.Items)) ||
		(cluster.Status.Status == "Scaling" && cluster.Status.ScaleInID != nil)
}

func (c *Controller) ScaleInCluster(cluster *godisapis.GodisCluster, clusterConfig *corev1.ConfigMap, godisList *godisapis.GodisList) error {
	return nil
}

func godisSelector(cluster *godisapis.GodisCluster) labels.Selector {
	clusterReq, _ := labels.NewRequirement("cluster-name", selection.Equals, []string{cluster.Name})
	return labels.NewSelector().Add(*clusterReq)
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

func godisPeerURL(cluster *godisapis.GodisCluster, id int) string {
	return fmt.Sprintf("http://%s:6300", godisServiceFQDN(cluster, id))
}

func clusterPeers(cluster *godisapis.GodisCluster, ids []int) string {
	initialPeer := func(id int) string {
		return fmt.Sprintf("%d@%s", id, godisPeerURL(cluster, id))
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
			Name:      configMapName(cluster),
			Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, godisapis.SchemeGroupVersion.WithKind("GodisCluster")),
			},
		},
		Data: map[string]string{
			"next-id":         strconv.Itoa(replicas + 1),
			"initial-cluster": clusterPeers(cluster, ids),
		},
	}
}

func newConfigMapForJoin(cluster *godisapis.GodisCluster, oldCfg *corev1.ConfigMap, godisList *godisapis.GodisList, newID int) *corev1.ConfigMap {
	ids := make([]int, 0)
	for _, godis := range godisList.Items {
		_, id, _ := splitGodisNameID(godis.Name)
		if id != newID {
			ids = append(ids, id)
		}
	}
	ids = append(ids, newID)

	newCfg := oldCfg.DeepCopy()
	newCfg.Data["next-id"] = strconv.Itoa(newID + 1)
	newCfg.Data["initial-cluster"] = clusterPeers(cluster, ids)
	return newCfg
}

func newConfigMapForRemove(cluster *godisapis.GodisCluster, oldCfg *corev1.ConfigMap) *corev1.ConfigMap {
	newCfg := oldCfg.DeepCopy()
	return newCfg
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

func sendMeetCommand(cluster *godisapis.GodisCluster, newID int, godisList *godisapis.GodisList) error {
	meetCmd := meetReply(cluster, newID)
	return sendRequest(cluster, godisList, meetCmd)
}

func meetReply(cluster *godisapis.GodisCluster, newID int) resp.Reply {
	reply := &resp.ArrayReply{
		Len:   4,
		Value: make([]resp.Reply, 4),
	}
	reply.Value[0] = &resp.SimpleStringReply{Value: "cluster"}
	reply.Value[1] = &resp.SimpleStringReply{Value: "meet"}
	reply.Value[2] = &resp.SimpleStringReply{Value: strconv.Itoa(newID)}
	reply.Value[3] = &resp.SimpleStringReply{Value: godisPeerURL(cluster, newID)}
	return reply
}

func sendForgetCommand(cluster *godisapis.GodisCluster, deletedID int, godisList *godisapis.GodisList) error {
	forgetCmd := forgetReply(deletedID)
	return sendRequest(cluster, godisList, forgetCmd)
}

func forgetReply(deletedID int) resp.Reply {
	reply := &resp.ArrayReply{
		Len:   3,
		Value: make([]resp.Reply, 3),
	}
	reply.Value[0] = &resp.SimpleStringReply{Value: "cluster"}
	reply.Value[1] = &resp.SimpleStringReply{Value: "forget"}
	reply.Value[2] = &resp.SimpleStringReply{Value: strconv.Itoa(deletedID)}
	return reply
}

func sendRequest(cluster *godisapis.GodisCluster, godisList *godisapis.GodisList, reply resp.Reply) error {
	sendRequestTo := func(id int) error {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:6379", godisServiceFQDN(cluster, id)))
		if err != nil {
			return err
		}
		defer conn.Close()

		return client.SendRequest(conn, reply).Err()
	}

	for _, godis := range godisList.Items {
		_, id, _ := splitGodisNameID(godis.Name)
		if err := sendRequestTo(id); err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to send meet command")
}
