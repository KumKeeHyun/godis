package controller

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	godisclientset "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned"
	godisscheme "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned/scheme"
	godisinformers "github.com/KumKeeHyun/godis/pkg/controller/generated/informers/externalversions/godis/v1"
	godislisters "github.com/KumKeeHyun/godis/pkg/controller/generated/listers/godis/v1"
)

const controllerAgentName = "godis-controller"

type Controller struct {
	kubeClient  kubernetes.Interface
	godisClient godisclientset.Interface

	podsLister  corelisters.PodLister
	podsSynced  cache.InformerSynced
	godisLister godislisters.GodisLister
	godisSynced cache.InformerSynced

	workqueue workqueue.RateLimitingInterface
	recorder  record.EventRecorder
}

func New(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	godisClient godisclientset.Interface,
	podInformer coreinformers.PodInformer,
	godisInformer godisinformers.GodisInformer) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	utilruntime.Must(godisscheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(1)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(""),
	})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		corev1.EventSource{Component: controllerAgentName},
	)

	controller := &Controller{
		kubeClient:  kubeClient,
		godisClient: godisClient,
		podsLister:  podInformer.Lister(),
		podsSynced:  podInformer.Informer().HasSynced,
		godisLister: godisInformer.Lister(),
		godisSynced: godisInformer.Informer().HasSynced,
		workqueue:   workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		recorder:    recorder,
	}

	logger.Info("Setting up event handlers")
	godisInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: controller.enqueueGodis,
			UpdateFunc: func(oldObj, newObj interface{}) {
				controller.enqueueGodis(newObj)
			},
			DeleteFunc: controller.enqueueGodis,
		},
	)
	//podInformer.Informer().AddEventHandler(
	//	cache.ResourceEventHandlerFuncs{
	//		AddFunc: controller.handleObject,
	//		UpdateFunc: func(oldObj, newObj interface{}) {
	//			newPod := newObj.(*corev1.Pod)
	//			oldPod := oldObj.(*corev1.Pod)
	//			if newPod.ResourceVersion == oldPod.ResourceVersion {
	//				// Periodic resync will send update events for all known Deployments.
	//				// Two different versions of the same Deployment will always have different RVs.
	//				return
	//			}
	//			controller.handleObject(newObj)
	//		},
	//		DeleteFunc: controller.handleObject,
	//	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shut down the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting godis controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.podsSynced, c.godisSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Godis resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		// Run the syncHandler, passing it the namespace/name string of the
		// Godis resource to be synced.
		if err := c.syncHandler(ctx, key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		c.workqueue.Forget(obj)
		logger.Info("Successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Godis resource
// with the current status of the resource.
//
// add Godis -> create pods
// update Godis -> confChange + create/delete pods
// delete Godis -> delete pods
//

func (c *Controller) syncHandler(ctx context.Context, key string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "resourceName", key)

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	godis, err := c.godisLister.Godises(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			// delete
			utilruntime.HandleError(fmt.Errorf("godis '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}
	logger.Info("detect godis resource", "name", godis.Name, "replicas", *godis.Spec.Replicas, "members", godis.Status.Members)

	clusterReq, err := labels.NewRequirement("cluster-name", selection.Equals, []string{godis.Name})
	if err != nil {
		return err
	}
	selector := labels.NewSelector().Add(*clusterReq)
	pods, err := c.podsLister.Pods(godis.Namespace).List(selector)
	if err != nil {
		return err
	}
	logger.Info("find pods of controller", "name", godis.Name, "pods length", len(pods))

	if godis.Spec.Replicas != nil && len(pods) == 0 {
		// init
		initialIDs := make([]int, 0, *godis.Spec.Replicas)
		for id := 1; id <= int(*godis.Spec.Replicas); id++ {
			initialIDs = append(initialIDs, id)
		}
		for _, pod := range newPodsForInit(godis, initialIDs) {
			svc := newService(pod)
			_, err := c.kubeClient.CoreV1().Services(godis.Namespace).Create(context.TODO(), svc, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			_, err = c.kubeClient.CoreV1().Pods(godis.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		}
	} else if godis.Spec.Replicas != nil && int(*godis.Spec.Replicas) > len(pods) {

	} else if godis.Spec.Replicas != nil && int(*godis.Spec.Replicas) < len(pods) {

	}

	if err := c.updateGodisStatus(godis); err != nil {
		return err
	}

	return nil
}

func (c *Controller) updateGodisStatus(godis *godisapis.Godis) error {
	godisCopy := godis.DeepCopy()
	godisCopy.Status.Replicas = *godis.Spec.Replicas

	cluster := make([]string, 0, godisCopy.Status.Replicas)
	for i := 0; i < int(godisCopy.Status.Replicas); i++ {
		cluster = append(cluster, fmt.Sprintf("%d", i))
	}
	godisCopy.Status.Members = strings.Join(cluster, ",")

	_, err := c.godisClient.KumkeehyunV1().Godises(godis.Namespace).UpdateStatus(context.TODO(), godisCopy, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error update status: %v", err)
	}
	return nil
}

// enqueueGodis takes a Godis resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Godis.
func (c *Controller) enqueueGodis(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	logger := klog.FromContext(context.Background())
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		logger.V(4).Info("deleted object", "resourceName", object.GetName())
	}
	logger.V(4).Info("Processing object", "object", klog.KObj(object))
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		if ownerRef.Kind != "Godis" {
			return
		}

		godis, err := c.godisLister.Godises(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			logger.V(4).Info("Ignore orphaned object", "object", klog.KObj(object), "godis", ownerRef.Name)
			return
		}

		c.enqueueGodis(godis)
		return
	}
}

func newPodsForInit(godis *godisapis.Godis, ids []int) []*corev1.Pod {
	res := make([]*corev1.Pod, 0, len(ids))
	initialCluster := clusterPeers(godis, ids)
	for _, id := range ids {
		res = append(res, newPod(godis, id, initialCluster, false))
	}
	return res
}

func newPodForJoin(godis *godisapis.Godis, id int) *corev1.Pod {
	return newPod(godis, id, clusterPeersForJoin(godis, id), true)
}

func newPod(godis *godisapis.Godis, id int, initialCluster string, join bool) *corev1.Pod {
	podLabels := map[string]string{
		"cluster-name": godis.Name,
		"id":           strconv.Itoa(id),
	}
	podName := godisPodName(godis, id)

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: godis.Namespace,
			Labels:    podLabels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(godis, godisapis.SchemeGroupVersion.WithKind("Godis")),
			},
		},
		Spec: corev1.PodSpec{
			Hostname:  podName,
			Subdomain: godis.Name,
			Containers: []corev1.Container{
				{
					Name:  "godis",
					Image: "kbzjung359/godis",
					Args:  []string{"cluster"},
					Env: []corev1.EnvVar{
						{
							Name:  "CLUSTER_ID",
							Value: fmt.Sprintf("%d", id),
						},
						{
							Name:  "CLUSTER_INITIAL-CLUSTER",
							Value: initialCluster,
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
							Value: strconv.FormatBool(join),
						},
					},
				},
			},
		},
	}
}

func godisPodName(godis *godisapis.Godis, id int) string {
	return fmt.Sprintf("%s-%d", godis.Name, id)
}

func newService(pod *corev1.Pod) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pod.Name + "-endpoint",
			Namespace:       pod.Namespace,
			OwnerReferences: pod.OwnerReferences,
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
			Selector: pod.Labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
}

func godisServiceName(godis *godisapis.Godis, id int) string {
	return fmt.Sprintf("%s-%d-endpoint", godis.Name, id)
}

func godisFQDN(godis *godisapis.Godis, id int) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", godisServiceName(godis, id), godis.Namespace)
}

func clusterPeers(godis *godisapis.Godis, ids []int) string {
	peerURL := func(id int, podFQDN string) string {
		return fmt.Sprintf("%d@http://%s:6300", id, podFQDN)
	}
	peers := make([]string, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, peerURL(id, godisFQDN(godis, id)))
	}
	return strings.Join(peers, ",")
}

func clusterPeersForJoin(godis *godisapis.Godis, nid int) string {
	members := strings.Split(godis.Status.Members, ",")
	ids := make([]int, 0, len(members)+1)
	for _, member := range members {
		mid, _ := strconv.Atoi(member)
		ids = append(ids, mid)
	}
	ids = append(ids, nid)
	return clusterPeers(godis, ids)
}
