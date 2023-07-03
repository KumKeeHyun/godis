package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
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

	godisclientset "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned"
	godisscheme "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned/scheme"
	godisinformers "github.com/KumKeeHyun/godis/pkg/controller/generated/informers/externalversions/godis/v1"
	godislisters "github.com/KumKeeHyun/godis/pkg/controller/generated/listers/godis/v1"
)

const controllerAgentName = "godis-controller"

type Controller struct {
	kubeClient  kubernetes.Interface
	godisClient godisclientset.Interface

	podsLister    corelisters.PodLister
	podsSynced    cache.InformerSynced
	clusterLister godislisters.GodisClusterLister
	clusterSynced cache.InformerSynced
	godisLister   godislisters.GodisLister
	godisSynced   cache.InformerSynced

	clusterQueue workqueue.RateLimitingInterface
	godisQueue   workqueue.RateLimitingInterface
	recorder     record.EventRecorder
}

func New(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	godisClient godisclientset.Interface,
	podInformer coreinformers.PodInformer,
	clusterInformer godisinformers.GodisClusterInformer,
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

		podsLister:    podInformer.Lister(),
		podsSynced:    podInformer.Informer().HasSynced,
		clusterLister: clusterInformer.Lister(),
		clusterSynced: clusterInformer.Informer().HasSynced,
		godisLister:   godisInformer.Lister(),
		godisSynced:   godisInformer.Informer().HasSynced,

		clusterQueue: workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		godisQueue:   workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		recorder:     recorder,
	}

	logger.Info("Setting up event handlers")
	clusterInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: controller.enqueueClusterEvent,
			UpdateFunc: func(oldObj, newObj interface{}) {
				controller.enqueueClusterEvent(newObj)
			},
			DeleteFunc: controller.enqueueClusterEvent,
		},
	)
	godisInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: controller.enqueueGodisEvent,
			UpdateFunc: func(oldObj, newObj interface{}) {
				controller.enqueueGodisEvent(newObj)
			},
			DeleteFunc: controller.enqueueGodisEvent,
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

func (c *Controller) enqueueClusterEvent(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.clusterQueue.Add(key)
}

func (c *Controller) enqueueGodisEvent(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.godisQueue.Add(key)
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shut down the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.clusterQueue.ShutDown()
	defer c.godisQueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting godis controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.podsSynced, c.clusterSynced, c.godisSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Godis resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runClusterWorker, time.Second)
		//go wait.UntilWithContext(ctx, c.runGodisWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

//
//func newPod(godis *godisapis.Godis, id int, initialCluster string, join bool) *corev1.Pod {
//	podLabels := map[string]string{
//		"cluster-name": godis.Name,
//		"id":           strconv.Itoa(id),
//	}
//	podName := godisPodName(godis, id)
//
//	return &corev1.Pod{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      podName,
//			Namespace: godis.Namespace,
//			Labels:    podLabels,
//			OwnerReferences: []metav1.OwnerReference{
//				*metav1.NewControllerRef(godis, godisapis.SchemeGroupVersion.WithKind("Godis")),
//			},
//		},
//		Spec: corev1.PodSpec{
//			Hostname:  podName,
//			Subdomain: godis.Name,
//			Containers: []corev1.Container{
//				{
//					Name:  "godis",
//					Image: "kbzjung359/godis",
//					Args:  []string{"cluster"},
//					Env: []corev1.EnvVar{
//						{
//							Name:  "CLUSTER_ID",
//							Value: fmt.Sprintf("%d", id),
//						},
//						{
//							Name:  "CLUSTER_INITIAL-CLUSTER",
//							Value: initialCluster,
//						},
//						{
//							Name:  "CLUSTER_WALDIR",
//							Value: "/tmp/godis/wal",
//						},
//						{
//							Name:  "CLUSTER_SNAPDIR",
//							Value: "/tmp/godis/snap",
//						},
//						{
//							Name:  "CLUSTER_JOIN",
//							Value: strconv.FormatBool(join),
//						},
//					},
//				},
//			},
//		},
//	}
//}
//
//func godisPodName(godis *godisapis.Godis, id int) string {
//	return fmt.Sprintf("%s-%d", godis.Name, id)
//}
//
//func newService(pod *corev1.Pod) *corev1.Service {
//	return &corev1.Service{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:            pod.Name + "-endpoint",
//			Namespace:       pod.Namespace,
//			OwnerReferences: pod.OwnerReferences,
//		},
//		Spec: corev1.ServiceSpec{
//			Ports: []corev1.ServicePort{
//				{
//					Name: "client",
//					Port: 6379,
//					TargetPort: intstr.IntOrString{
//						Type:   intstr.Int,
//						IntVal: 6379,
//					},
//				},
//				{
//					Name: "peer",
//					Port: 6300,
//					TargetPort: intstr.IntOrString{
//						Type:   intstr.Int,
//						IntVal: 6300,
//					},
//				},
//			},
//			Selector: pod.Labels,
//			Type:     corev1.ServiceTypeClusterIP,
//		},
//	}
//}
//
