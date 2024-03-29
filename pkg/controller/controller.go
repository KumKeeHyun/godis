package controller

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
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

	clusterLister godislisters.GodisClusterLister
	clusterSynced cache.InformerSynced
	godisLister   godislisters.GodisLister
	godisSynced   cache.InformerSynced

	clusterQueue workqueue.RateLimitingInterface
	godisQueue   workqueue.RateLimitingInterface
	recorder     record.EventRecorder

	clusterClient GodisClusterClient
}

func New(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	godisClient godisclientset.Interface,
	clusterInformer godisinformers.GodisClusterInformer,
	godisInformer godisinformers.GodisInformer,
	clusterClient GodisClusterClient) *Controller {
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

		clusterLister: clusterInformer.Lister(),
		clusterSynced: clusterInformer.Informer().HasSynced,
		godisLister:   godisInformer.Lister(),
		godisSynced:   godisInformer.Informer().HasSynced,

		clusterQueue: workqueue.NewRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(time.Second, 1000*time.Second)),
		godisQueue:   workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		recorder:     recorder,

		clusterClient: clusterClient,
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
			DeleteFunc: func(obj interface{}) {
				controller.enqueueGodisEvent(obj)
				controller.handleGodisObject(obj)
			},
		},
	)

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

func (c *Controller) handleGodisObject(obj interface{}) {
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
		logger.V(4).Info("Recovered deleted object", "resourceName", object.GetName())
	}

	logger.V(4).Info("Processing object", "object", klog.KObj(object))
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Foo, we should not do anything more
		// with it.
		if ownerRef.Kind != "GodisCluster" {
			return
		}

		cluster, err := c.clusterLister.GodisClusters(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			logger.V(4).Info("Ignore orphaned object", "object", klog.KObj(object), "GodisCluster", ownerRef.Name)
			return
		}

		c.enqueueClusterEvent(cluster)
		return
	}
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

	if ok := cache.WaitForCacheSync(ctx.Done(), c.clusterSynced, c.godisSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Godis resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runClusterWorker, time.Second)
		go wait.UntilWithContext(ctx, c.runGodisWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}
