package controller

import (
	corev1 "k8s.io/api/core/v1"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stest "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"

	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
)

func newCluster(name string, replicas *int32) *godisapis.GodisCluster {
	return &godisapis.GodisCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: godisapis.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: godisapis.GodisClusterSpec{
			Replicas: replicas,
		},
	}
}

func initializedCluster(cluster *godisapis.GodisCluster) *godisapis.GodisCluster {
	cluster.Status.Status = "Running"
	cluster.Status.Replicas = *cluster.Spec.Replicas
	cluster.Status.NextID = int(cluster.Status.Replicas) + 1
	return cluster
}

func (f *fixture) expectCreateConfigMapsForInit(cluster *godisapis.GodisCluster, replicas int) {
	config := newConfigMapForInit(cluster, replicas)
	f.expectedKubeActions = append(f.expectedKubeActions, k8stest.NewCreateAction(
		schema.GroupVersionResource{Resource: "configmaps"},
		config.Namespace,
		config,
	))
}

func (f *fixture) expectCreateGodis(cluster *godisapis.GodisCluster, id int, initial bool) {
	godis := newGodis(cluster, id, initial)
	f.expectedGodisActions = append(f.expectedGodisActions, k8stest.NewCreateAction(
		schema.GroupVersionResource{Resource: "godises"},
		godis.Namespace,
		godis,
	))
}

func (f *fixture) expectUpdateConfigMaps(config *corev1.ConfigMap) {
	f.expectedKubeActions = append(f.expectedKubeActions, k8stest.NewUpdateAction(
		schema.GroupVersionResource{Resource: "configmaps"},
		config.Namespace,
		config,
	))
}

func (f *fixture) expectUpdateStatusClusterForStartInit(cluster *godisapis.GodisCluster) *godisapis.GodisCluster {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Initializing"
	initialReplicas := int(*cluster.Spec.Replicas)
	clusterCopy.Status.InitialReplicas = &initialReplicas
	clusterCopy.Status.NextID = initialReplicas + 1

	f.expectedGodisActions = append(f.expectedGodisActions, k8stest.NewUpdateSubresourceAction(
		schema.GroupVersionResource{Resource: "godisclusters"},
		"status",
		clusterCopy.Namespace,
		clusterCopy,
	))
	return clusterCopy
}

func (f *fixture) expectUpdateStatusClusterForEndInit(cluster *godisapis.GodisCluster) {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = int32(*cluster.Status.InitialReplicas)
	clusterCopy.Status.InitialReplicas = nil

	f.expectedGodisActions = append(f.expectedGodisActions, k8stest.NewUpdateSubresourceAction(
		schema.GroupVersionResource{Resource: "godisclusters"},
		"status",
		clusterCopy.Namespace,
		clusterCopy,
	))
}

func (f *fixture) expectUpdateStatusClusterForStartScaleOut(cluster *godisapis.GodisCluster, newID int) *godisapis.GodisCluster {
	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Scaling"
	clusterCopy.Status.ScaleOutID = &newID
	clusterCopy.Status.NextID = newID + 1

	f.expectedGodisActions = append(f.expectedGodisActions, k8stest.NewUpdateSubresourceAction(
		schema.GroupVersionResource{Resource: "godisclusters"},
		"status",
		clusterCopy.Namespace,
		clusterCopy,
	))
	return clusterCopy
}

func (f *fixture) expectUpdateStatusClusterForEndScaleOut(cluster *godisapis.GodisCluster) {

	clusterCopy := cluster.DeepCopy()
	clusterCopy.Status.Status = "Running"
	clusterCopy.Status.Replicas = clusterCopy.Status.Replicas + 1
	clusterCopy.Status.ScaleOutID = nil

	f.expectedGodisActions = append(f.expectedGodisActions, k8stest.NewUpdateSubresourceAction(
		schema.GroupVersionResource{Resource: "godisclusters"},
		"status",
		clusterCopy.Namespace,
		clusterCopy,
	))
}

func TestClusterInitialize(t *testing.T) {
	f := newFixture(t)
	_, ctx := ktesting.NewTestContext(t)

	cluster := newCluster("test", int32Ptr(3))
	f.godisObjs = append(f.godisObjs, cluster)

	cluster = f.expectUpdateStatusClusterForStartInit(cluster)
	f.expectCreateConfigMapsForInit(cluster, 3)
	f.expectCreateGodis(cluster, 1, true)
	f.expectCreateGodis(cluster, 2, true)
	f.expectCreateGodis(cluster, 3, true)
	f.expectUpdateStatusClusterForEndInit(cluster)

	f.runCluster(ctx, getKey(cluster, t))
}

func TestClusterInitializeRestartingAtUpdateStatus(t *testing.T) {
	f := newFixture(t)
	_, ctx := ktesting.NewTestContext(t)

	cluster := newCluster("test", int32Ptr(3))
	cluster.Status.Status = "Initializing"
	initialReplicas := int(*cluster.Spec.Replicas)
	cluster.Status.InitialReplicas = &initialReplicas
	// update status -> crash -> edited spec
	cluster.Spec.Replicas = int32Ptr(4)
	f.godisObjs = append(f.godisObjs, cluster)

	f.expectCreateConfigMapsForInit(cluster, 3)
	f.expectCreateGodis(cluster, 1, true)
	f.expectCreateGodis(cluster, 2, true)
	f.expectCreateGodis(cluster, 3, true)
	f.expectUpdateStatusClusterForEndInit(cluster)

	f.runCluster(ctx, getKey(cluster, t))
}

func TestClusterScaleOut(t *testing.T) {
	f := newFixture(t)
	_, ctx := ktesting.NewTestContext(t)

	cluster := initializedCluster(newCluster("test", int32Ptr(3)))
	cluster.Spec.Replicas = int32Ptr(4)
	f.godisObjs = append(f.godisObjs, cluster)

	config := newConfigMapForInit(cluster, 3)
	f.kubeObjs = append(f.kubeObjs, config)

	for i := 1; i <= 3; i++ {
		godis := newGodis(cluster, i, true)
		f.godisObjs = append(f.godisObjs, godis)
	}

	cluster = f.expectUpdateStatusClusterForStartScaleOut(cluster, 4)
	configCopy := config.DeepCopy()
	configCopy.Data["initial-cluster"] = clusterPeers(cluster, []int{1, 2, 3, 4})
	f.expectUpdateConfigMaps(configCopy)
	f.expectCreateGodis(cluster, 4, false)
	f.expectUpdateStatusClusterForEndScaleOut(cluster)

	f.runCluster(ctx, getKey(cluster, t))
}
