package controller

import (
	"context"
	"k8s.io/client-go/tools/cache"
	"reflect"
	"testing"
	"time"

	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	"github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned/fake"
	godisinformers "github.com/KumKeeHyun/godis/pkg/controller/generated/informers/externalversions"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stest "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

var (
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }
)

type mockGodisClusterClient struct{}

func (c *mockGodisClusterClient) Meet(godises []*godisapis.Godis, newID int) error {
	return nil
}

func (c *mockGodisClusterClient) Forget(godises []*godisapis.Godis, deletedID int) error {
	return nil
}

type fixture struct {
	t *testing.T

	kubeClient  *k8sfake.Clientset
	godisClient *fake.Clientset

	expectedKubeActions  []k8stest.Action
	expectedGodisActions []k8stest.Action

	kubeObjs  []runtime.Object
	godisObjs []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{
		t:         t,
		kubeObjs:  []runtime.Object{},
		godisObjs: []runtime.Object{},
	}
	return f
}

func (f *fixture) newController(ctx context.Context) *Controller {
	f.kubeClient = k8sfake.NewSimpleClientset(f.kubeObjs...)
	f.godisClient = fake.NewSimpleClientset(f.godisObjs...)

	godisInformer := godisinformers.NewSharedInformerFactory(f.godisClient, noResyncPeriodFunc())
	for _, obj := range f.godisObjs {
		if _, ok := obj.(*godisapis.GodisCluster); ok {
			godisInformer.Kumkeehyun().V1().GodisClusters().Informer().GetIndexer().Add(obj)
		} else if _, ok = obj.(*godisapis.Godis); ok {
			godisInformer.Kumkeehyun().V1().Godises().Informer().GetIndexer().Add(obj)
		}
	}
	godisInformer.Start(ctx.Done())

	c := New(ctx, f.kubeClient, f.godisClient,
		godisInformer.Kumkeehyun().V1().GodisClusters(),
		godisInformer.Kumkeehyun().V1().Godises(),
		&mockGodisClusterClient{})

	c.clusterSynced = alwaysReady
	c.godisSynced = alwaysReady
	c.recorder = &record.FakeRecorder{}

	return c
}

func (f *fixture) runCluster(ctx context.Context, clusterKey string) {
	f.runControllerCluster(ctx, clusterKey, false)
}

func (f *fixture) runClusterExpectErr(ctx context.Context, clusterKey string) {
	f.runControllerCluster(ctx, clusterKey, true)
}

func (f *fixture) runControllerCluster(ctx context.Context, clusterKey string, expectErr bool) {
	c := f.newController(ctx)

	err := c.syncCluster(ctx, clusterKey)
	if !expectErr && err != nil {
		f.t.Errorf("error syncing cluster: %v", err)
	} else if expectErr && err == nil {
		f.t.Error("expected error syncing cluster, got nil")
	}

	kubeActions := filterInformerActions(f.kubeClient.Actions())
	if len(kubeActions) != len(f.expectedKubeActions) {
		f.t.Errorf("expected kubeActions len %d, got %d", len(f.expectedKubeActions), len(kubeActions))
	}
	for i, kubeAction := range kubeActions {
		expectedAction := f.expectedKubeActions[i]
		checkAction(expectedAction, kubeAction, f.t)
	}

	godisActions := filterInformerActions(f.godisClient.Actions())
	if len(godisActions) != len(f.expectedGodisActions) {
		f.t.Errorf("expected kubeActions len %d, got %d", len(f.expectedKubeActions), len(kubeActions))
	}
	for i, godisAction := range godisActions {
		expectedAction := f.expectedGodisActions[i]
		checkAction(expectedAction, godisAction, f.t)
	}

}

// checkAction verifies that expected and actual actions are equal and both have
// same attached resources
func checkAction(expected, actual k8stest.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) && actual.GetSubresource() == expected.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	switch a := actual.(type) {
	case k8stest.CreateActionImpl:
		e, _ := expected.(k8stest.CreateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case k8stest.UpdateActionImpl:
		e, _ := expected.(k8stest.UpdateActionImpl)
		expObject := e.GetObject()
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expObject, object))
		}
	case k8stest.PatchActionImpl:
		e, _ := expected.(k8stest.PatchActionImpl)
		expPatch := e.GetPatch()
		patch := a.GetPatch()

		if !reflect.DeepEqual(expPatch, patch) {
			t.Errorf("Action %s %s has wrong patch\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintSideBySide(expPatch, patch))
		}
	default:
		t.Errorf("Uncaptured Action %s %s, you should explicitly add a case to capture it",
			actual.GetVerb(), actual.GetResource().Resource)
	}
}

// filterInformerActions filters list and watch actions for testing resources.
// Since list and watch don't change resource state we can filter it to lower
// nose level in our tests.
func filterInformerActions(actions []k8stest.Action) []k8stest.Action {
	ret := []k8stest.Action{}
	for _, action := range actions {
		if len(action.GetNamespace()) == 0 ||
			(action.Matches("get", "configmaps") ||
				action.Matches("get", "godises") ||
				action.Matches("list", "godises") ||
				action.Matches("get", "godises")) {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

func getKey(obj interface{}, t *testing.T) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		t.Errorf("Unexpected error getting key for obj %v: %v", obj, err)
		return ""
	}
	return key
}

func int32Ptr(i int32) *int32 {
	return &i
}
