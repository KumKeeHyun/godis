/*
Copyright The Kubernetes Authors.

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

// Code generated by informer-gen. DO NOT EDIT.

package v1

import (
	"context"
	time "time"

	godisv1 "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	versioned "github.com/KumKeeHyun/godis/pkg/controller/generated/clientset/versioned"
	internalinterfaces "github.com/KumKeeHyun/godis/pkg/controller/generated/informers/externalversions/internalinterfaces"
	v1 "github.com/KumKeeHyun/godis/pkg/controller/generated/listers/godis/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// GodisInformer provides access to a shared informer and lister for
// Godises.
type GodisInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.GodisLister
}

type godisInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewGodisInformer constructs a new informer for Godis type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewGodisInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredGodisInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredGodisInformer constructs a new informer for Godis type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredGodisInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KumkeehyunV1().Godises(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KumkeehyunV1().Godises(namespace).Watch(context.TODO(), options)
			},
		},
		&godisv1.Godis{},
		resyncPeriod,
		indexers,
	)
}

func (f *godisInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredGodisInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *godisInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&godisv1.Godis{}, f.defaultInformer)
}

func (f *godisInformer) Lister() v1.GodisLister {
	return v1.NewGodisLister(f.Informer().GetIndexer())
}