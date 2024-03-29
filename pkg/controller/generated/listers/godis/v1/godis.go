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

// Code generated by lister-gen. DO NOT EDIT.

package v1

import (
	v1 "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// GodisLister helps list Godises.
// All objects returned here must be treated as read-only.
type GodisLister interface {
	// List lists all Godises in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.Godis, err error)
	// Godises returns an object that can list and get Godises.
	Godises(namespace string) GodisNamespaceLister
	GodisListerExpansion
}

// godisLister implements the GodisLister interface.
type godisLister struct {
	indexer cache.Indexer
}

// NewGodisLister returns a new GodisLister.
func NewGodisLister(indexer cache.Indexer) GodisLister {
	return &godisLister{indexer: indexer}
}

// List lists all Godises in the indexer.
func (s *godisLister) List(selector labels.Selector) (ret []*v1.Godis, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Godis))
	})
	return ret, err
}

// Godises returns an object that can list and get Godises.
func (s *godisLister) Godises(namespace string) GodisNamespaceLister {
	return godisNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// GodisNamespaceLister helps list and get Godises.
// All objects returned here must be treated as read-only.
type GodisNamespaceLister interface {
	// List lists all Godises in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.Godis, err error)
	// Get retrieves the Godis from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1.Godis, error)
	GodisNamespaceListerExpansion
}

// godisNamespaceLister implements the GodisNamespaceLister
// interface.
type godisNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all Godises in the indexer for a given namespace.
func (s godisNamespaceLister) List(selector labels.Selector) (ret []*v1.Godis, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Godis))
	})
	return ret, err
}

// Get retrieves the Godis from the indexer for a given namespace and name.
func (s godisNamespaceLister) Get(name string) (*v1.Godis, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1.Resource("godis"), name)
	}
	return obj.(*v1.Godis), nil
}
