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

package v1alpha1

import (
	v1alpha1 "github.com/kongyi-ibm/k8s-deployment-operator/pkg/apis/deploycontrol/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// DeployDaemonLister helps list DeployDaemons.
type DeployDaemonLister interface {
	// List lists all DeployDaemons in the indexer.
	List(selector labels.Selector) (ret []*v1alpha1.DeployDaemon, err error)
	// DeployDaemons returns an object that can list and get DeployDaemons.
	DeployDaemons(namespace string) DeployDaemonNamespaceLister
	DeployDaemonListerExpansion
}

// deployDaemonLister implements the DeployDaemonLister interface.
type deployDaemonLister struct {
	indexer cache.Indexer
}

// NewDeployDaemonLister returns a new DeployDaemonLister.
func NewDeployDaemonLister(indexer cache.Indexer) DeployDaemonLister {
	return &deployDaemonLister{indexer: indexer}
}

// List lists all DeployDaemons in the indexer.
func (s *deployDaemonLister) List(selector labels.Selector) (ret []*v1alpha1.DeployDaemon, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.DeployDaemon))
	})
	return ret, err
}

// DeployDaemons returns an object that can list and get DeployDaemons.
func (s *deployDaemonLister) DeployDaemons(namespace string) DeployDaemonNamespaceLister {
	return deployDaemonNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// DeployDaemonNamespaceLister helps list and get DeployDaemons.
type DeployDaemonNamespaceLister interface {
	// List lists all DeployDaemons in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*v1alpha1.DeployDaemon, err error)
	// Get retrieves the DeployDaemon from the indexer for a given namespace and name.
	Get(name string) (*v1alpha1.DeployDaemon, error)
	DeployDaemonNamespaceListerExpansion
}

// deployDaemonNamespaceLister implements the DeployDaemonNamespaceLister
// interface.
type deployDaemonNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all DeployDaemons in the indexer for a given namespace.
func (s deployDaemonNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.DeployDaemon, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.DeployDaemon))
	})
	return ret, err
}

// Get retrieves the DeployDaemon from the indexer for a given namespace and name.
func (s deployDaemonNamespaceLister) Get(name string) (*v1alpha1.DeployDaemon, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("deploydaemon"), name)
	}
	return obj.(*v1alpha1.DeployDaemon), nil
}
