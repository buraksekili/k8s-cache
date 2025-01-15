package main

import (
	"errors"
	"fmt"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	supportedKinds = []client.Object{}
	ErrNilObj      = errors.New("object is nil")
	ErrGvkNotFound = errors.New("gvk not found in the cache")
)

type cacheStore map[schema.GroupVersionKind]cache.Indexer

type CacheStores struct {
	storesByGvk cacheStore
	scheme      *runtime.Scheme
}

func New(scheme *runtime.Scheme) (CacheStores, error) {
	stores := make(map[schema.GroupVersionKind]cache.Indexer)

	for i := range supportedKinds {
		gvk, err := gvkFromObject(supportedKinds[i], scheme)
		if err != nil {
			return CacheStores{}, err
		}

		registerGvkIntoCache(*gvk, stores)
	}

	return CacheStores{
		storesByGvk: stores,
		scheme:      scheme,
	}, nil
}

// List only works with structured types; not working with partial objects or unstructured data
func (s *CacheStores) List(out client.ObjectList, opts ...client.ListOption) error {
	if out == nil {
		return ErrNilObj
	}

	gvk, err := gvkFromObject(out, s.scheme)
	if err != nil {
		return err
	}

	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	store := s.storesByGvk[*gvk]
	if store == nil {
		return ErrGvkNotFound
	}

	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	var objs []interface{}

	switch {
	case listOpts.FieldSelector != nil:
		requiresExact := requiresExactMatch(listOpts.FieldSelector)
		if !requiresExact {
			return fmt.Errorf("non-exact field matches are not supported by the cache")
		}
		// list all objects by the field selector. If this is namespaced and we have one, ask for the
		// namespaced index key. Otherwise, ask for the non-namespaced variant by using the fake "all namespaces"
		// namespace.
		objs, err = byIndexes(store, listOpts.FieldSelector.Requirements(), listOpts.Namespace)
	case listOpts.Namespace != "":
		objs, err = store.ByIndex(cache.NamespaceIndex, listOpts.Namespace)
	default:
		objs = store.List()
	}
	if err != nil {
		return err
	}

	var labelSel labels.Selector
	if listOpts.LabelSelector != nil {
		labelSel = listOpts.LabelSelector
	}

	limitSet := listOpts.Limit > 0

	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, item := range objs {
		// if the Limit option is set and the number of items
		// listed exceeds this limit, then stop reading.
		if limitSet && int64(len(runtimeObjs)) >= listOpts.Limit {
			break
		}
		obj, isObj := item.(runtime.Object)
		if !isObj {
			return fmt.Errorf("cache contained %T, which is not an Object", item)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return err
		}
		if labelSel != nil {
			lbls := labels.Set(meta.GetLabels())
			if !labelSel.Matches(lbls) {
				continue
			}
		}

		var outObj runtime.Object
		outObj = obj.DeepCopyObject()
		outObj.GetObjectKind().SetGroupVersionKind(*gvk)
		runtimeObjs = append(runtimeObjs, outObj)
	}

	return apimeta.SetList(out, runtimeObjs)
}

func (s *CacheStores) Get(obj client.Object) (item interface{}, exists bool, err error) {
	if obj == nil {
		return nil, false, fmt.Errorf("cannot add nil object")
	}

	gvk, err := gvkFromObject(obj, s.scheme)
	if err != nil {
		return nil, false, err
	}

	store := s.storesByGvk[*gvk]
	if store == nil {
		return nil, false, nil
	}

	return store.GetByKey(client.ObjectKeyFromObject(obj).String())
}

func (s *CacheStores) Delete(obj client.Object) error {
	if obj == nil {
		return fmt.Errorf("cannot delete nil object")
	}

	gvk, err := gvkFromObject(obj, s.scheme)
	if err != nil {
		return err
	}

	store := s.storesByGvk[*gvk]
	if store == nil {
		return nil
	}

	return store.Delete(obj)
}

func (s *CacheStores) Add(obj client.Object) error {
	if obj == nil {
		return fmt.Errorf("cannot add nil object")
	}

	gvk, err := gvkFromObject(obj, s.scheme)
	if err != nil {
		return err
	}

	store := s.storesByGvk[*gvk]
	if store == nil {
		store = registerGvkIntoCache(*gvk, s.storesByGvk)
	}
	//obj.GetObjectKind().SetGroupVersionKind(*gvk)

	return store.Add(obj)
}

func (s *CacheStores) GetByType(t schema.GroupVersionKind) cache.Indexer {
	return s.storesByGvk[t]
}

func (s *CacheStores) IndexField(obj client.Object, field string, extractValue client.IndexerFunc) error {
	if obj == nil {
		return nil
	}

	gvk, err := gvkFromObject(obj, s.scheme)
	if err != nil {
		return nil
	}

	store := s.storesByGvk[*gvk]
	if store == nil {
		return nil
	}

	return indexByField(store, field, extractValue)
}

// allNamespacesNamespace is used as the "namespace" when we want to list across all namespaces.
const allNamespacesNamespace = "__all"

// keyToNamespacedKey prefixes the given index key with a namespace
// for use in field selector indexes.
func keyToNamespacedKey(ns string, baseKey string) string {
	if ns != "" {
		return ns + "/" + baseKey
	}
	return allNamespacesNamespace + "/" + baseKey
}

func indexByField(store cache.Indexer, field string, extractValue client.IndexerFunc) error {
	indexFunc := func(objRaw interface{}) ([]string, error) {
		obj, isObj := objRaw.(client.Object)
		if !isObj {
			return nil, fmt.Errorf("object of type %T is not an Object", objRaw)
		}
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return nil, err
		}

		ns := meta.GetNamespace()

		rawVals := extractValue(obj)
		var vals []string
		if ns == "" {
			vals = make([]string, len(rawVals))
		} else {
			vals = make([]string, len(rawVals)*2)
		}

		for i, rawVal := range rawVals {
			vals[i] = keyToNamespacedKey(ns, rawVal)
			if ns != "" {
				vals[i+len(rawVals)] = keyToNamespacedKey("", rawVal)
			}
		}

		return vals, nil
	}

	return store.AddIndexers(
		cache.Indexers{
			fieldIdxName(field): indexFunc,
		},
	)
}

func fieldIdxName(field string) string {
	return "tyk_f:" + field
}

func registerGvkIntoCache(gvk schema.GroupVersionKind, c cacheStore) cache.Indexer {
	newCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	c[gvk] = newCache
	return newCache
}

// requiresExactMatch checks if the given field selector is of the form `k=v` or `k==v`.
func requiresExactMatch(sel fields.Selector) bool {
	reqs := sel.Requirements()
	if len(reqs) == 0 {
		return false
	}

	for _, req := range reqs {
		if req.Operator != selection.Equals && req.Operator != selection.DoubleEquals {
			return false
		}
	}
	return true
}

func byIndexes(indexer cache.Indexer, requires fields.Requirements, namespace string) ([]interface{}, error) {
	var (
		err  error
		objs []interface{}
		vals []string
	)
	indexers := indexer.GetIndexers()
	for idx, req := range requires {
		indexName := fieldIdxName(req.Field)
		indexedValue := keyToNamespacedKey(namespace, req.Value)
		if idx == 0 {
			// we use first require to get snapshot data
			// TODO(halfcrazy): use complicated index when client-go provides byIndexes
			// https://github.com/kubernetes/kubernetes/issues/109329
			objs, err = indexer.ByIndex(indexName, indexedValue)
			if err != nil {
				return nil, err
			}
			if len(objs) == 0 {
				return nil, nil
			}
			continue
		}
		fn, exist := indexers[indexName]
		if !exist {
			return nil, fmt.Errorf("index with name %s does not exist", indexName)
		}
		filteredObjects := make([]interface{}, 0, len(objs))
		for _, obj := range objs {
			vals, err = fn(obj)
			if err != nil {
				return nil, err
			}
			for _, val := range vals {
				if val == indexedValue {
					filteredObjects = append(filteredObjects, obj)
					break
				}
			}
		}
		if len(filteredObjects) == 0 {
			return nil, nil
		}
		objs = filteredObjects
	}

	return objs, nil
}

func gvkFromObject(obj runtime.Object, scheme *runtime.Scheme) (*schema.GroupVersionKind, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot get nil object")
	}
	if scheme == nil {
		return nil, fmt.Errorf("cannot get scheme nil object")
	}

	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return nil, err
	}

	return &gvk, nil
}
