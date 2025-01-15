package main

import (
	"log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	util "k8s.io/apimachinery/pkg/util/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	supportedKinds = []client.Object{
		&appsv1.Deployment{},
	}

	scheme := runtime.NewScheme()
	util.Must(appsv1.AddToScheme(scheme))

	cacheStores, err := New(scheme)
	if err != nil {
		log.Fatalf("failed to instanciate gvk cache, err: %v", err)
	}

	err = setupCacheIndexes(cacheStores)
	if err != nil {
		log.Fatalf("failed to setup cache indexes, err: %v", err)
	}

	deploy := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "deploy",
			Namespace: "default",
			Annotations: map[string]string{
				dummyAnnotation: dummyAnnotationVal,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "somename",
				},
			},
		},
	}

	err = cacheStores.Add(&deploy)
	if err != nil {
		log.Fatalf("failed to add deployment to the cache, err: %v", err)
	}

	deployFromCache, exists, err := cacheStores.Get(&deploy)
	if err != nil {
		log.Fatalf("failed to get the deployment, err: %v", err)
	}
	if !exists {
		log.Fatal("failed to find the deployment in the cache")
	}

	cachedDeploy, ok := deployFromCache.(*appsv1.Deployment)
	if !ok || cachedDeploy == nil {
		log.Fatal("the cached deployment is malformed")
	}

	result := equality.Semantic.DeepEqual(*cachedDeploy, deploy)
	log.Println("these two structs are equal? => ", result)

	deploys := appsv1.DeploymentList{}
	err = cacheStores.List(&deploys, client.MatchingFields{
		customIdx: dummyAnnotationVal,
	})
	if err != nil {
		log.Fatalf("failed to list deployments, err: %v", err)
	}

	log.Printf("found %d deployments using custom index\n", len(deploys.Items))
}

const (
	customIdx          = "my_custom_index"
	dummyAnnotation    = "my.domain/label"
	dummyAnnotationVal = "someval"
)

func setupCacheIndexes(stores CacheStores) error {
	err := stores.IndexField(
		&appsv1.Deployment{},
		customIdx,
		func(o client.Object) []string {
			api, ok := o.(*appsv1.Deployment)
			if !ok {
				return nil
			}

			// parse dummy idx val
			expectedVal := api.GetAnnotations()[dummyAnnotation]
			if expectedVal == "" {
				return nil
			}

			return []string{expectedVal}
		},
	)
	if err != nil {
		return err
	}

	return nil
}
