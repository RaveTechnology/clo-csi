package registry

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	kt "k8s.io/client-go/testing"
	"strings"
	"sync"
	"testing"
)

// The fake API explicitly implements status-subresource semantics and CAS.
func kubeFixture(failStatus bool) *fake.FakeDynamicClient {
	c := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{Resource(KindVolume): "CloVolumeList", Resource(KindIssuer): "CloControllerEpochList", Resource(KindNode): "CloNodeBindingList", Resource(KindOperation): "CloOperationList", Resource(KindFence): "CloFenceList"})
	var mu sync.Mutex
	seq := 0
	objects := map[string]*unstructured.Unstructured{}
	c.PrependReactor("*", "*", func(a kt.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		key := a.GetResource().Resource
		switch a.GetVerb() {
		case "create":
			obj := a.(kt.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
			key += "/" + obj.GetName()
			if objects[key] != nil {
				return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: a.GetResource().Resource}, obj.GetName())
			}
			seq++
			obj.SetUID(types.UID(fmt.Sprint(seq)))
			obj.SetResourceVersion(fmt.Sprint(seq))
			delete(obj.Object, "status")
			objects[key] = obj
			return true, obj.DeepCopy(), nil
		case "list":
			list := &unstructured.UnstructuredList{}
			list.SetAPIVersion(Group + "/v1alpha1")
			selector := a.(kt.ListAction).GetListRestrictions().Labels
			for k, obj := range objects {
				if strings.HasPrefix(k, key+"/") && selector.Matches(labels.Set(obj.GetLabels())) {
					list.Items = append(list.Items, *obj.DeepCopy())
				}
			}
			return true, list, nil
		case "patch":
			pa := a.(kt.PatchAction)
			key += "/" + pa.GetName()
			old := objects[key]
			if old == nil {
				return true, nil, fmt.Errorf("missing")
			}
			patch, e := jsonpatch.DecodePatch(pa.GetPatch())
			if e != nil {
				return true, nil, e
			}
			raw, _ := json.Marshal(old.Object)
			updated, e := patch.Apply(raw)
			if e != nil {
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: a.GetResource().Resource}, old.GetName(), e)
			}
			obj := &unstructured.Unstructured{}
			if e = obj.UnmarshalJSON(updated); e != nil {
				return true, nil, e
			}
			seq++
			obj.SetResourceVersion(fmt.Sprint(seq))
			objects[key] = obj
			return true, obj.DeepCopy(), nil
		case "get":
			key += "/" + a.(kt.GetAction).GetName()
			obj := objects[key]
			if obj == nil {
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: a.GetResource().Resource}, key)
			}
			return true, obj.DeepCopy(), nil
		case "update":
			obj := a.(kt.UpdateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
			key += "/" + obj.GetName()
			old := objects[key]
			if old == nil {
				return true, nil, fmt.Errorf("missing")
			}
			if failStatus {
				return true, nil, fmt.Errorf("lost status response")
			}
			if a.GetSubresource() != "status" {
				return true, nil, fmt.Errorf("must use status CAS")
			}
			if obj.GetResourceVersion() != old.GetResourceVersion() || obj.GetUID() != old.GetUID() {
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: a.GetResource().Resource}, obj.GetName(), fmt.Errorf("stale"))
			}
			seq++
			old = old.DeepCopy()
			old.Object["status"] = obj.Object["status"]
			old.SetResourceVersion(fmt.Sprint(seq))
			objects[key] = old
			return true, old.DeepCopy(), nil
		}
		return false, nil, nil
	})
	return c
}

func TestKubeStatusInitializationFailureCannotAuthorizeMutation(t *testing.T) {
	ctx := context.Background()
	c := kubeFixture(true)
	r := New(NewKubernetes(c, "clo-system"), Installation{installID, projectID})
	if _, e := r.CreateVolume(ctx, volumeSpec("pvc-a")); e == nil {
		t.Fatal("status failure hidden")
	}
	if _, e := r.Volume(ctx, Name("v", "pvc-a")); e == nil {
		t.Fatal("incomplete object treated as ready")
	}
	if _, e := r.CreateVolume(ctx, volumeSpec("pvc-a")); e == nil {
		t.Fatal("incomplete object silently replaced")
	}
}
func TestKubernetesBackendCASAndInt64(t *testing.T) {
	ctx := context.Background()
	r := New(NewKubernetes(kubeFixture(false), "clo-system"), Installation{installID, projectID})
	s := volumeSpec("pvc-a")
	s.RequiredBytes = (1 << 54) + 3
	v, e := r.CreateVolume(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if v.Spec.RequiredBytes != s.RequiredBytes {
		t.Fatal("int64 precision lost")
	}
	copy := v
	v.Status.Serial = "first"
	if _, e = r.SaveVolume(ctx, v, v.Version); e != nil {
		t.Fatal(e)
	}
	copy.Status.Serial = "second"
	if _, e = r.SaveVolume(ctx, copy, copy.Version); e == nil {
		t.Fatal("stale writer accepted")
	}
}

func TestKubernetesConcurrentCASHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	b := NewKubernetes(kubeFixture(false), "clo-system")
	d, e := b.Create(ctx, document(KindVolume, Meta{Name: "one"}, volumeSpec("pvc-a"), VolumeStatus{Initialized: true}))
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := b.CAS(ctx, d, d.Version); results <- e }()
	}
	wg.Wait()
	close(results)
	wins := 0
	for e := range results {
		if e == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("Kubernetes CAS winners=%d", wins)
	}
}
