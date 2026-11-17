package registry

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kt "k8s.io/client-go/testing"
	"testing"
)

type initializationRecovery interface {
	RecoverInitializations(context.Context, Installation) error
}

func recoverInitializations(t *testing.T, b *Kubernetes) error {
	t.Helper()
	r, ok := any(b).(initializationRecovery)
	if !ok {
		t.Fatal("no initialization recovery path")
	}
	return r.RecoverInitializations(context.Background(), Installation{installID, projectID})
}
func TestKubernetesInterruptedInitializationRecoversOriginalUIDAllKinds(t *testing.T) {
	for _, kind := range Kinds {
		for _, fault := range []string{"before-status", "lost-status-response", "before-seal"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				ctx := context.Background()
				c := kubeFixture(false)
				injected := false
				c.PrependReactor("*", "*", func(a kt.Action) (bool, runtime.Object, error) {
					match := a.GetVerb() == "update" && a.GetSubresource() == "status" && fault != "before-seal" || a.GetVerb() == "patch" && fault == "before-seal"
					if !match || injected {
						return false, nil, nil
					}
					injected = true
					if fault == "lost-status-response" {
						// Invoke the underlying status reactor, persisting its result, then lose the reply.
						handled, _, e := c.ReactionChain[1].React(a)
						if !handled || e != nil {
							t.Fatalf("underlying status write: %v", e)
						}
					}
					return true, nil, fmt.Errorf("injected initialization interruption")
				})
				b := NewKubernetes(c, "clo-system")
				d := document(kind, Meta{Name: "init-test"}, map[string]any{"installationID": installID, "projectID": projectID}, map[string]any{"initialized": true, "sent": false})
				if _, e := b.Create(ctx, d); e == nil {
					t.Fatal("initialization interruption not visible")
				}
				raw, e := c.Resource(Resource(kind)).Namespace("clo-system").Get(ctx, d.Name, metav1.GetOptions{})
				if e != nil {
					t.Fatal(e)
				}
				uid := string(raw.GetUID())
				if _, e = b.Get(ctx, kind, d.Name); e == nil {
					t.Fatal("unsealed record exposed to domain writers")
				}
				restarted := NewKubernetes(c, "clo-system")
				if e = recoverInitializations(t, restarted); e != nil {
					t.Fatal(e)
				}
				ready, e := restarted.Get(ctx, kind, d.Name)
				if e != nil || ready.UID != uid || !equalJSON(ready.Spec, d.Spec) || !equalJSON(ready.Status, d.Status) {
					t.Fatalf("recovery changed identity/payload: %+v %v", ready, e)
				}
				if e = recoverInitializations(t, restarted); e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}
func TestKubernetesInitializationRecoveryNeverResetsUsedOrLegacyLostStatus(t *testing.T) {
	ctx := context.Background()
	c := kubeFixture(false)
	b := NewKubernetes(c, "clo-system")
	d, e := b.Create(ctx, document(KindOperation, Meta{Name: "used-op"}, map[string]any{"installationID": installID, "projectID": projectID}, map[string]any{"initialized": true, "sent": false}))
	if e != nil {
		t.Fatal(e)
	}
	d.Status = []byte(`{"initialized":true,"sent":true,"phase":"Dispatched"}`)
	if _, e = b.CAS(ctx, d, d.Version); e != nil {
		t.Fatal(e)
	}
	ri := c.Resource(Resource(KindOperation)).Namespace("clo-system")
	raw, _ := ri.Get(ctx, d.Name, metav1.GetOptions{})
	delete(raw.Object, "status")
	if _, e = ri.UpdateStatus(ctx, raw, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if e = recoverInitializations(t, b); e != nil {
		t.Fatal(e)
	}
	if got, e := b.Get(ctx, d.Kind, d.Name); e == nil && len(got.Status) > 0 {
		t.Fatal("used Sent state reset")
	}
	raw, _ = ri.Get(ctx, d.Name, metav1.GetOptions{})
	if status, _, _ := unstructured.NestedMap(raw.Object, "status"); len(status) > 0 {
		t.Fatal("lost status rewritten")
	}
}

func TestKubernetesRecoveryRejectsChangedUncommittedStatusAndForeignScope(t *testing.T) {
	for _, alter := range []string{"used-status", "foreign-scope", "bad-digest"} {
		t.Run(alter, func(t *testing.T) {
			ctx := context.Background()
			c := kubeFixture(true)
			b := NewKubernetes(c, "clo-system")
			spec := map[string]any{"installationID": installID, "projectID": projectID}
			if alter == "foreign-scope" {
				spec["projectID"] = "foreign"
			}
			if _, e := b.Create(ctx, document(KindOperation, Meta{Name: "partial"}, spec, map[string]any{"initialized": true, "sent": false})); e == nil {
				t.Fatal("missing injected failure")
			}
			c.PrependReactor("list", "*", func(a kt.Action) (bool, runtime.Object, error) {
				handled, obj, e := c.ReactionChain[1].React(a)
				if !handled || e != nil {
					return handled, obj, e
				}
				list := obj.(*unstructured.UnstructuredList)
				for i := range list.Items {
					switch alter {
					case "used-status":
						list.Items[i].Object["status"] = map[string]any{"initialized": true, "sent": true}
					case "bad-digest":
						_ = unstructured.SetNestedField(list.Items[i].Object, "wrong", "spec", intentKey, "digest")
					}
				}
				return true, list, nil
			})
			if e := recoverInitializations(t, b); e == nil {
				t.Fatal("unsafe initial state recovered")
			}
			for _, action := range c.Actions() {
				if action.GetVerb() == "patch" {
					t.Fatal("unsafe record sealed")
				}
			}
		})
	}
}
func TestKubernetesLegacyStatusLossIsNeverInitializedFromSpec(t *testing.T) {
	ctx := context.Background()
	c := kubeFixture(false)
	ri := c.Resource(Resource(KindVolume)).Namespace("clo-system")
	obj, _ := unstructuredDocument(document(KindVolume, Meta{Name: "legacy"}, volumeSpec("legacy"), VolumeStatus{Initialized: true}))
	obj.SetNamespace("clo-system")
	if _, e := ri.Create(ctx, obj, metav1.CreateOptions{}); e != nil {
		t.Fatal(e)
	}
	b := NewKubernetes(c, "clo-system")
	if e := recoverInitializations(t, b); e != nil {
		t.Fatal(e)
	}
	r := New(b, Installation{installID, projectID})
	if _, e := r.Volume(ctx, "legacy"); e == nil {
		t.Fatal("legacy missing status authorized")
	}
	for _, a := range c.Actions() {
		if a.GetVerb() == "patch" || a.GetVerb() == "update" {
			t.Fatal("legacy object rewritten")
		}
	}
}
