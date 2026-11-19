package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func typedInitializationOperation(ids []string) Document {
	return document(KindOperation, Meta{Name: "typed-init-operation"}, OperationSpec{
		InstallationID: installID, ProjectID: projectID, Type: "create", Nonce: "original-nonce",
		VolumeRecordID: "v-original", Issuer: protocol.IssuerDescriptor{PodUID: "original-pod"},
		Before: protocol.Snapshot{}, APIVolumeIDs: ids, TargetSizeGB: 10,
		Deadline: time.Date(2026, 11, 19, 12, 0, 0, 123456789, time.UTC),
	}, OperationStatus{Initialized: true, Phase: Prepared})
}

func TestKubernetesTypedOperationPreservesVolumeLists(t *testing.T) {
	for _, ids := range [][]string{nil, {}, {"known-boot"}} {
		ctx := context.Background()
		b := NewKubernetes(kubeFixture(false), "clo-system")
		d, err := b.Create(ctx, typedInitializationOperation(ids))
		if err != nil {
			t.Fatalf("typed create with apiVolumeIDs=%v could not initialize: %v", ids, err)
		}
		got, err := b.Get(ctx, KindOperation, d.Name)
		if err != nil || got.UID != d.UID || got.UID == "" {
			t.Fatalf("committed operation unavailable or identity changed: %+v %v", got, err)
		}
		var spec OperationSpec
		var status OperationStatus
		if json.Unmarshal(got.Spec, &spec) != nil || json.Unmarshal(got.Status, &status) != nil || !reflect.DeepEqual(spec.APIVolumeIDs, ids) || spec.TargetSizeGB != 10 || !status.Initialized || status.Sent || status.Phase != Prepared {
			t.Fatalf("initialization changed original request or authorized a send: %+v %+v", spec, status)
		}
	}
}

// Encode the old v1 commitment independently, before the API prunes its null.
func legacyNullOperation(t *testing.T) (*unstructured.Unstructured, string) {
	t.Helper()
	obj, err := unstructuredDocument(typedInitializationOperation(nil))
	if err != nil {
		t.Fatal(err)
	}
	obj.SetNamespace("clo-system")
	spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
	status, _, _ := unstructured.NestedMap(obj.Object, "status")
	raw, err := json.Marshal([]any{"clo-system", "CloOperation", "typed-init-operation", spec, status})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	obj.Object["spec"].(map[string]any)[intentKey] = map[string]any{"version": "v1", "digest": digest, "status": status}
	obj.SetLabels(map[string]string{initializingLabel: "v1"})
	delete(obj.Object, "status")
	// Reproduce the persisted object, after the former CRD pruned its null.
	unstructured.RemoveNestedField(obj.Object, "spec", "apiVolumeIDs")
	return obj, digest
}

func TestKubernetesRecoversOriginalLegacyPrunedNullCommitment(t *testing.T) {
	ctx := context.Background()
	c := kubeFixture(false)
	ri := c.Resource(Resource(KindOperation)).Namespace("clo-system")
	obj, digest := legacyNullOperation(t)
	stored, err := ri.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b := NewKubernetes(c, "clo-system")
	if err = recoverInitializations(t, b); err != nil {
		t.Fatalf("original never-authorized operation cannot recover: %v", err)
	}
	got, err := b.Get(ctx, KindOperation, obj.GetName())
	if err != nil || got.UID != string(stored.GetUID()) || !equalJSON(got.Status, []byte(`{"initialized":true,"phase":"Prepared","sent":false,"result":"","reason":""}`)) {
		t.Fatalf("legacy recovery changed UID or original unsent status: %+v %v", got, err)
	}
	after, _ := ri.Get(ctx, obj.GetName(), metav1.GetOptions{})
	if !reflect.DeepEqual(after.Object["spec"], stored.Object["spec"]) || after.GetAnnotations()[initializedAnnotation] != "v1:"+string(stored.GetUID())+":"+digest {
		t.Fatal("recovery rewrote immutable spec/commitment or used a different seal")
	}
	r := New(b, Installation{installID, projectID})
	operation, err := r.Operation(ctx, obj.GetName())
	if err != nil {
		t.Fatal(err)
	}
	operation.Status.Phase = Dispatched
	operation.Status.Sent = true
	if _, err = r.SaveOperation(ctx, operation, operation.Version); err != nil {
		t.Fatalf("recovered typed operation cannot use normal status CAS: %v", err)
	}
}

// The actual API, not the dynamic fake, must exercise structural CRD pruning.
// This opt-in uses a separately owned namespace and never calls the CLO API.
func TestLocalAPITypedOperationInitializationAndStatusCAS(t *testing.T) {
	file := os.Getenv("CLO_REGISTRY_LOCAL_KUBECONFIG")
	if file == "" {
		t.Skip("explicit isolated loopback API opt-in not set")
	}
	ns := os.Getenv("CLO_REGISTRY_NULL_TEST_NAMESPACE")
	run := os.Getenv("CLO_REGISTRY_NULL_TEST_RUN")
	if !strings.HasPrefix(ns, "clo-registry-init-") || run == "" {
		t.Fatal("explicit test namespace and owner marker required")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", file)
	if err != nil || !strings.HasPrefix(cfg.Host, "https://127.0.0.1:") {
		t.Fatal("isolated loopback API kubeconfig required")
	}
	c, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	namespace, err := c.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Get(ctx, ns, metav1.GetOptions{})
	if err != nil || namespace.GetLabels()["clo.rave-technology.io/lab-run"] != run {
		t.Fatal("test namespace owner differs")
	}
	b := NewKubernetes(c, ns)
	if err = recoverInitializations(t, b); err != nil {
		t.Fatal(err)
	}
	for i, ids := range [][]string{nil, {}, {"known-boot"}} {
		doc := typedInitializationOperation(ids)
		doc.Name = fmt.Sprintf("typed-null-%d-%d", time.Now().UnixNano(), i)
		d, err := b.Create(ctx, doc)
		if err != nil {
			t.Fatalf("typed apiVolumeIDs=%v rejected after API round trip: %v", ids, err)
		}
		var spec OperationSpec
		if json.Unmarshal(d.Spec, &spec) != nil || !reflect.DeepEqual(spec.APIVolumeIDs, ids) {
			t.Fatal("real API changed the volume list")
		}
		r := New(b, Installation{installID, projectID})
		op, err := r.Operation(ctx, d.Name)
		if err != nil {
			t.Fatal(err)
		}
		op.Status.Phase, op.Status.Sent = Dispatched, true
		if _, err = r.SaveOperation(ctx, op, op.Version); err != nil {
			t.Fatalf("typed operation cannot persist sent status: %v", err)
		}
		ri := c.Resource(Resource(KindOperation)).Namespace(ns)
		raw, err := ri.Get(ctx, d.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		delete(raw.Object, "status")
		if _, err = ri.UpdateStatus(ctx, raw, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err = recoverInitializations(t, b); err != nil {
			t.Fatal(err)
		}
		if _, err = b.Get(ctx, KindOperation, d.Name); err == nil {
			t.Fatal("used status loss was silently reset")
		}
	}
}

func TestKubernetesLegacyNullCompatibilityCannotAuthorizeChangedOrUsedState(t *testing.T) {
	for _, fault := range []string{"changed-target", "added-api-volume", "used-status", "sealed-status-loss"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			c := kubeFixture(false)
			ri := c.Resource(Resource(KindOperation)).Namespace("clo-system")
			obj, digest := legacyNullOperation(t)
			if fault == "changed-target" {
				_ = unstructured.SetNestedField(obj.Object, int64(12), "spec", "targetSizeGB")
			}
			if fault == "added-api-volume" {
				_ = unstructured.SetNestedStringSlice(obj.Object, []string{"foreign-volume"}, "spec", "apiVolumeIDs")
			}
			stored, err := ri.Create(ctx, obj, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if fault == "used-status" {
				stored.Object["status"] = map[string]any{"initialized": true, "phase": "Dispatched", "sent": true}
				if _, err = ri.UpdateStatus(ctx, stored, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "sealed-status-loss" {
				patch, _ := json.Marshal([]map[string]any{
					{"op": "add", "path": "/metadata/labels", "value": map[string]string{}},
					{"op": "add", "path": "/metadata/annotations", "value": map[string]string{initializedAnnotation: "v1:" + string(stored.GetUID()) + ":" + digest}},
				})
				if _, err = ri.Patch(ctx, stored.GetName(), types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			b := NewKubernetes(c, "clo-system")
			err = recoverInitializations(t, b)
			if fault != "sealed-status-loss" && err == nil {
				t.Fatal("changed or already-used operation was initialized")
			}
			if _, err = b.Get(ctx, KindOperation, stored.GetName()); err == nil {
				t.Fatal("unsafe record exposed to domain writers")
			}
			after, _ := ri.Get(ctx, stored.GetName(), metav1.GetOptions{})
			status, _, _ := unstructured.NestedMap(after.Object, "status")
			if fault == "used-status" {
				if status["sent"] != true || status["phase"] != "Dispatched" {
					t.Fatal("previous send state was reset")
				}
			} else if len(status) != 0 {
				t.Fatal("unsafe or lost status was populated")
			}
		})
	}
}
