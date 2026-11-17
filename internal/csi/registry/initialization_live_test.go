package registry

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strings"
	"testing"
)

// Explicitly opted-in, isolated local API only. CRDs and rendered chart RBAC
// must already be installed in this disposable cluster's clo-system namespace.
// This never uses the ambient KUBECONFIG and never connects to CLO.
type interruptedDynamic struct {
	dynamic.Interface
	fault string
	fired bool
}
type interruptedNamespaceable struct {
	dynamic.NamespaceableResourceInterface
	owner *interruptedDynamic
}
type interruptedResource struct {
	dynamic.ResourceInterface
	owner *interruptedDynamic
}

func (d *interruptedDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &interruptedNamespaceable{d.Interface.Resource(gvr), d}
}
func (n *interruptedNamespaceable) Namespace(ns string) dynamic.ResourceInterface {
	return &interruptedResource{n.NamespaceableResourceInterface.Namespace(ns), n.owner}
}
func (r *interruptedResource) UpdateStatus(ctx context.Context, o *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	d := r.owner
	if !d.fired && d.fault != "before-seal" {
		d.fired = true
		if d.fault == "lost-status-response" {
			if _, e := r.ResourceInterface.UpdateStatus(ctx, o, opts); e != nil {
				return nil, e
			}
		}
		return nil, fmt.Errorf("injected status interruption")
	}
	return r.ResourceInterface.UpdateStatus(ctx, o, opts)
}
func (r *interruptedResource) Patch(ctx context.Context, name string, typ types.PatchType, data []byte, opts metav1.PatchOptions, sub ...string) (*unstructured.Unstructured, error) {
	if !r.owner.fired && r.owner.fault == "before-seal" {
		r.owner.fired = true
		return nil, fmt.Errorf("injected seal interruption")
	}
	return r.ResourceInterface.Patch(ctx, name, typ, data, opts, sub...)
}
func TestLocalAPIInitializationRecovery(t *testing.T) {
	file := os.Getenv("CLO_REGISTRY_LOCAL_KUBECONFIG")
	if file == "" {
		t.Skip("isolated local API opt-in not set")
	}
	cfg, e := clientcmd.BuildConfigFromFlags("", file)
	if e != nil {
		t.Fatal("cannot read local test kubeconfig")
	}
	if !strings.HasPrefix(cfg.Host, "https://127.0.0.1:") {
		t.Fatal("local acceptance requires an explicit loopback API endpoint")
	}
	cfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:clo-system:clo-csi-controller"}
	client, e := dynamic.NewForConfig(cfg)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	for ki, kind := range Kinds {
		for fi, fault := range []string{"before-status", "lost-status-response", "before-seal"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				spec := map[string]any{"installationID": installID, "projectID": projectID}
				switch kind {
				case KindVolume:
					spec["csiName"] = "live"
					spec["fingerprint"] = "fp"
					spec["providerName"] = "csi-live"
					spec["requiredBytes"] = int64(1<<54 + 3)
					spec["fsType"] = "ext4"
					spec["accessMode"] = "SINGLE_NODE_WRITER"
				case KindNode:
					spec["ref"] = map[string]any{"uid": "node-live"}
					spec["identity"] = map[string]any{"attempt": "live"}
				case KindOperation:
					spec["type"] = "attach"
					spec["nonce"] = "live"
					spec["issuer"] = map[string]any{"podUID": "pod-live"}
					spec["before"] = map[string]any{"bootID": "boot-live"}
				case KindFence:
					spec["bindingID"] = "binding-live"
					spec["serverID"] = projectID
					spec["nodeUID"] = "node-live"
					spec["epoch"] = int64(1)
					spec["operator"] = "local-test"
					spec["nonce"] = "live"
				}
				name := fmt.Sprintf("local-init-%d-%d", ki, fi)
				d := document(kind, Meta{Name: name}, spec, map[string]any{"initialized": true, "sent": false, "largeInteger": int64(1<<54 + 3)})
				interrupted := &interruptedDynamic{Interface: client, fault: fault}
				if _, e := NewKubernetes(interrupted, "clo-system").Create(ctx, d); e == nil || !interrupted.fired {
					t.Fatalf("interruption not reached: %v", e)
				}
				ri := client.Resource(Resource(kind)).Namespace("clo-system")
				raw, e := ri.Get(ctx, name, metav1.GetOptions{})
				if e != nil {
					t.Fatal(e)
				}
				uid := string(raw.GetUID())
				b := NewKubernetes(client, "clo-system")
				if _, e = b.Get(ctx, kind, name); e == nil {
					t.Fatal("uncommitted record exposed")
				}
				if e = b.RecoverInitializations(ctx, Installation{installID, projectID}); e != nil {
					t.Fatal(e)
				}
				ready, e := b.Get(ctx, kind, name)
				if e != nil || ready.UID != uid || !equalJSON(ready.Spec, d.Spec) || !equalJSON(ready.Status, d.Status) {
					t.Fatalf("UID/spec/status recovery mismatch: %v", e)
				}
				// Metadata patch permission must not permit changing immutable ownership or digest.
				for _, patch := range []string{`[{"op":"replace","path":"/spec/projectID","value":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}]`, `[{"op":"replace","path":"/spec/initializationIntent/digest","value":"0000000000000000000000000000000000000000000000000000000000000000"}]`} {
					if _, e = ri.Patch(ctx, name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{}); e == nil {
						t.Fatal("CEL allowed immutable spec patch")
					}
				}
				ready.Status = []byte(`{"initialized":true,"sent":true,"phase":"Dispatched"}`)
				if _, e = b.CAS(ctx, ready, ready.Version); e != nil {
					t.Fatal(e)
				}
				raw, e = ri.Get(ctx, name, metav1.GetOptions{})
				if e != nil {
					t.Fatal(e)
				}
				delete(raw.Object, "status")
				if _, e = ri.UpdateStatus(ctx, raw, metav1.UpdateOptions{}); e != nil {
					t.Fatal(e)
				}
				if e = b.RecoverInitializations(ctx, Installation{installID, projectID}); e != nil {
					t.Fatal(e)
				}
				if _, e = b.Get(ctx, kind, name); e == nil {
					t.Fatal("used status loss silently recovered")
				}
				raw, e = ri.Get(ctx, name, metav1.GetOptions{})
				if e != nil {
					t.Fatal(e)
				}
				if status, _, _ := unstructured.NestedMap(raw.Object, "status"); len(status) > 0 {
					t.Fatal("Sent status was reset")
				}
			})
		}
	}
}
