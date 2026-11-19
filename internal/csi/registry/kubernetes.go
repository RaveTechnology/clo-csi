package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kjson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/dynamic"
)

const Group = "storage.clo.rave-technology.io"

func Resource(kind string) schema.GroupVersionResource {
	plural := map[string]string{KindVolume: "clovolumes", KindNode: "clonodebindings", KindOperation: "clooperations", KindFence: "clofences", KindIssuer: "clocontrollerepochs"}[kind]
	return schema.GroupVersionResource{Group: Group, Version: "v1alpha1", Resource: plural}
}

type Kubernetes struct {
	client    dynamic.Interface
	namespace string
}

func NewKubernetes(client dynamic.Interface, namespace string) *Kubernetes {
	return &Kubernetes{client, namespace}
}
func (b *Kubernetes) resource(kind string) (dynamic.ResourceInterface, error) {
	gvr := Resource(kind)
	if gvr.Resource == "" || b.namespace == "" {
		return nil, protocol.Fail(protocol.Invalid, "unknown registry kind or namespace")
	}
	return b.client.Resource(gvr).Namespace(b.namespace), nil
}
func kubeError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case apierrors.IsNotFound(err):
		return protocol.Fail(protocol.NotFound, "registry object missing")
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		return protocol.Fail(protocol.Conflict, "registry CAS conflict")
	default:
		return protocol.Fail(protocol.Unavailable, "Kubernetes registry request failed")
	}
}
func unstructuredDocument(d Document) (*unstructured.Unstructured, error) {
	spec := map[string]any{}
	status := map[string]any{}
	if kjson.Unmarshal(d.Spec, &spec) != nil || kjson.Unmarshal(d.Status, &status) != nil {
		return nil, protocol.Fail(protocol.Invalid, "invalid document JSON")
	}
	obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": Group + "/v1alpha1", "kind": d.Kind, "spec": spec, "status": status}}
	obj.SetName(d.Name)
	obj.SetUID(types.UID(d.UID))
	obj.SetResourceVersion(d.Version)
	obj.SetFinalizers([]string{Group + "/protect"})
	return obj, nil
}
func asDocument(obj *unstructured.Unstructured) (Document, error) {
	if e := readyInitialization(obj); e != nil {
		return Document{}, e
	}
	domainSpec, _, _ := unstructured.NestedMap(obj.Object, "spec")
	delete(domainSpec, intentKey)
	if intent, found, _ := unstructured.NestedMap(obj.Object, "spec", intentKey); found {
		status, _ := intent["status"].(map[string]any)
		digest, _ := intent["digest"].(string)
		if original, legacy := legacyPrunedOperationSpec(obj, status, digest); legacy {
			// Keep typed status CAS equivalent to the original committed request.
			domainSpec = original
		}
	}
	spec, e := json.Marshal(domainSpec)
	if e != nil {
		return Document{}, e
	}
	var status json.RawMessage
	if obj.Object["status"] != nil {
		status, e = json.Marshal(obj.Object["status"])
	}
	return Document{obj.GetKind(), obj.GetName(), string(obj.GetUID()), obj.GetResourceVersion(), spec, status}, e
}
func (b *Kubernetes) Get(ctx context.Context, kind, name string) (Document, error) {
	ri, e := b.resource(kind)
	if e != nil {
		return Document{}, e
	}
	obj, e := ri.Get(ctx, name, metav1.GetOptions{})
	if e != nil {
		return Document{}, kubeError(e)
	}
	return asDocument(obj)
}
func (b *Kubernetes) Create(ctx context.Context, d Document) (Document, error) {
	ri, e := b.resource(d.Kind)
	if e != nil {
		return Document{}, e
	}
	obj, e := unstructuredDocument(d)
	if e != nil {
		return Document{}, e
	}
	obj.SetNamespace(b.namespace)
	status := obj.Object["status"].(map[string]any)
	if status["initialized"] != true {
		return Document{}, protocol.Fail(protocol.Invalid, "initial status must be initialized")
	}
	obj.Object["spec"].(map[string]any)[intentKey] = map[string]any{"version": "v1", "status": status, "digest": intentDigest(obj, status)}
	obj.SetLabels(map[string]string{initializingLabel: "v1"})
	delete(obj.Object, "status")
	created, e := ri.Create(ctx, obj, metav1.CreateOptions{})
	if e != nil {
		return Document{}, kubeError(e)
	}
	ready, e := b.completeInitialization(ctx, created)
	if e != nil {
		return Document{}, e
	}
	return asDocument(ready)
}
func (b *Kubernetes) CAS(ctx context.Context, d Document, expected string) (Document, error) {
	if d.Version != expected || d.UID == "" {
		return Document{}, protocol.Fail(protocol.Conflict, "missing CAS identity")
	}
	ri, e := b.resource(d.Kind)
	if e != nil {
		return Document{}, e
	}
	obj, e := ri.Get(ctx, d.Name, metav1.GetOptions{})
	if e != nil {
		return Document{}, kubeError(e)
	}
	current, e := asDocument(obj)
	if e != nil {
		return Document{}, e
	}
	if current.UID != d.UID || current.Version != expected || !equalJSON(current.Spec, d.Spec) {
		return Document{}, protocol.Fail(protocol.Conflict, "registry CAS identity changed")
	}
	status := map[string]any{}
	if kjson.Unmarshal(d.Status, &status) != nil {
		return Document{}, protocol.Fail(protocol.Invalid, "invalid status JSON")
	}
	obj.Object["status"] = status
	updated, e := ri.UpdateStatus(ctx, obj, metav1.UpdateOptions{})
	if e != nil {
		return Document{}, kubeError(e)
	}
	return asDocument(updated)
}
func (b *Kubernetes) List(ctx context.Context, kind string) ([]Document, error) {
	ri, e := b.resource(kind)
	if e != nil {
		return nil, e
	}
	var out []Document
	next := ""
	for {
		list, e := ri.List(ctx, metav1.ListOptions{Limit: 100, Continue: next})
		if e != nil {
			return nil, kubeError(e)
		}
		for _, obj := range list.Items {
			d, e := asDocument(&obj)
			if e != nil {
				return nil, fmt.Errorf("registry decode: %w", e)
			}
			out = append(out, d)
		}
		if list.GetContinue() == "" {
			break
		}
		if list.GetContinue() == next {
			return nil, protocol.Fail(protocol.RecoveryRequired, "repeated Kubernetes pagination cursor")
		}
		next = list.GetContinue()
	}
	return out, nil
}
