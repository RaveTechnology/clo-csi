package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

const intentKey = "initializationIntent"
const initializingLabel = Group + "/initializing"
const initializedAnnotation = Group + "/initialization-complete"

// The immutable spec carries a creation intent. No domain writer can observe
// the record until status is persisted AND the UID-bound metadata seal exists.
// In particular, a later loss of status on a sealed record is never repaired.
func intentDigest(obj *unstructured.Unstructured, status map[string]any) string {
	spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
	delete(spec, intentKey)
	return initializationDigest(obj, spec, status)
}
func initializationDigest(obj *unstructured.Unstructured, spec, status map[string]any) string {
	raw, _ := json.Marshal([]any{obj.GetNamespace(), obj.GetKind(), obj.GetName(), spec, status})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// The former CRD pruned apiVolumeIDs:null from operation specs. Reconstruct
// only that known loss, and only if the original immutable hash proves it.
// The persisted spec and its commitment are never rewritten.
func legacyPrunedOperationSpec(obj *unstructured.Unstructured, status map[string]any, digest string) (map[string]any, bool) {
	if obj.GetKind() != KindOperation || digest == "" {
		return nil, false
	}
	spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
	if _, present := spec["apiVolumeIDs"]; present {
		return nil, false
	}
	delete(spec, intentKey)
	spec["apiVolumeIDs"] = nil
	if initializationDigest(obj, spec, status) != digest {
		return nil, false
	}
	return spec, true
}
func creationIntent(obj *unstructured.Unstructured) (map[string]any, string, error) {
	intent, found, e := unstructured.NestedMap(obj.Object, "spec", intentKey)
	if e != nil || !found {
		return nil, "", protocol.Fail(protocol.RecoveryRequired, "missing immutable initialization intent")
	}
	status, ok := intent["status"].(map[string]any)
	digest, _ := intent["digest"].(string)
	if !ok || status["initialized"] != true || intent["version"] != "v1" || digest == "" {
		return nil, "", protocol.Fail(protocol.RecoveryRequired, "invalid immutable initialization intent")
	}
	if digest != intentDigest(obj, status) {
		if _, legacy := legacyPrunedOperationSpec(obj, status, digest); !legacy {
			return nil, "", protocol.Fail(protocol.RecoveryRequired, "invalid immutable initialization intent")
		}
	}
	return status, digest, nil
}
func readyInitialization(obj *unstructured.Unstructured) error {
	_, found, e := unstructured.NestedMap(obj.Object, "spec", intentKey)
	if e != nil {
		return protocol.Fail(protocol.RecoveryRequired, "invalid initialization metadata")
	}
	if !found { // Legacy records remain fail-closed through the existing status decoder.
		if obj.GetLabels()[initializingLabel] != "" || obj.GetAnnotations()[initializedAnnotation] != "" {
			return protocol.Fail(protocol.RecoveryRequired, "initialization intent lost")
		}
		return nil
	}
	_, digest, e := creationIntent(obj)
	if e != nil {
		return e
	}
	if obj.GetUID() == "" || obj.GetAnnotations()[initializedAnnotation] != "v1:"+string(obj.GetUID())+":"+digest || obj.GetLabels()[initializingLabel] != "" {
		return protocol.Fail(protocol.RecoveryRequired, "registry initialization not committed")
	}
	status, _, _ := unstructured.NestedMap(obj.Object, "status")
	if status["initialized"] != true {
		return protocol.Fail(protocol.RecoveryRequired, "committed registry status lost")
	}
	return nil
}
func (b *Kubernetes) completeInitialization(ctx context.Context, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	status, digest, e := creationIntent(obj)
	if e != nil {
		return nil, e
	}
	if obj.GetUID() == "" || obj.GetLabels()[initializingLabel] != "v1" || obj.GetAnnotations()[initializedAnnotation] != "" {
		return nil, protocol.Fail(protocol.RecoveryRequired, "initialization recovery requires an uncommitted creation intent")
	}
	current, found, e := unstructured.NestedMap(obj.Object, "status")
	if e != nil {
		return nil, protocol.Fail(protocol.RecoveryRequired, "invalid initial status")
	}
	ri, e := b.resource(obj.GetKind())
	if e != nil {
		return nil, e
	}
	if found && len(current) > 0 {
		a, _ := json.Marshal(current)
		z, _ := json.Marshal(status)
		if !equalJSON(a, z) {
			return nil, protocol.Fail(protocol.RecoveryRequired, "uncommitted status differs from creation intent")
		}
	} else {
		obj = obj.DeepCopy()
		obj.Object["status"] = status
		obj, e = ri.UpdateStatus(ctx, obj, metav1.UpdateOptions{})
		if e != nil {
			return nil, kubeError(e)
		}
	}
	labels := obj.GetLabels()
	delete(labels, initializingLabel)
	if labels == nil {
		labels = map[string]string{}
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[initializedAnnotation] = "v1:" + string(obj.GetUID()) + ":" + digest
	patch, _ := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(obj.GetUID())},
		{"op": "test", "path": "/metadata/resourceVersion", "value": obj.GetResourceVersion()},
		{"op": "add", "path": "/metadata/labels", "value": labels},
		{"op": "add", "path": "/metadata/annotations", "value": annotations},
	})
	ready, e := ri.Patch(ctx, obj.GetName(), types.JSONPatchType, patch, metav1.PatchOptions{})
	if e != nil {
		return nil, kubeError(e)
	}
	return ready, nil
}

// RecoverInitializations finishes only persisted, never-authorized creation
// transactions. It performs no native CLO calls and never rewrites a seal or a
// used status. Concurrent recovery uses the same resourceVersion/UID CAS.
func (b *Kubernetes) RecoverInitializations(ctx context.Context, installation Installation) error {
	for _, kind := range Kinds {
		ri, e := b.resource(kind)
		if e != nil {
			return e
		}
		next := ""
		for {
			list, e := ri.List(ctx, metav1.ListOptions{LabelSelector: initializingLabel + "=v1", Limit: 100, Continue: next})
			if e != nil {
				return kubeError(e)
			}
			for i := range list.Items {
				obj := &list.Items[i]
				install, _, _ := unstructured.NestedString(obj.Object, "spec", "installationID")
				project, _, _ := unstructured.NestedString(obj.Object, "spec", "projectID")
				if install != installation.ID || project != installation.ProjectID || obj.GetKind() != kind || obj.GetNamespace() != b.namespace {
					return protocol.Fail(protocol.RecoveryRequired, "initialization recovery scope mismatch")
				}
				if _, e = b.completeInitialization(ctx, obj); e != nil {
					return e
				}
			}
			if list.GetContinue() == "" {
				break
			}
			if list.GetContinue() == next {
				return protocol.Fail(protocol.RecoveryRequired, "repeated initialization pagination cursor")
			}
			next = list.GetContinue()
		}
	}
	return nil
}
func (r *Repository) RecoverInitializations(ctx context.Context) error {
	if b, ok := r.backend.(interface {
		RecoverInitializations(context.Context, Installation) error
	}); ok {
		return b.RecoverInitializations(ctx, r.installation)
	}
	return nil
}
