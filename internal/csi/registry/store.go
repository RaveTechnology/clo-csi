package registry

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	kjson "k8s.io/apimachinery/pkg/util/json"
	"reflect"
)

func equalJSON(a, b json.RawMessage) bool {
	var x, y any
	if kjson.Unmarshal(a, &x) != nil || kjson.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

type Backend interface {
	Get(context.Context, string, string) (Document, error)
	Create(context.Context, Document) (Document, error)
	CAS(context.Context, Document, string) (Document, error)
	List(context.Context, string) ([]Document, error)
}

func clone(d Document) Document {
	d.Spec = append(json.RawMessage(nil), d.Spec...)
	d.Status = append(json.RawMessage(nil), d.Status...)
	return d
}
func document(kind string, m Meta, spec, status any) Document {
	s, _ := json.Marshal(spec)
	t, _ := json.Marshal(status)
	return Document{kind, m.Name, m.UID, m.Version, s, t}
}
func decode(d Document, spec, status any) (Meta, error) {
	if len(d.Status) == 0 {
		return Meta{}, protocol.Fail(protocol.RecoveryRequired, "uninitialized %s/%s", d.Kind, d.Name)
	}
	var check struct {
		Initialized bool `json:"initialized"`
	}
	if json.Unmarshal(d.Status, &check) != nil || !check.Initialized {
		return Meta{}, protocol.Fail(protocol.RecoveryRequired, "incomplete registry status %s/%s", d.Kind, d.Name)
	}
	if json.Unmarshal(d.Spec, spec) != nil || json.Unmarshal(d.Status, status) != nil {
		return Meta{}, protocol.Fail(protocol.RecoveryRequired, "invalid registry document")
	}
	return Meta{d.Name, d.UID, d.Version}, nil
}
