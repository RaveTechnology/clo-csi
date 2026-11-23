package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"io"
	corev1 "k8s.io/api/core/v1"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type AdminRequest struct {
	Scope    Scope                    `json:"scope"`
	Action   string                   `json:"action"`
	Recovery protocol.RecoveryRequest `json:"recovery"`
	Data     []byte                   `json:"data,omitempty"`
}
type AdminResponse struct {
	Error   string             `json:"error,omitempty"`
	Record  *registry.Document `json:"record,omitempty"`
	Data    []byte             `json:"data,omitempty"`
	Message string             `json:"message,omitempty"`
}

func safeAdminMessage(e error) string {
	var p *protocol.Error
	if errors.As(e, &p) {
		return p.Message
	}
	return "admin request rejected"
}

type AdminClient interface {
	Do(context.Context, AdminRequest) (AdminResponse, error)
}
type maintenance interface {
	Repository() *registry.Repository
	Recover(context.Context, protocol.RecoveryRequest) (registry.OperationRecord, error)
	Maintain(context.Context, protocol.RecoveryRequest, []byte) ([]byte, error)
}
type AdminService struct {
	coordinator maintenance
	scope       Scope
	mu          sync.Mutex
}

func NewAdminService(coordinator maintenance, scope Scope) *AdminService {
	return &AdminService{coordinator: coordinator, scope: scope}
}
func record(kind string, meta registry.Meta, spec, status any) *registry.Document {
	a, _ := json.Marshal(spec)
	b, _ := json.Marshal(status)
	return &registry.Document{Kind: kind, Name: meta.Name, UID: meta.UID, Version: meta.Version, Spec: a, Status: b}
}
func (s *AdminService) Do(ctx context.Context, q AdminRequest) (AdminResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out AdminResponse
	if e := q.Scope.Validate(); e != nil {
		return out, e
	}
	if q.Scope.ProjectID != s.scope.ProjectID || q.Scope.InstallationID != s.scope.InstallationID || q.Scope.Namespace != s.scope.Namespace {
		return out, errors.New("admin scope differs from running installation")
	}
	repo := s.coordinator.Repository()
	if repo.Installation().ID != q.Scope.InstallationID || repo.Installation().ProjectID != q.Scope.ProjectID {
		return out, errors.New("registry installation scope differs")
	}
	switch q.Action {
	case "inspect":
		if q.Recovery.OperationID != "" {
			v, e := repo.Operation(ctx, q.Recovery.OperationID)
			if e != nil {
				return out, e
			}
			out.Record = record(registry.KindOperation, v.Meta, v.Spec, v.Status)
		} else if q.Recovery.VolumeID != "" {
			var v registry.VolumeRecord
			var e error
			if strings.HasPrefix(q.Recovery.VolumeID, "clo:") {
				v, e = repo.FindVolume(ctx, q.Recovery.VolumeID)
			} else {
				v, e = repo.Volume(ctx, q.Recovery.VolumeID)
			}
			if e != nil {
				return out, e
			}
			out.Record = record(registry.KindVolume, v.Meta, v.Spec, v.Status)
		} else if q.Recovery.BindingID != "" {
			v, e := repo.Node(ctx, q.Recovery.BindingID)
			if e != nil {
				return out, e
			}
			out.Record = record(registry.KindNode, v.Meta, v.Spec, v.Status)
		} else {
			v, e := repo.Issuer(ctx)
			if e != nil {
				return out, e
			}
			out.Record = record(registry.KindIssuer, v.Meta, v.Spec, v.Status)
		}
		return out, nil
	case "freeze", "resume", "export", "restore":
		if q.Recovery.Action != q.Action {
			return out, errors.New("maintenance action differs")
		}
		if q.Recovery.DryRun {
			out.Message = "scope checked; maintenance action would run through coordinator"
			return out, nil
		}
		if q.Action == "restore" && len(q.Data) == 0 {
			return out, errors.New("backup data required")
		}
		data, e := s.coordinator.Maintain(ctx, q.Recovery, q.Data)
		if e != nil {
			return out, e
		}
		out.Data = data
		out.Message = q.Action + " completed"
		return out, nil
	case "reconcile", "record-fence", "recover-attachment", "clear-quarantine", "retire-unpublished", "retire-incomplete-format":
		if q.Recovery.Action != q.Action {
			return out, errors.New("recovery action differs")
		}
		v, e := s.coordinator.Recover(ctx, q.Recovery)
		if e != nil {
			return out, e
		}
		out.Record = record(registry.KindOperation, v.Meta, v.Spec, v.Status)
		out.Message = v.Status.Result
		if !q.Recovery.DryRun {
			switch q.Action {
			case "record-fence":
				f, e := repo.Fence(ctx, registry.Name("f", q.Recovery.Nonce))
				if e != nil {
					return AdminResponse{}, e
				}
				out.Record = record(registry.KindFence, f.Meta, f.Spec, f.Status)
			case "clear-quarantine":
				n, e := repo.Node(ctx, q.Recovery.BindingID)
				if e != nil {
					return AdminResponse{}, e
				}
				out.Record = record(registry.KindNode, n.Meta, n.Spec, n.Status)
			case "retire-unpublished":
				volume, e := repo.Volume(ctx, q.Recovery.VolumeID)
				if e != nil {
					return AdminResponse{}, e
				}
				out.Record = record(registry.KindVolume, volume.Meta, volume.Spec, volume.Status)
			}
		}
		return out, nil
	default:
		return out, errors.New("unsupported admin action")
	}
}
func adminHandler(service AdminClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/admin" {
			w.WriteHeader(404)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		var q AdminRequest
		if d.Decode(&q) != nil {
			w.WriteHeader(400)
			return
		}
		out, e := service.Do(r.Context(), q)
		w.Header().Set("Content-Type", "application/json")
		if e != nil {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": safeAdminMessage(e)})
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	})
}

type unixAdmin struct{ socket string }

func NewUnixAdminClient(socket string) AdminClient { return &unixAdmin{socket} }
func (c *unixAdmin) Do(ctx context.Context, q AdminRequest) (AdminResponse, error) {
	var out AdminResponse
	raw, e := json.Marshal(q)
	if e != nil {
		return out, e
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", c.socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Minute}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/admin", bytes.NewReader(raw))
	if e != nil {
		return out, e
	}
	response, e := client.Do(req)
	if e != nil {
		return out, errors.New("controller admin socket unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		var problem struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&problem)
		if problem.Error == "" {
			problem.Error = "controller admin request rejected"
		}
		return out, protocol.Fail(protocol.Blocked, "%s", problem.Error)
	}
	if json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(&out) != nil {
		return out, errors.New("invalid controller admin response")
	}
	return out, nil
}

type RemoteAdmin struct{ Kubeconfig string }

func (c *RemoteAdmin) command(ctx context.Context, scope Scope, input []byte, args ...string) ([]byte, error) {
	base := []string{"--context=" + scope.Context, "--namespace=" + scope.Namespace}
	if c.Kubeconfig != "" {
		base = append(base, "--kubeconfig="+c.Kubeconfig)
	}
	cmd := exec.CommandContext(ctx, "kubectl", append(base, args...)...)
	cmd.Stdin = bytes.NewReader(input)
	raw, e := cmd.Output()
	if e != nil {
		return nil, errors.New("scoped kubectl admin transport failed")
	}
	return raw, nil
}
func (c *RemoteAdmin) Do(ctx context.Context, q AdminRequest) (AdminResponse, error) {
	var out AdminResponse
	if e := q.Scope.Validate(); e != nil {
		return out, e
	}
	raw, e := c.command(ctx, q.Scope, nil, "get", "configmap", "clo-csi-installation", "-o", "json")
	if e != nil {
		return out, e
	}
	var marker corev1.ConfigMap
	if json.Unmarshal(raw, &marker) != nil || marker.Immutable == nil || !*marker.Immutable || marker.Namespace != q.Scope.Namespace || marker.Data["projectID"] != q.Scope.ProjectID || marker.Data["installationID"] != q.Scope.InstallationID {
		return out, errors.New("immutable installation marker differs from expected scope")
	}
	request, e := json.Marshal(q)
	if e != nil {
		return out, e
	}
	raw, e = c.command(ctx, q.Scope, request, "exec", "-i", "deploy/clo-csi-controller", "-c", "driver", "--", "/clo-csi", "admin", "local")
	if e != nil {
		return out, e
	}
	if json.Unmarshal(raw, &out) != nil {
		return out, errors.New("invalid scoped admin response")
	}
	if out.Error != "" {
		return AdminResponse{}, errors.New(out.Error)
	}
	return out, nil
}
