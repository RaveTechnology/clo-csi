package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/RaveTechnology/clo-cloud-go/cloud"
)

type inventoryHTTPRoundTrip func(*http.Request) (*http.Response, error)

func (f inventoryHTTPRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type inventoryHTTPCloud struct {
	cloud.StorageAPI
	client *cloud.Client
	broken string
}

func (c inventoryHTTPCloud) ListServers(ctx context.Context, project string) ([]cloud.Server, error) {
	return c.client.ListServers(ctx, project)
}
func (c inventoryHTTPCloud) ListVolumes(ctx context.Context, project string) ([]cloud.Volume, error) {
	if c.broken == "volumes" {
		return c.client.ListVolumes(ctx, project)
	}
	return c.StorageAPI.ListVolumes(ctx, project)
}

func TestLostEpochRejectsTruncatedHTTPInventoryWithoutChangingAuthority(t *testing.T) {
	for _, broken := range []string{"servers", "missing-result", "volumes"} {
		t.Run(broken, func(t *testing.T) {
			h := setup(t, true)
			ctx := context.Background()
			if broken == "volumes" {
				create(t, h, "owned-data")
			}
			if e := h.c.Freeze(ctx); e != nil {
				t.Fatal(e)
			}
			epoch, _ := h.r.Issuer(ctx)
			epoch.Status.RestoreNeedsFence = true
			if _, e := h.r.SaveIssuer(ctx, epoch, epoch.Version); e != nil {
				t.Fatal(e)
			}
			before, _ := h.r.Issuer(ctx)
			counts := []int{h.api.createCalls, h.api.attachCalls, h.api.detachCalls, h.api.extendCalls, h.api.deleteCalls, h.api.powerCalls}
			old := http.DefaultTransport
			http.DefaultTransport = inventoryHTTPRoundTrip(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" || r.URL.Host != "api.clo.ru" {
					t.Fatal("unexpected transport", r.Method, r.URL.Host)
				}
				count := 1
				result := []map[string]any{{"id": serverID, "name": h.api.server.Name, "project": projectID, "status": "ACTIVE", "switch_status": "ON"}}
				if strings.HasSuffix(r.URL.Path, "/servers") && broken != "volumes" {
					count = 2
				}
				if strings.HasSuffix(r.URL.Path, "/volumes") {
					count = 2
					result = []map[string]any{{"id": "boot-volume", "name": "boot", "size": 20, "bootable": true, "status": "IN_USE", "attached_to_server": map[string]any{"id": serverID}}}
				}
				body := map[string]any{"count": count, "result": result}
				if broken == "missing-result" {
					delete(body, "result")
				}
				raw, _ := json.Marshal(body)
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw))), Request: r}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = old })
			client, e := cloud.New("unit-only-token")
			if e != nil {
				t.Fatal(e)
			}
			h.c.api = inventoryHTTPCloud{h.api, client, broken}
			if e = h.c.Resume(ctx); e == nil {
				t.Fatal("partial inventory became proof of fencing")
			}
			after, _ := h.r.Issuer(ctx)
			if !after.Status.Frozen || !after.Status.RestoreNeedsFence || after.Status.Current != before.Status.Current || !reflect.DeepEqual(after.Status.Prior, before.Status.Prior) {
				t.Fatal("failed inventory changed authority")
			}
			if !reflect.DeepEqual(counts, []int{h.api.createCalls, h.api.attachCalls, h.api.detachCalls, h.api.extendCalls, h.api.deleteCalls, h.api.powerCalls}) {
				t.Fatal("recovery dispatched with incomplete inventory")
			}
		})
	}
}
