package channel

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"golang.org/x/time/rate"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestConcurrentStageProofsQueueBeforeHTTPDeadline(t *testing.T) {
	// One cloud proof needs several fresh paced API reads. Starting the HTTP
	// deadline for all eight callers at once makes the last callers expire
	// before their proof can finish, even though each individual proof fits.
	api := rate.NewLimiter(rate.Every(20*time.Millisecond), 1)
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for range 7 {
			if e := api.Wait(r.Context()); e != nil {
				return
			}
		}
		_ = json.NewEncoder(w).Encode(protocol.StagePermit{})
	}))
	defer s.Close()
	c, e := NewClient(s.URL, certificatePEM(s.Certificate()), tokenFile(t))
	if e != nil {
		t.Fatal(e)
	}
	c.http.Timeout = 400 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := make(chan struct{})
	errors := make(chan error, 8)
	for range 8 {
		go func() { <-start; _, e := c.AuthorizeStage(ctx, protocol.StageRequest{}); errors <- e }()
	}
	close(start)
	for range 8 {
		if e = <-errors; e != nil {
			t.Error("individually bounded proof expired in the shared API queue:", e)
		}
	}
}

func TestCloudProofAdmissionLeavesRecoveryChannelUsable(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stage" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(protocol.StagePermit{})
			return
		}
		_ = json.NewEncoder(w).Encode(protocol.NodeRef{BindingID: "live-binding"})
	}))
	defer s.Close()
	c, e := NewClient(s.URL, certificatePEM(s.Certificate()), tokenFile(t))
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := c.AuthorizeStage(context.Background(), protocol.StageRequest{}); done <- e }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ref, e := c.Binding(ctx)
	close(release)
	if e != nil || ref.BindingID != "live-binding" {
		t.Fatal("cloud proof blocked the recovery channel", e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
