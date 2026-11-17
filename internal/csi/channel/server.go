package channel

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"io"
	"net/http"
	"strings"
	"time"
)

type Service interface {
	RegisterNode(context.Context, protocol.Caller, protocol.NodeHello) (protocol.NodeRef, error)
	Binding(context.Context, protocol.Caller) (protocol.NodeRef, error)
	AuthorizeStage(context.Context, protocol.StageRequest) (protocol.StagePermit, error)
	Receive(context.Context, protocol.NodeRef, protocol.Receipt) error
	PollNodeJob(context.Context, protocol.NodeRef) (protocol.Job, error)
	ReceiveJobReceipt(context.Context, protocol.NodeRef, protocol.Receipt) error
	Ready(context.Context) bool
}
type wireError struct {
	Code    protocol.Code `json:"code"`
	Message string        `json:"message"`
}

func reply(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, e error) {
	code, status, msg := protocol.Unavailable, http.StatusInternalServerError, "controller request failed"
	var pe *protocol.Error
	if errors.As(e, &pe) {
		code = pe.Code
		msg = pe.Message
		switch code {
		case protocol.Invalid:
			status = 400
		case protocol.NotFound:
			status = 404
		case protocol.Conflict, protocol.Blocked, protocol.RecoveryRequired:
			status = 409
		case protocol.Unavailable:
			status = 503
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wireError{code, msg})
}
func decodeBody(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(value); e != nil {
		return protocol.Fail(protocol.Invalid, "invalid node request JSON")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return protocol.Fail(protocol.Invalid, "trailing node request JSON")
	}
	return nil
}
func NewServer(service Service, auth Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.RawQuery != "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			w.WriteHeader(http.StatusUnauthorized)
			reply(w, wireError{protocol.Invalid, "node authentication rejected"})
			return
		}
		caller, e := auth.Authenticate(r.Context(), strings.TrimPrefix(header, "Bearer "))
		if e != nil {
			var pe *protocol.Error
			if errors.As(e, &pe) && pe.Code == protocol.Unavailable {
				failure(w, authUnavailable())
			} else {
				w.WriteHeader(http.StatusUnauthorized)
				reply(w, wireError{protocol.Invalid, "node authentication rejected"})
			}
			return
		}
		if service == nil {
			failure(w, protocol.Fail(protocol.Unavailable, "controller unavailable"))
			return
		}
		ctx := r.Context()
		if r.URL.Path == "/register" {
			var hello protocol.NodeHello
			if e = decodeBody(w, r, &hello); e != nil {
				failure(w, e)
				return
			}
			ref, e := service.RegisterNode(ctx, caller, hello)
			if e != nil {
				failure(w, e)
				return
			}
			reply(w, ref)
			return
		}
		binding, e := service.Binding(ctx, caller)
		if e != nil {
			failure(w, e)
			return
		}
		if binding.UID != caller.NodeUID || binding.Name != caller.NodeName || binding.BindingID == "" {
			failure(w, protocol.Fail(protocol.Blocked, "authenticated node binding differs"))
			return
		}
		switch r.URL.Path {
		case "/binding":
			reply(w, binding)
		case "/poll":
			pollCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			defer cancel()
			job, e := service.PollNodeJob(pollCtx, binding)
			if errors.Is(e, context.DeadlineExceeded) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if e != nil {
				failure(w, e)
				return
			}
			reply(w, job)
		case "/stage":
			var request protocol.StageRequest
			if e = decodeBody(w, r, &request); e == nil && request.Node != binding {
				e = protocol.Fail(protocol.Blocked, "stage node generation differs")
			}
			if e != nil {
				failure(w, e)
				return
			}
			permit, e := service.AuthorizeStage(ctx, request)
			if e != nil {
				failure(w, e)
				return
			}
			reply(w, permit)
		case "/receipt", "/report":
			var receipt protocol.Receipt
			if e = decodeBody(w, r, &receipt); e == nil && receipt.Node != binding {
				e = protocol.Fail(protocol.Blocked, "receipt node generation differs")
			}
			if e != nil {
				failure(w, e)
				return
			}
			if r.URL.Path == "/receipt" {
				e = service.ReceiveJobReceipt(ctx, binding, receipt)
			} else {
				e = service.Receive(ctx, binding, receipt)
			}
			if e != nil {
				failure(w, e)
				return
			}
			reply(w, struct{}{})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}
