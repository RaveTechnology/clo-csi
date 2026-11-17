package controller

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rpcError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	var e *protocol.Error
	if !errors.As(err, &e) {
		return status.Error(codes.Internal, "CSI operation failed")
	}
	code := codes.Internal
	switch e.Code {
	case protocol.Invalid:
		code = codes.InvalidArgument
	case protocol.Conflict:
		code = codes.Aborted
	case protocol.Blocked, protocol.RecoveryRequired:
		code = codes.FailedPrecondition
	case protocol.NotFound:
		code = codes.NotFound
	case protocol.Exists:
		code = codes.AlreadyExists
	case protocol.Unavailable:
		code = codes.Unavailable
	}
	return status.Error(code, e.Message)
}
