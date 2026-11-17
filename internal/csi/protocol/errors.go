package protocol

import "fmt"

type Code string

const (
	Conflict         Code = "Conflict"
	Blocked          Code = "Blocked"
	RecoveryRequired Code = "RecoveryRequired"
	Invalid          Code = "Invalid"
	NotFound         Code = "NotFound"
	Exists           Code = "AlreadyExists"
	Unavailable      Code = "Unavailable"
)

type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }
func Fail(code Code, format string, args ...any) error {
	return &Error{code, fmt.Sprintf(format, args...)}
}
