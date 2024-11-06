package connman

import (
	"errors"
	"net"
	"strings"
)

type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

var (
	ManagerClosedError   = &Error{message: "manager closed"}
	ConnectorClosedError = &Error{message: "connector closed"}
	AddrBlockedError     = &Error{message: "address temporary blocked"}
)

const (
	dialOp = "dial"
)

func IsDialNetOpError(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if is := errors.As(err, &opErr); !is {
		return false
	}
	return strings.EqualFold(opErr.Op, dialOp)
}

func IsDialTimeoutError(err error) bool {
	var opErr *net.OpError
	if is := errors.As(err, &opErr); !is {
		return false
	}
	return strings.EqualFold(opErr.Op, dialOp) && opErr.Timeout()
}

func NewDialNetOpError(network, addr string, err error) error {
	e := &net.OpError{
		Op:  dialOp,
		Net: network,
		Err: err,
	}
	e.Addr, _ = net.ResolveTCPAddr("tcp", addr)
	return e
}
