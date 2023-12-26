package rebalance

import "errors"

var (
	ErrNoSuitableDP       = errors.New("no suitable data partition")
	ErrWrongStatus        = errors.New("wrong status")
	ErrNoSuitableDstNode  = errors.New("no suitable destination dataNode")
	ErrWrongRatio         = errors.New("ratio setting error")
	ErrLessThanUsageRatio = errors.New("less than usage ratio")
)
