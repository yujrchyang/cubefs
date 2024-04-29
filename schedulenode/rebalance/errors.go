package rebalance

import "errors"

var (
	ErrNoSuitableDP       = errors.New("no suitable data partition")
	ErrWrongStatus        = errors.New("wrong status")
	ErrNoSuitableDstNode  = errors.New("no suitable destination dataNode")
	ErrWrongRatio         = errors.New("ratio setting error")
	ErrLessThanUsageRatio = errors.New("less than usage ratio")
	ErrInputNodesInvalid  = errors.New("input node list invalid")
	ErrQueryParams        = errors.New("should specify zoneName or taskId")
	ErrQueryRecord        = errors.New("can't find record")
	ErrParamsNotFount     = errors.New("params not found")
)
