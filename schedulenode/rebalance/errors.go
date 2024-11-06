package rebalance

import "errors"

var (
	ErrNoSuitablePartition   = errors.New("no suitable partition for migrate")
	ErrWrongStatus           = errors.New("wrong status")
	ErrNoSuitableDstNode     = errors.New("no suitable destination node")
	ErrWrongRatio            = errors.New("ratio setting error")
	ErrLessThanUsageRatio    = errors.New("less than usage ratio")
	ErrInputNodesInvalid     = errors.New("input node list invalid")
	ErrQueryParams           = errors.New("should specify zoneName or taskId")
	ErrQueryRecord           = errors.New("can't find record")
	ErrParamsNotFount        = errors.New("params not found")
	ErrInvalidWorkerType     = errors.New("invalid worker type")
	ErrReachMaxInodeLimit    = errors.New("reach migrate inode count limit on single node")
	ErrDataPartitionNotFound = errors.New("dataPartition not found")
	ErrReachClusterCurrency  = errors.New("reach cluster bad partition limit")
	ErrReachVolCurrency      = errors.New("reach vol bad partition limit")
	ErrMigVolumesTooMany     = errors.New("too many migrate volumes")
)
