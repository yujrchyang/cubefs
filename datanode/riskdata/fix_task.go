package riskdata

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
)

type FixTask struct {
	WorkingDir  string
	PartitionID uint64
	Remotes     []string
	HAType      proto.CrossRegionHAType
	Fragment    *Fragment
	GetLocalCRC func() (uint32, error)
	GetLocalFGP func() (storage.Fingerprint, error)
}
