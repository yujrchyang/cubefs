package migration

import "fmt"

type ControlConfig struct {
	InodeCheckStep  int
	InodeConcurrent int
	MinEkLen        int
	MinInodeSize    uint64
	MaxEkAvgSize    uint64
	DirectWrite     bool
}

func (c *ControlConfig) String() string {
	return fmt.Sprintf("InodeCheckStep=%v InodeConcurrent=%v MinEkLen=%v MinInodeSize=%v MaxEkAvgSize=%v DirectWrite=%v",
		c.InodeCheckStep, c.InodeConcurrent, c.MinEkLen, c.MinInodeSize, c.MaxEkAvgSize, c.DirectWrite)
}