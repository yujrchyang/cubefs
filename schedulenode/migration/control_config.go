package migration

type ControlConfig struct {
	InodeCheckStep  int
	InodeConcurrent int
	MinEkLen        int
	MinInodeSize    uint64
	MaxEkAvgSize    uint64
}