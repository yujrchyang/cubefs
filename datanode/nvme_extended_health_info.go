package datanode

type nvmeExtendedHealthInfo struct {
	softReadRecoverableErrs     uint32
	flashDieRaidRecoverableErrs uint32
	pcieRxCorrectErrs           uint32
	pcieRxUncorrectErrs         uint32
	dataReadFromFlash           uint32
	dataWriteToFlash            uint32
	tempThrottleInfo            uint32
	powerConsumption            uint32
	pfBbdReadCnt                uint32
	sfxCriticalWarning          uint32
	raidRecoveryTotalCount      uint32
	rvd                         uint32
	opn                         [32]uint8
	totalPhysicalCapability     uint64
	freePhysicalCapability      uint64 //unit is sectorCount and the sector size is 512
	physicalUsageRatio          uint32
	compRatio                   uint32
	otpRsaEn                    uint32
	powerMwConsumption          uint32
	ioSpeed                     uint32
}
