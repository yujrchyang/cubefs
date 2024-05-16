package migration

const (
	_ = iota
	InodeOpenFailedCode
	InodeInitTaskCode
	InodeReadFailedCode
	InodeMergeFailedCode
	InodeLookupEkCode
	InodeLockExtentFailedCode
	InodeCheckInodeFailedCode
)

const (
	InodeOpenFailed         = "open file failed"
	InodeInitTaskFailed     = "init task failed"
	InodeReadAndWriteFailed = "read and write data failed"
	InodeMergeFailed        = "send meta merge failed"
	InodeLookupEkFailed     = "lookup extent failed"
	InodeLockExtentFailed   = "lock extent failed"
	InodeUnlockExtentFailed = "unlock extent failed"
	InodeCheckInodeFailed   = "check inode failed"
)

const (
	ClusterKey   = "cluster"
	VolNameKey   = "vol"
	LimitSizeKey = "limit"
	MpIdKey      = "mp"
	InodeIdKey   = "inode"
)

const (
	Compact = "compact"
	FileMig = "fileMig"
)

const (
	ConsumeTask    = "ConsumeTask"
	RunMpTask      = "RunMpTask"
	RunInodeTask   = "RunInodeTask"
)
