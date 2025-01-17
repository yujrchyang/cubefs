package migcore

type MigInodeStage uint32

const (
	Init MigInodeStage = iota
	OpenFile
	LookupEkSegment
	LockExtents
	CheckCanMigrate
	ReadAndWriteEkData
	MetaMergeExtents
	InodeMigStopped
)

func (s MigInodeStage) String() string {
	switch s {
	case Init:
		return "Init"
	case OpenFile:
		return "OpenFile"
	case LookupEkSegment:
		return "LookupEkSegment"
	case LockExtents:
		return "LockExtents"
	case CheckCanMigrate:
		return "CheckCanMigrate"
	case ReadAndWriteEkData:
		return "ReadAndWriteDataNode"
	case MetaMergeExtents:
		return "MetaMergeExtents"
	case InodeMigStopped:
		return "InodeMigStopped"
	}
	return ""
}
