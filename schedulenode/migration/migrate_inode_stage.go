package migration

type migrateInodeStage uint32

const (
	Init migrateInodeStage = iota
	OpenFile
	LookupEkSegment
	LockExtents
	CheckCanMigrate
	ReadAndWriteEkData
	MetaMergeExtents
	InodeMigStopped
)

func (s migrateInodeStage) String() string {
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
		return "ReadAndWriteEkData"
	case MetaMergeExtents:
		return "MetaMergeExtents"
	case InodeMigStopped:
		return "InodeMigStopped"
	}
	return ""
}
