package migration

type MigrateDirection uint8

const (
	SSDTOHDDFILEMIGRATE MigrateDirection = iota
	HDDTOSSDFILEMIGRATE
	COMPACTFILEMIGRATE
	S3FILEMIGRATE
)

const (
	NoneMediumType = "none"
)

func (m MigrateDirection) String() string {
	switch m {
	case SSDTOHDDFILEMIGRATE:
		return "SsdToHddFileMigrate"
	case HDDTOSSDFILEMIGRATE:
		return "HddToSsdFileMigrate"
	case COMPACTFILEMIGRATE:
		return "CompactFileMigrate"
	}
	return "S3FileMigrate"
}
