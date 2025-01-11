package migration

type MigrateDirection uint8

const (
	NoneFileMigrate MigrateDirection = iota
	SSDToHDDFileMigrate
	HDDToSSDFileMigrate
	CompactFileMigrate
	S3FileMigrate
	ReverseS3FileMigrate // s3回迁
	InvalidMigrateDirection
)

const (
	NoneMediumType = "none"
)

func (m MigrateDirection) String() string {
	switch m {
	case NoneFileMigrate:
		return "NoneFileMigrate"
	case SSDToHDDFileMigrate:
		return "SSDToHDDFileMigrate"
	case HDDToSSDFileMigrate:
		return "HDDToSSDFileMigrate"
	case CompactFileMigrate:
		return "CompactFileMigrate"
	case S3FileMigrate:
		return "S3FileMigrate"
	case ReverseS3FileMigrate:
		return "ReverseS3FileMigrate"
	}
	return "invalid migrate direction"
}
