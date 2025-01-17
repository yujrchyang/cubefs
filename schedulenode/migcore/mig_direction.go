package migration

type MigDirection uint8

const (
	NoneFileMigrate MigDirection = iota
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

func (m MigDirection) String() string {
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
