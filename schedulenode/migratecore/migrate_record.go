package migration

type MigrateRecord struct {
	MigCnt      uint64
	MigInodeCnt uint64
	MigEkCnt    uint64
	NewEkCnt    uint64
	MigSize     uint64
	MigErrCode  int
	MigErrCnt   uint64
	MigErrMsg   string
}
