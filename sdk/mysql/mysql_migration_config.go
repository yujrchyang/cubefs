package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"time"
)

const (
	ColumnId            = "id"
	ColumnSmart         = "smart"
	ColumnSmartRules    = "smart_rules"
	ColumnHddDirs       = "hdd_dirs"
	ColumnSsdDirs       = "ssd_dirs"
	ColumnMigrationBack = "migration_back"
	ColumnCompact       = "compact"
)

func AddMigrationConfig(vc *proto.MigrationConfig) (err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlAddMigrationConfig)
	defer metrics.Set(err)

	sqlCmd := "insert into migration_config(cluster_name, vol_name, smart, smart_rules, hdd_dirs, ssd_dirs, migration_back, compact) values(?, ?, ?, ?, ?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, vc.ClusterName)
	args = append(args, vc.VolName)
	args = append(args, vc.Smart)
	args = append(args, vc.SmartRules)
	args = append(args, vc.HddDirs)
	args = append(args, vc.SsdDirs)
	args = append(args, vc.MigrationBack)
	args = append(args, vc.Compact)

	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddMigrationConfig] add migration config failed, clusterName(%v), volName(%v), smart(%v), smartRules(%v) hddDirs(%v) ssdDirs(%v) migrationBack(%v) compact(%v) err(%v)",
			vc.ClusterName, vc.VolName, vc.Smart, vc.SmartRules, vc.HddDirs, vc.SsdDirs, vc.MigrationBack, vc.Compact, err)
		return err
	}
	return
}

func SelectAllMigrationConfig(limit, offset int) (volumeConfigs []*proto.MigrationConfig, err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlSelectMigrationConfig)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from migration_config limit ? offset ?", migrationConfigColumns())
	rows, err = db.Query(sqlCmd, limit, offset)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		vc := &proto.MigrationConfig{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&vc.Id, &vc.ClusterName, &vc.VolName, &vc.Smart, &vc.SmartRules, &vc.HddDirs, &vc.SsdDirs, &vc.MigrationBack, &vc.Compact, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		vc.CreateTime = createTime
		vc.UpdateTime = updateTime
		volumeConfigs = append(volumeConfigs, vc)
	}
	return
}

func SelectVolumeConfig(clusterName, volName string) (volumeConfigs []*proto.MigrationConfig, err error) {
	metrics := exporter.NewModuleTP(proto.MonitorMysqlSelectMigrationConfig)
	defer metrics.Set(err)

	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select %s from migration_config where cluster_name=? and vol_name=?", migrationConfigColumns())
	rows, err = db.Query(sqlCmd, clusterName, volName)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		vc := &proto.MigrationConfig{}
		var ct string
		var ut string
		var createTime time.Time
		var updateTime time.Time
		err = rows.Scan(&vc.Id, &vc.ClusterName, &vc.VolName, &vc.Smart, &vc.SmartRules, &vc.HddDirs, &vc.SsdDirs, &vc.MigrationBack, &vc.Compact, &ct, &ut)
		if err != nil {
			return
		}
		if createTime, err = FormatTime(ct); err != nil {
			return
		}
		if updateTime, err = FormatTime(ut); err != nil {
			return
		}
		vc.CreateTime = createTime
		vc.UpdateTime = updateTime
		volumeConfigs = append(volumeConfigs, vc)
	}
	return
}

func UpdateMigrationConfig(vc *proto.MigrationConfig) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewModuleTP(proto.MonitorMysqlUpdateMigrationConfig)
	defer metrics.Set(err)

	sqlCmd := "update migration_config set smart=?, smart_rules=?, hdd_dirs=?, ssd_dirs=?, migration_back=?, compact=? where id=?"
	args := make([]interface{}, 0)
	args = append(args, vc.Smart)
	args = append(args, vc.SmartRules)
	args = append(args, vc.HddDirs)
	args = append(args, vc.SsdDirs)
	args = append(args, vc.MigrationBack)
	args = append(args, vc.Compact)
	args = append(args, vc.Id)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[UpdateMigrationConfig] update migration config failed, id(%v) clusterName(%v), volName(%v), smart(%v), smartRules(%v) hddDirs(%v) ssdDirs(%v) migrationBack(%v) compact(%v) err(%v)",
			vc.Id, vc.ClusterName, vc.VolName, vc.Smart, vc.SmartRules, vc.HddDirs, vc.SsdDirs, vc.MigrationBack, vc.Compact, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("update migration config info has exception, affected rows less then one, id(%v) clusterName(%v), volName(%v)", vc.Id, vc.ClusterName, vc.VolName)
		return errors.New("affected rows less then one")
	}
	return
}

func DeleteMigrationConfig(cluster, volume string) (err error) {
	var (
		rs   sql.Result
		nums int64
	)
	metrics := exporter.NewModuleTP(proto.MonitorMysqlDeleteMigrationConfig)
	defer metrics.Set(err)
	sqlCmd := "delete from migration_config where cluster_name=? and vol_name=?"
	args := make([]interface{}, 0)
	args = append(args, cluster)
	args = append(args, volume)
	if rs, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete migration config failed, clusterName(%v), volName(%v), err(%v)",
			cluster, volume, err)
		return
	}
	if nums, err = rs.RowsAffected(); err != nil {
		return
	}
	if nums <= 0 {
		log.LogErrorf("delete migration config failed, affected rows less then one, clusterName(%v), volName(%v)",
			cluster, volume)
		return errors.New("affected rows less then one")
	}
	return
}

func migrationConfigColumns() (columns string) {
	columnSlice := make([]string, 0)
	columnSlice = append(columnSlice, ColumnId)
	columnSlice = append(columnSlice, ColumnClusterName)
	columnSlice = append(columnSlice, ColumnVolumeName)
	columnSlice = append(columnSlice, ColumnSmart)
	columnSlice = append(columnSlice, ColumnSmartRules)
	columnSlice = append(columnSlice, ColumnHddDirs)
	columnSlice = append(columnSlice, ColumnSsdDirs)
	columnSlice = append(columnSlice, ColumnMigrationBack)
	columnSlice = append(columnSlice, ColumnCompact)
	columnSlice = append(columnSlice, ColumnCreateTime)
	columnSlice = append(columnSlice, ColumnUpdateTime)
	return strings.Join(columnSlice, ",")
}
