package main

import (
	"fmt"

	"github.com/cubefs/cubefs/client/util"
)

const (
	ump_cfs_close = iota
	ump_cfs_open
	ump_cfs_rename
	ump_cfs_truncate
	ump_cfs_ftruncate
	ump_cfs_fallocate
	ump_cfs_posix_fallocate
	ump_cfs_flush
	ump_cfs_flush_redolog
	ump_cfs_flush_binlog
	ump_cfs_flush_relaylog
	ump_cfs_mkdirs
	ump_cfs_rmdir
	ump_cfs_link
	ump_cfs_symlink
	ump_cfs_unlink
	ump_cfs_readlink
	ump_cfs_stat
	ump_cfs_stat64
	ump_cfs_chmod
	ump_cfs_fchmod
	ump_cfs_chown
	ump_cfs_fchown
	ump_cfs_utimens
	ump_cfs_faccessat
	ump_cfs_read
	ump_cfs_read_binlog
	ump_cfs_read_relaylog
	ump_cfs_write
	ump_cfs_write_redolog
	ump_cfs_write_binlog
	ump_cfs_write_relaylog
)

func (c *client) initUmpKeys(isMysql bool) {
	volName := c.volName
	if isMysql {
		volName = util.GetNormalizedVolName(c.volName)
	}
	volKeyPrefix := fmt.Sprintf("%s_%s_", c.mw.Cluster(), volName)
	clusterKeyPrefix := fmt.Sprintf("%s_%s_", c.mw.Cluster(), gClientManager.moduleName)
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_close", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_open", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_rename", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_truncate", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_ftruncate", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_fallocate", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_posix_fallocate", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_flush", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_flush_redolog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_flush_binlog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_flush_relaylog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_mkdirs", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_rmdir", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_link", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_symlink", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_unlink", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_readlink", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_stat", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_stat64", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_chmod", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_fchmod", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_chown", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_fchown", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_utimens", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_faccessat", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_read", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_read_binlog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_read_relaylog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_write", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_write_redolog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_write_binlog", volKeyPrefix))
	c.umpKeyVolArr = append(c.umpKeyVolArr, fmt.Sprintf("%vcfs_write_relaylog", volKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_close", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_open", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_rename", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_truncate", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_ftruncate", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_fallocate", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_posix_fallocate", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_flush", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_redolog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_binlog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_flush_relaylog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_mkdirs", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_rmdir", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_link", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_symlink", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_unlink", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_readlink", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_stat", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_stat64", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_chmod", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_fchmod", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_chown", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_fchown", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_utimens", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_faccessat", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_read", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_read_binlog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_read_relaylog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_write", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_write_redolog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_write_binlog", clusterKeyPrefix))
	c.umpKeyClusterArr = append(c.umpKeyClusterArr, fmt.Sprintf("%vcfs_write_relaylog", clusterKeyPrefix))
}

func (c *client) umpFunctionKeyFast(act int) string {
	return c.umpKeyVolArr[act]
}

func (c *client) umpFunctionGeneralKeyFast(act int) string {
	return c.umpKeyClusterArr[act]
}
