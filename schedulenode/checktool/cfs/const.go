package cfs

import "time"

const (
	maxWarnInterval = time.Minute * 10
)

const (
	dataNodeAliveRetryToken = "dataNodeAliveRetryToken"
	metaNodeAliveRetryToken = "metaNodeAliveRetryToken"
	resetDbBackRecoverToken = "resetDbBackRecoverToken"
)

const (
	ClusterNameSpark = "spark"
	ClusterNameMysql = "mysql"
	ClusterNameDbbak = "cfs_dbBack"
	ClusterNameAMS   = "cfs_AMS_MCA"
	DomainSpark      = "cn.chubaofs.jd.local"
	DomainMysql      = "cn.elasticdb.jd.local"
	DomainDbbak      = "cn.chubaofs-seqwrite.jd.local"
	DomainNL         = "nl.chubaofs.jd.local"
	DomainOchama     = "nl.chubaofs.ochama.com"
)
