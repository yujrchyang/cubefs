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
