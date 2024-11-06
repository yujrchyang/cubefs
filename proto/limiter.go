package proto

import math "math"

// normal limit opcode start from 512 to 1023, total count 512
const (
	OpExtentRepairWrite_ = iota + 512
	OpFlushDelete_
	OpExtentRepairWriteToApplyTempFile_
	OpExtentRepairWriteByPolicy_
	OpExtentRepairReadToRollback_
	OpExtentRepairReadToComputeCrc_
	OpExtentReadToGetCrc_
	OpFetchDataPartitionView_
	OpFixIssueFragments_
	OpPlaybackTinyDelete_
	OpRecoverTrashExtent_
)

type RequestSource uint8

const (
	SourceBase RequestSource = iota
	SourceFlashNode
)

// read source opcode start from 1024 to 1279, total count 256
const (
	OpStreamFollowerReadSrcBase_      = 1024
	OpStreamFollowerReadSrcFlashNode_ = OpStreamFollowerReadSrcBase_ - 1 + int(SourceFlashNode)
)

const (
	RateLimit       = "rate limit"
	ConcurrentLimit = "concurrent limit"
)

func GetOpMsgExtend(opcode int) (m string) {
	if opcode <= math.MaxUint8 {
		return GetOpMsg(uint8(opcode))
	}

	switch opcode {
	case OpExtentRepairWrite_:
		m = "OpExtentRepairWrite_"
	case OpExtentRepairWriteToApplyTempFile_:
		m = "OpExtentRepairWriteToApplyTempFile_"
	case OpExtentRepairWriteByPolicy_:
		m = "OpExtentRepairWriteByPolicy_"
	case OpExtentRepairReadToRollback_:
		m = "OpExtentRepairReadToRollback_"
	case OpExtentRepairReadToComputeCrc_:
		m = "OpExtentRepairReadToComputeCrc_"
	case OpExtentReadToGetCrc_:
		m = "OpExtentReadToGetCrc_"
	case OpFlushDelete_:
		m = "OpFlushDelete_"
	case OpFetchDataPartitionView_:
		m = "OpFetchDataPartitionView_"
	case OpFixIssueFragments_:
		m = "OpFixIssueFragments_"
	case OpPlaybackTinyDelete_:
		m = "OpPlaybackTinyDelete_"
	case OpStreamFollowerReadSrcFlashNode_:
		m = "OpStreamFollowerReadSrcFlashNode_"
	case OpRecoverTrashExtent_:
		m = "OpRecoverTrashExtent_"
	}
	return m
}

func GetOpCodeExtend(m string) (opcode int) {
	switch m {
	case "OpExtentRepairWrite_":
		opcode = OpExtentRepairWrite_
	case "OpExtentRepairWriteToApplyTempFile_":
		opcode = OpExtentRepairWriteToApplyTempFile_
	case "OpExtentRepairWriteByPolicy_":
		opcode = OpExtentRepairWriteByPolicy_
	case "OpExtentRepairReadToRollback_":
		opcode = OpExtentRepairReadToRollback_
	case "OpExtentRepairReadToComputeCrc_":
		opcode = OpExtentRepairReadToComputeCrc_
	case "OpExtentReadToGetCrc_":
		opcode = OpExtentReadToGetCrc_
	case "OpFlushDelete_":
		opcode = OpFlushDelete_
	case "OpFetchDataPartitionView_":
		opcode = OpFetchDataPartitionView_
	case "OpFixIssueFragments_":
		opcode = OpFixIssueFragments_
	case "OpPlaybackTinyDelete_":
		opcode = OpPlaybackTinyDelete_
	case "OpStreamFollowerReadSrcFlashNode_":
		opcode = OpStreamFollowerReadSrcFlashNode_
	case "OpRecoverTrashExtent_":
		opcode = OpRecoverTrashExtent_
	}
	return opcode
}

func GetOpByReadSource(originOp uint8, source RequestSource) int {
	if source == SourceBase {
		return int(originOp)
	}
	switch originOp {
	case OpStreamFollowerRead:
		return OpStreamFollowerReadSrcBase_ - 1 + int(source)
	default:
		return int(originOp)
	}
}
