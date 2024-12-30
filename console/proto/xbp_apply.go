package proto

const (
	XbpParamType      = "type"   // 0-审批单
	XbpParamStatus    = "status" // 0-进行中 1-已结单 2-已撤回 -1-被驳回
	XbpParamTicketID  = "ticketId"
	XbpParamDraftID   = "draftId"
	XbpParamToken     = "token"
	XbpParamTimestamp = "timestamp" //ms级

	XbpCreateTicketPath = "/api/api/ticket/create"
)

// xbp表单key
const (
	XbpClusterKey   = "集群"
	XbpTypeKey      = "操作类型"
	XbpModuleKey    = "模块"
	XbpOperationKey = "配置项"
	XbpParamsKey    = "参数项"
	XbpVolumeKey    = "卷名"
)

const (
	XBP_InProcessing = iota
	XBP_Approved
	XBP_Withdrawn
	XBP_UnderDeploy
	XBP_DelpoyFailed
	XBP_Reject = -1
)

func GetXbpStatusMessage(status int) string {
	switch status {
	case XBP_InProcessing:
		return "申请处理中"
	case XBP_Approved:
		return "已审批通过"
	case XBP_Withdrawn:
		return "申请已撤销"
	case XBP_UnderDeploy:
		return "操作执行中"
	case XBP_DelpoyFailed:
		return "操作执行失败"
	case XBP_Reject:
		return "申请被驳回"
	}
	return ""
}

// todo：需要response 0,否则会再次请求回调
type XbpCreateResponse struct {
	TicketID uint64 `json:"ticketId"`
}

type XBPApply struct {
	ProcessID       uint64                 `json:"processId"`
	UserPin         string                 `json:"username"`
	ApplicationInfo map[string]interface{} `json:"applicationInfo"`
}

func NewXBPApply(processID uint64, createBy string) *XBPApply {
	return &XBPApply{
		ProcessID:       processID,
		UserPin:         createBy,
		ApplicationInfo: make(map[string]interface{}),
	}
}

func (apply *XBPApply) Add(key string, value interface{}) {
	apply.ApplicationInfo[key] = value
}
