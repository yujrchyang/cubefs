package intramig

type MigTaskStage uint8

const (
	GetMPInfo MigTaskStage = iota
	GetMNProfPort
	ListAllIno
	GetInodes
	WaitSubTask
	Stopped
)

func (s MigTaskStage) String() string {
	switch s {
	case GetMPInfo:
		return "GetMPInfo"
	case GetMNProfPort:
		return "GetMNProfPort"
	case ListAllIno:
		return "ListAllIno"
	case GetInodes:
		return "GetInodes"
	case WaitSubTask:
		return "WaitSubTask"
	case Stopped:
		return "Stopped"
	}
	return ""
}
