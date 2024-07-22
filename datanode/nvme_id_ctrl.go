package datanode

type nvmeIdCtrl struct {
	vId        uint16 //Only the first two bytes of vid are used, so there is no complete description of the struct. The vme_id_ctrl struct can be found in linux/nvme.h
	ignoreWord [2047]uint16
}
