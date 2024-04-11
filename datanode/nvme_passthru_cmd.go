package datanode

import (
	"syscall"
	"unsafe"
)

type nvmePassthruCmd struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2        uint32
	cdw3        uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMs   uint32
	result      uint32
}

func nvmeAdminPassthru(fd int, cmd nvmePassthruCmd) error {
	_, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), NVME_IOCTL_ADMIN_CMD, uintptr(unsafe.Pointer(&cmd)))
	if err != 0 {
		return syscall.Errno(err)
	}
	return nil
}

