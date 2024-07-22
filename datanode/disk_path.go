package datanode

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	regexpDiskPath = regexp.MustCompile("^(/(\\w|-)+)+(:(\\d)+)?$")
)

type DiskPath struct {
	path     string
	reserved uint64
}

func (p *DiskPath) Path() string {
	return p.path
}

func (p *DiskPath) Reserved() uint64 {
	return p.reserved
}

func (p *DiskPath) SetReserved(reserved uint64) {
	p.reserved = reserved
}

func (p *DiskPath) String() string {
	return fmt.Sprintf("DiskPath(path=%v, reserved=%v)", p.path, p.reserved)
}

func ParseDiskPath(str string) (p *DiskPath, success bool) {
	if !regexpDiskPath.MatchString(str) {
		return
	}
	var parts = strings.Split(str, ":")
	p = &DiskPath{
		path: parts[0],
		reserved: func() uint64 {
			if len(parts) > 1 {
				var val, _ = strconv.ParseUint(parts[1], 10, 64)
				return val
			}
			return 0
		}(),
	}
	success = true
	return
}
