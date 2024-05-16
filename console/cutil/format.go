package cutil

import "fmt"

var units = []string{"B", "KB", "MB", "GB", "TB", "PB"}
var step uint64 = 1024

func fixUnit(curSize uint64, curUnitIndex int) (newSize uint64, newUnitIndex int) {
	if curSize >= step && curUnitIndex < len(units)-1 {
		return fixUnit(curSize/step, curUnitIndex+1)
	}
	return curSize, curUnitIndex
}

func FormatSize(size uint64) string {
	fixedSize, fixedUnitIndex := fixUnit(size, 0)
	return fmt.Sprintf("%v %v", fixedSize, units[fixedUnitIndex])
}

func FormatRatio(ratio float64) string {
	return fmt.Sprintf("%.2f%%", ratio*100)
}

func IntToBool(n int) bool {
	// 0 - false
	// 1 - true
	return n != 0
}

func Paginate[T any](pageNum, pageSize int, lists []T) []T {
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	total := len(lists)
	if pageSize >= total {
		return lists
	}
	start := (pageNum - 1) * pageSize
	end := pageNum * pageSize
	if end > total {
		end = total
	}
	return lists[start:end]
}
