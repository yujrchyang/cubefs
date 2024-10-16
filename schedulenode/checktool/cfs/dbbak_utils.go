package cfs

const (
	tinyExtentCountForDbbak   = 128
	tinyExtentStartIDForDbbak = 50000000
)

func isDbBackTinyExtent(extentID uint64) bool {
	return extentID >= tinyExtentCountForDbbak && extentID < tinyExtentCountForDbbak+tinyExtentStartIDForDbbak
}
