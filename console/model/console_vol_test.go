package model

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	a, _   = InitTestSreDB()
	table1 = VolumeSummaryView{}
	table2 = VolumeHistoryCurve{}
)

func TestLoadZombieVols(t *testing.T) {
	result, err := LoadZombieVols("mysql")
	assert.NoError(t, err)
	fmt.Printf("zombieVols: %v\n", result)
}

func TestZombieVolsDetails(t *testing.T) {
	result, err := LoadZombieVolDetails("spark", 1, 10)
	assert.NoError(t, err)
	fmt.Printf("zombieVolsDetails: %v\n", result)
}

func TestLoadNoDeleteVol(t *testing.T) {
	result, err := LoadNoDeleteVol("spark")
	assert.NoError(t, err)
	fmt.Printf("noDeletedVol: %v\n", result)
}

func TestNoDeletedVolDetails(t *testing.T) {
	result, err := LoadNoDeletedVolDetails("spark", 1, 10)
	assert.NoError(t, err)
	fmt.Printf("noDeletedVolDetails: %v\n", result)
}

func TestLoadInodesTopNVol(t *testing.T) {
	res, err := LoadInodeTopNVol("master-test", 10, "", "", 0)
	assert.NoError(t, err)
	assert.Equal(t, len(res), 10)

	res, err = LoadUsedGBTopNVol("master-test", 10, "", "", 0)
	assert.NoError(t, err)
	assert.Equal(t, len(res), 10)

	res, err = LoadInodeTopNVol("vcfsk", 10, "", "", 0)
	assert.Equal(t, 0, len(res))
}

func TestLoadVolumeHistoryData(t *testing.T) {
	end := time.Now()
	start := end.AddDate(0, 0, -15)
	result, err := LoadVolumeHistoryData("spark", "test_zwx", start, end)
	assert.NoError(t, err)
	fmt.Printf("result: %v", result)
}
