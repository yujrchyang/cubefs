package traffic

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getVolSourceAndPinInfo(t *testing.T) {
	_, err := InitTestMysqlDB()
	assert.Nil(t, err)
	vols, err := getVolSourceAndPinInfo("spark")
	assert.Nil(t, err)
	if len(vols) == 0 {
		fmt.Println("vol len = 0")
	}
}
