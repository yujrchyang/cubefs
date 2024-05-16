package scheduleTask

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var testRWorker = &RebalanceWorker{
	serverMap: map[string]string{
		"test":  "11.116.195.5:7000",
		"spark": "11.116.195.5:7000",
	},
}

func Test_GetTaskInfo(t *testing.T) {
	view, err := testRWorker.GetTaskInfo(85)
	assert.Nil(t, err)
	fmt.Printf("%v", view)
}
