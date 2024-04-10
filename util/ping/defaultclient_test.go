package ping

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPingClient(t *testing.T) {
	_, _ = log.InitLog("/tmp/utiltest", "pingclient", log.DebugLevel, nil)
	defer log.LogFlush()
	localhost := "127.0.0.1:17030"
	unkown := "1.2.3.4:17030"
	unkown2 := "1.2.3.5:17030"
	hosts := []string{unkown, localhost}
	var getHostFunc = func() ([]string, error) {
		return hosts, nil
	}
	err := StartDefaultClient(getHostFunc)
	assert.NoError(t, err)

	newHosts := []string{unkown, localhost}
	SortByDistanceASC(newHosts)
	//does not ping yet
	assert.Equal(t, strings.Join(hosts, ";"), strings.Join(newHosts, ";"))

	//execute ping
	ForceRefreshPing()

	//asc
	newHosts = []string{unkown, localhost}
	EnablePingSort(false)
	SortByDistanceASC(newHosts)
	assert.Equal(t, strings.Join(hosts, ";"), strings.Join(newHosts, ";"))

	EnablePingSort(true)
	SortByDistanceASC(newHosts)
	assert.Equal(t, fmt.Sprintf("%v;%v", localhost, unkown), strings.Join(newHosts, ";"))

	//desc
	newHosts = []string{unkown, localhost}
	SortByDistanceDESC(newHosts)
	assert.Equal(t, fmt.Sprintf("%v;%v", unkown, localhost), strings.Join(hosts, ";"))

	newHosts = []string{unkown, localhost, localhost, unkown2}
	SortByDistanceASC(newHosts)
	assert.Equal(t, localhost, newHosts[0])
	assert.Equal(t, localhost, newHosts[1])

	newHosts = []string{unkown, localhost, localhost, unkown2}
	SortByDistanceDESC(newHosts)
	assert.Equal(t, localhost, newHosts[2])
	assert.Equal(t, localhost, newHosts[3])

	StopDefaultClient()
}
