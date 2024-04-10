package ping

import (
	"github.com/cubefs/cubefs/util/ping/pingclient"
)

var defaultPingClient *pingclient.PingClient

func StartDefaultClient(getIpList func() ([]string, error)) (err error) {
	defaultPingClient = pingclient.NewPingClient(getIpList)
	err = defaultPingClient.Start()
	return
}

func StopDefaultClient() {
	defaultPingClient.Close()
}

func SortByDistanceASC(hosts []string) {
	defaultPingClient.SortByDistanceASC(hosts)
}

func SortByDistanceDESC(hosts []string) {
	defaultPingClient.SortByDistanceDESC(hosts)
}

func ForceRefreshPing() {
	defaultPingClient.ForceExecutePing()
}

func GetPingEnable() bool {
	return defaultPingClient.GetPingEnable()
}

func EnablePingSort(enable bool) {
	defaultPingClient.SetPingEnable(enable)
}
