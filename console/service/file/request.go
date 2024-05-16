package file

import (
	"encoding/json"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
)

const (
	GetDeletedDentrysPath = "/getDeletedDentrys"
)

type deletedDentry struct {
	ParentId  uint64 `json:"pid"`
	Name      string `json:"name"`
	Inode     uint64 `json:"ino"`
	Type      uint32 `json:"type"`
	Timestamp int64  `json:"ts"`
	From      string `json:"from"`
}

func getDeletedDentrysReq(addr string, port string, pid uint64, parentIno uint64, batch int64, prev string, ts int64) *cutil.HttpRequest {
	addr = strings.Split(addr, ":")[0]
	path := "http://" + addr + ":" + port + GetDeletedDentrysPath
	var request = cutil.NewAPIRequest(http.MethodGet, path)
	request.AddParam("pid", strconv.FormatUint(pid, 10))
	request.AddParam("pIno", strconv.FormatUint(parentIno, 10))
	request.AddParam("batch", strconv.FormatInt(batch, 10))
	request.AddParam("prev", prev)
	if ts != 0 {
		request.AddParam("ts", strconv.FormatInt(ts, 10))
	}
	return request
}

func getAllDeletedDentrysByParentIno(host, port string, pid, pIno uint64, prev *proto.DeletedDentry) (ddentrys []*proto.DeletedDentry, err error) {
	var (
		token string
		ts    int64
		ddens []*deletedDentry
	)

	if prev != nil {
		token = prev.Name
		ts = prev.Timestamp
	}
	req := getDeletedDentrysReq(host, port, pid, pIno, 0, token, ts)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("getAllDeletedDentrysByParentIno: send request failed, err(%v)", err)
		return
	}
	if err = json.Unmarshal(resp, &ddens); err != nil {
		log.LogErrorf("getAllDeletedDentrysByParentIno: unmarshal failed, err(%v)", err)
		return
	}
	ddentrys = make([]*proto.DeletedDentry, 0)
	for _, dden := range ddens {
		ddentry := &proto.DeletedDentry{
			ParentID:  dden.ParentId,
			Name:      dden.Name,
			Inode:     dden.Inode,
			Type:      btoi(proto.IsDir(dden.Type)),
			Timestamp: dden.Timestamp,
			From:      strings.Split(dden.From, "_")[0],
		}
		ddentrys = append(ddentrys, ddentry)
	}
	return
}

func btoi(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
