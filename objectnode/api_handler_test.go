package objectnode

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestIsRequestNetError(t *testing.T) {

	type __sample struct {
		err    error
		req    *http.Request
		result bool
	}

	var samples = []__sample{
		{err: nil, req: nil, result: false},
		{err: errors.New("test"), req: nil, result: false},
		{err: errors.New("test"), req: &http.Request{}, result: false},
		{err: errors.New("write tcp 11.62.96.18:1601->172.20.17.206:65234: write: broken pipe"), req: &http.Request{RemoteAddr: "172.20.17.206:65234"}, result: true},
		{err: errors.New("write tcp 11.62.96.18:1601->172.20.17.206:65234: write: broken pipe"), req: &http.Request{RemoteAddr: "172.20.17.6:65234"}, result: false},
		{err: errors.New("write tcp 11.62.96.18:1601->172.20.17.199:46361: write: connection reset by peer"), req: &http.Request{RemoteAddr: "172.20.17.199:46361"}, result: true},
		{err: errors.New("write tcp 11.62.96.18:1601->172.20.17.199:46361: write: connection reset by peer"), req: &http.Request{RemoteAddr: "172.20.17.199:4631"}, result: false},
		{err: errors.New("read tcp 11.62.96.18:1601->172.20.17.222:52489: read: connection reset by peer"), req: &http.Request{RemoteAddr: "172.20.17.222:52489"}, result: true},
		{err: errors.New("read tcp 11.62.96.18:1601->172.20.17.222:52489: read: connection reset by peer"), req: &http.Request{RemoteAddr: "172.20.17.220:5489"}, result: false},
	}

	for i, s := range samples {
		t.Run(fmt.Sprintf("Case_%02d", i), func(t *testing.T) {
			if actual := isRequestNetError(s.req, s.err); actual != s.result {
				t.Fatalf("result mismatch: expected(%v) actual(%v)", s.result, actual)
			}
		})
	}
}