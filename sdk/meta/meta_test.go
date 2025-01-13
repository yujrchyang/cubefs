package meta

import (
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestMetaRateLimit(t *testing.T) {
	file := "TestMetaRateLimit"
	create(file)
	// limited by 10 op/s for looking up
	limit := map[string]string{proto.VolumeKey: ltptestVolume, proto.OpcodeKey: strconv.Itoa(int(proto.OpMetaLookup)), proto.ClientVolOpRateKey: "10"}
	err := mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	mw.updateLimiterConfig(true)
	assert.Equal(t, rate.Limit(10), mw.getOpLimiter(proto.OpMetaLookup).Limit())
	// consume burst first
	for i := 0; i < 10; i++ {
		mw.Lookup_ll(nil, proto.RootIno, file)
	}
	begin := time.Now()
	for i := 0; i < 11; i++ {
		mw.Lookup_ll(nil, proto.RootIno, file)
	}
	cost := time.Since(begin)
	assert.True(t, cost > time.Second)

	// not limited for looking up
	limit[proto.ClientVolOpRateKey] = "0"
	err = mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	mw.updateLimiterConfig(true)
	lookupLimiter := mw.getOpLimiter(proto.OpMetaLookup)
	assert.True(t, lookupLimiter == nil || lookupLimiter.Limit() == rate.Inf)
	begin = time.Now()
	for i := 0; i < 20; i++ {
		mw.Lookup_ll(nil, proto.RootIno, file)
	}
	cost = time.Since(begin)
	assert.True(t, cost < 5*time.Millisecond)
}
