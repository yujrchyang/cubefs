package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetNormalizedVolName(t *testing.T) {
	tests := []struct {
		name string
		vol  string
		want string
	}{
		{name: "not_mysql", vol: "a-b-c", want: "a-b-c"},
		{name: "coraldb_no_sharding", vol: "db-lf86a-0", want: "db"},
		{name: "coraldb_multi_shardings", vol: "db-1-lf86a-xx-02", want: "db-1"},
		{name: "jed_no_sharding", vol: "db-1-2-ht3e-0-rdonly-0", want: "db-1-2"},
		{name: "jed_multi_shardings", vol: "db-ht3e-xx-80-rdonly-0", want: "db"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, GetNormalizedVolName(test.vol))
		})
	}
}
