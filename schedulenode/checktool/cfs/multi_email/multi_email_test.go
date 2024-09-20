package multi_email

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestEmail(t *testing.T) {
	t.Skipf("skip email test")
	InitMultiMail(25, "mx.jd.local", "storage-sre@jd.com", "storage-sre", "******", []string{"xuxihao3@jd.com"})
	elements := make([][]string, 0)
	elements = append(elements, []string{
		"ID",
		"Name",
	})
	for i := 0; i < 200; i++ {
		elements = append(elements, []string{
			strconv.Itoa(i),
			fmt.Sprintf("joy-%v", i+10000),
		})
	}
	err := Email("subject: test send email", "this is the summary", elements)
	assert.NoError(t, err)
}
