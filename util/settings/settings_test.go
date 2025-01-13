package settings

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func TestSettings(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)
	t.Logf("using tmp dir: %s", tmpDir)

	settings, err := OpenKeyValues(path.Join(tmpDir, "data_settings.json"))
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, settings)

	sets := make(map[string]interface{})
	sets["set1"] = false
	sets["set2"] = true
	sets["set3"] = "test"
	sets["set4"] = 1024
	sets["set5"] = 0.1

	// init settings
	for k, v := range sets {
		err = settings.Set(k, fmt.Sprintf("%v", v))
		assert.NoError(t, err)
	}

	// verify settings
	settings.Walk(func(key, value string) bool {
		if _, ok := sets[key]; ok {
			assert.Equal(t, fmt.Sprintf("%v", sets[key]), value)
		}
		return true
	})

	t.Run("Get No Exists", func(t *testing.T) {
		res, exist := settings.Get("set-n")
		assert.False(t, exist)
		assert.Equal(t, "", res)
	})

	t.Run("Get-SetBool", func(t *testing.T) {
		err = settings.SetBool("set-bool", true)
		assert.NoError(t, err)
		res, exist := settings.GetBool("set-bool")
		assert.True(t, exist)
		assert.Equal(t, true, res)

		res2, exist := settings.GetBool("set-bool")
		assert.True(t, exist)
		assert.True(t, res2)

		res3, exist := settings.Get("set-bool")
		assert.True(t, exist)
		assert.Equal(t, "true", res3)
	})

	t.Run("Del", func(t *testing.T) {
		for k, v := range sets {
			pref, ok := settings.Del(k)
			assert.True(t, ok)
			assert.Equal(t, fmt.Sprintf("%v", v), pref)
		}
		count := 0
		settings.Walk(func(key, value string) bool {
			if _, ok := sets[key]; ok {
				count++
			}
			return true
		})
		assert.Equal(t, 0, count)
	})

	t.Run("set empty", func(t *testing.T) {
		err = settings.Set("set-empty", "true")
		assert.NoError(t, err)
		res, exist := settings.Get("set-empty")
		assert.True(t, exist)
		assert.Equal(t, "true", res)

		err = settings.Set("set-empty", "")
		assert.NoError(t, err)

		res, exist = settings.Get("set-empty")
		assert.False(t, exist)
		assert.Equal(t, "", res)
	})

	t.Run("rollback on modiry", func(t *testing.T) {
		os.RemoveAll(tmpDir)
		err = settings.SetBool("set-bool", false)
		assert.Error(t, err)
		assert.Equal(t, true, os.IsNotExist(err))
		res, exist := settings.GetBool("set-bool")
		assert.True(t, exist)
		assert.Equal(t, true, res)
	})

	t.Run("rollback on delete", func(t *testing.T) {
		os.RemoveAll(tmpDir)
		err = settings.Set("set-roolback-del", "false")
		assert.Error(t, err)
		assert.Equal(t, true, os.IsNotExist(err))
		res, exist := settings.Get("set-roolback-del")
		assert.False(t, exist)
		assert.Equal(t, "", res)
	})
}
