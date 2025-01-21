package meta

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"golang.org/x/net/context"
)

var cfg *MetaConfig = &MetaConfig{
	Volume:        "ltptest",
	Masters:       []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"},
	ValidateOwner: true,
	Owner:         "ltptest",
}

func TestMetaAPI(t *testing.T) {
	ctx := context.Background()
	var (
		mw   *MetaWrapper
		err  error
		dir1 = "test1"
		dir2 = "test2"
		dir3 = "test.txt"
	)
	mw, err = NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("creat meta wrapper failed!")
	}
	// make directory: 1.not exit  2.exit
	dir1Inode, err := mw.makeDirectory(proto.RootIno, dir1)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir1)
	}
	dir2Inode, err := mw.makeDirectory(dir1Inode, dir2)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir2)
	}
	dir2Inode, err = mw.makeDirectory(dir1Inode, dir2)
	if err != nil {
		t.Errorf("make directory(%v) failed", dir2)
	}
	// make directory at wrong rootIno
	if _, err = mw.makeDirectory(0, dir3); err == nil {
		t.Errorf("make directory(%v) failed", dir3)
	}
	// look up path
	absPath := strings.Join([]string{dir1, dir2}, "/")
	inode, err := mw.LookupPath(ctx, proto.RootIno, absPath)
	if err != nil {
		t.Errorf("look up path err(%v)", err)
	}
	if inode != dir2Inode {
		t.Errorf("look up inode is not equal, get(%v), want(%v)", inode, dir2Inode)
	}
	// get root inode: 1.normal 2.has file
	rootInode, err := mw.GetRootIno(absPath, false)
	if err != nil {
		t.Errorf("get root inode failed: err(%v), dir(%v)", err, absPath)
	}
	if rootInode != dir2Inode {
		t.Errorf("get root inode is wrong")
	}
	filePath := strings.Join([]string{"/cfs/mnt", absPath, dir3}, "/")
	if _, err := os.Create(filePath); err != nil {
		t.Fatalf("create file(%v) failed err(%v)", err, filePath)
	}
	_, err = mw.GetRootIno(filePath, false)
	if err == nil {
		t.Errorf("get root inode failed: err(%v), dir(%v)", err, filePath)
	}
	// make directory at file
	if _, err = mw.makeDirectory(dir2Inode, dir3); err == nil {
		t.Errorf("make directory(%v) failed", dir3)
	}
	// clean test file
	err = os.RemoveAll(strings.Join([]string{"/cfs/mnt", dir1}, "/"))
	if err != nil {
		t.Errorf("TestMetaAPI: clean test dir(%v) failed, err(%v)", dir1, err)
	}
}

func TestWalkDirWithFilter(t *testing.T) {
	ctx := context.Background()
	var (
		mw    *MetaWrapper
		err   error
		dir1  = "test1"
		dir2  = "test2"
		dir3  = "test3"
		file1 = "test.txt"
		file2 = "test.log"
		file3 = "test.md"
	)

	mw, err = NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("create meta wrapper failed: %v", err)
	}

	// 创建测试目录结构
	dir1Inode, err := mw.makeDirectory(proto.RootIno, dir1)
	if err != nil {
		t.Fatalf("make directory(%v) failed: %v", dir1, err)
	}

	dir2Inode, err := mw.makeDirectory(dir1Inode, dir2)
	if err != nil {
		t.Fatalf("make directory(%v) failed: %v", dir2, err)
	}

	dir3Inode, err := mw.makeDirectory(dir2Inode, dir3)
	if err != nil {
		t.Fatalf("make directory(%v) failed: %v", dir3, err)
	}

	// 创建测试文件
	_, err = mw.Create_ll(ctx, dir2Inode, file1, 0644, 0, 0, nil)
	if err != nil {
		t.Fatalf("create file(%v) failed: %v", file1, err)
	}

	_, err = mw.Create_ll(ctx, dir2Inode, file2, 0644, 0, 0, nil)
	if err != nil {
		t.Fatalf("create file(%v) failed: %v", file2, err)
	}

	_, err = mw.Create_ll(ctx, dir3Inode, file3, 0644, 0, 0, nil)
	if err != nil {
		t.Fatalf("create file(%v) failed: %v", file3, err)
	}

	var matchedPaths []string
	// 定义过滤函数，只处理 .txt 文件
	filterTxtFiles := func(absolutePath string, dentry proto.Dentry) bool {
		ok := strings.HasSuffix(dentry.Name, ".txt")
		if ok {
			matchedPaths = append(matchedPaths, absolutePath)
		}
		return ok
	}

	// 定义过滤函数，只处理 .log 文件
	filterLogFiles := func(absolutePath string, dentry proto.Dentry) bool {
		ok := strings.HasSuffix(dentry.Name, ".log")
		if ok {
			matchedPaths = append(matchedPaths, absolutePath)
		}
		return ok
	}

	// 定义过滤函数，处理所有文件
	filterAllFiles := func(absolutePath string, dentry proto.Dentry) bool {
		matchedPaths = append(matchedPaths, absolutePath)
		return true
	}

	// 场景 1: 过滤 .txt 文件
	t.Run("FilterTxtFiles", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		err = mw.WalkDirWithFilter(proto.RootIno, "", "", "/", 100, filterTxtFiles)
		if err != nil {
			t.Fatalf("WalkDir failed: %v", err)
		}

		expectedPaths := []string{"/test1/test2/test.txt"}
		if len(matchedPaths) != len(expectedPaths) || matchedPaths[0] != expectedPaths[0] {
			t.Errorf("FilterTxtFiles result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
		}
	})

	// 场景 2: 过滤 .log 文件
	t.Run("FilterLogFiles", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		err = mw.WalkDirWithFilter(proto.RootIno, "", "", "/", 100, filterLogFiles)
		if err != nil {
			t.Fatalf("WalkDir failed: %v", err)
		}

		expectedPaths := []string{"/test1/test2/test.log"}
		if len(matchedPaths) != len(expectedPaths) || matchedPaths[0] != expectedPaths[0] {
			t.Errorf("FilterLogFiles result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
		}
	})

	// 场景 3: 过滤所有文件
	t.Run("FilterAllFiles", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		err = mw.WalkDirWithFilter(proto.RootIno, "", "", "/", 100, filterAllFiles)
		if err != nil {
			t.Fatalf("WalkDir failed: %v", err)
		}

		expectedPaths := []string{
			"/test1/test2/test.txt",
			"/test1/test2/test.log",
			"/test1/test2/test3/test.md",
		}
		if len(matchedPaths) != len(expectedPaths) {
			t.Errorf("FilterAllFiles result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
		} else {
			for i, path := range matchedPaths {
				if path != expectedPaths[i] {
					t.Errorf("FilterAllFiles result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
					break
				}
			}
		}
	})

	// 场景 4: 空目录
	t.Run("EmptyDirectory", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		err = mw.WalkDirWithFilter(dir3Inode, "", "", "/test1/test2/test3", 100, filterAllFiles)
		if err != nil {
			t.Fatalf("WalkDir failed: %v", err)
		}

		expectedPaths := []string{"/test1/test2/test3/test.md"}
		if len(matchedPaths) != len(expectedPaths) || matchedPaths[0] != expectedPaths[0] {
			t.Errorf("EmptyDirectory result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
		}
	})

	// 场景 5: 不存在的目录
	t.Run("NonExistentDirectory", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		err = mw.WalkDirWithFilter(999999, "", "", "/nonexistent", 100, filterAllFiles)
		if err == nil {
			t.Errorf("WalkDir should fail for non-existent directory")
		}
	})

	// 场景 6: 过滤目录
	t.Run("FilterDirectories", func(t *testing.T) {
		matchedPaths = nil // 重置记录
		filterDirs := func(absolutePath string, dentry proto.Dentry) bool {
			return dentry.Type == uint32(os.ModeDir)
		}
		err = mw.WalkDirWithFilter(proto.RootIno, "", "", "/", 100, filterDirs)
		if err != nil {
			t.Fatalf("WalkDir failed: %v", err)
		}

		expectedPaths := []string{
			"/test1",
			"/test1/test2",
			"/test1/test2/test3",
		}
		if len(matchedPaths) != len(expectedPaths) {
			t.Errorf("FilterDirectories result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
		} else {
			for i, path := range matchedPaths {
				if path != expectedPaths[i] {
					t.Errorf("FilterDirectories result mismatch, got: %v, want: %v", matchedPaths, expectedPaths)
					break
				}
			}
		}
	})

	// 清理测试目录
	err = os.RemoveAll(strings.Join([]string{"/cfs/mnt", dir1}, "/"))
	if err != nil {
		t.Errorf("clean test dir(%v) failed: %v", dir1, err)
	}
}

func TestMetaWrapper_Link(t *testing.T) {
	var (
		testFile = "/cfs/mnt/hello.txt"
		linkFile = "/cfs/mnt/link_hello"
		word     = "hello.txt is writing"
	)
	if _, err := os.Create(testFile); err != nil {
		t.Fatalf("create link file failed err(%v)", err)
	}

	err := ioutil.WriteFile(testFile, []byte(word), 0755)
	if err != nil {
		t.Fatalf("write link file failed err(%v)", err)
	}

	err = os.Link(testFile, linkFile)
	if err != nil {
		t.Fatalf("link failed err(%v)", err)
	}
	data, _ := ioutil.ReadFile(linkFile)
	if strings.Compare(string(data), word) != 0 {
		t.Fatalf("link error")
	}
	// clean test file
	err = os.Remove(linkFile)
	if err != nil {
		t.Errorf("TestMetaWrapper_Link: remove link file(%s) failed\n", linkFile)
	}
	err = os.Remove(testFile)
	if err != nil {
		t.Errorf("TestMetaWrapper_Link: remove test file(%s) failed\n", testFile)
	}
}

func TestCreateFileAfterInodeLost(t *testing.T) {
	ctx := context.Background()
	mw, err := NewMetaWrapper(cfg)
	if err != nil {
		t.Fatalf("creat meta wrapper failed!")
	}
	tests := []struct {
		name string
		mode uint32
	}{
		{
			name: "test_file",
			mode: 0644,
		},
		{
			name: "test_dir",
			mode: uint32(os.ModeDir),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := mw.Create_ll(ctx, 1, tt.name, tt.mode, 0, 0, nil)
			if err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: create err(%v) name(%v)", err, tt.name)
				return
			}
			if tt.mode != uint32(os.ModeDir) {
				if unlinkInfo, err := mw.InodeUnlink_ll(ctx, info.Inode); err != nil || unlinkInfo.Inode != info.Inode {
					t.Errorf("TestCreateFileAfterInodeLost: unlink err(%v) name(%v) unlinkInfo(%v) oldInfo(%v)",
						err, tt.name, unlinkInfo, info)
					return
				}
			}
			if err = mw.Evict(ctx, info.Inode, true); err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: evict err(%v) name(%v)", err, tt.name)
				return
			}
			newInfo, newErr := mw.Create_ll(ctx, 1, tt.name, tt.mode, 0, 0, nil)
			if newErr != nil || newInfo.Inode == info.Inode {
				t.Errorf("TestCreateFileAfterInodeLost: create again err(%v) name(%v) newInode(%v) oldInode(%v)",
					newErr, tt.name, newInfo, info)
			}
			// clean test file
			if err = os.Remove(strings.Join([]string{"/cfs/mnt", tt.name}, "/")); err != nil {
				t.Errorf("TestCreateFileAfterInodeLost: clean test file(%v) failed, err(%v)", tt.name, err)
			}
		})
	}
}

func TestCreateFile(t *testing.T) {
	mw, err := NewMetaWrapper(cfg)
	mw.ClearRWPartitions()
	ctx := context.Background()
	file := "a"
	mw.Delete_ll(ctx, proto.RootIno, file, false)
	_, err = mw.Create_ll(ctx, proto.RootIno, file, 0, 0, 0, nil)
	if err != nil {
		t.Errorf("TestCreateInode: create err(%v) name(%v)", err, file)
		return
	}
	mw.Delete_ll(ctx, proto.RootIno, file, false)
}
