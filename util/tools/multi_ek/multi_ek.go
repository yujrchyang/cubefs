package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	diskv1 "github.com/shirou/gopsutil/disk"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"sync"
	"syscall"
	"time"
)

// 工具用途：主要用于分布式缓存中缓存读一致性的测试
// 1 使用跳跃写法，批量生成extentKey很长的文件
// 2 文件分别存入本地文件系统和挂载点
// 3 开启缓存读后，循环检查挂载点的md5是否发生变化

var mountP = flag.String("path", "", "mount point")
var executeMin = flag.Int("minute", 10, "execute time/minutes")
var reOpen = flag.Bool("re-open", false, "re-open")
var testMountPath string
var testLocalPath string

const (
	baseStr = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randBytes(length int) []byte {
	bytes := []byte(baseStr)
	result := make([]byte, 0)
	rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return result
}

func init() {
	flag.Parse()
	if *mountP == "" {
		panic("path must be set")
	}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(*mountP, &fs)
	if fs.Type != diskv1.FUSE_SUPER_MAGIC {
		panic(fmt.Sprintf("fs type(%v) is not fuse(%v)", fs.Type, diskv1.FUSE_SUPER_MAGIC))
	}
	testMountPath = path.Join(*mountP, "multi_ek_test")
	testLocalPath = "/tmp/cfs_multi_ek_test"
	err = os.RemoveAll(testMountPath)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	err = os.RemoveAll(testLocalPath)
	os.Mkdir(testLocalPath, 0666)
	os.Mkdir(testMountPath, 0666)
}

type filePattern struct {
	size   int64
	sparse bool
}

var fdMap map[string]*os.File
var lock sync.RWMutex

func main() {
	fdMap = make(map[string]*os.File)
	testFiles := make([]filePattern, 0)
	for i := 0; i < 64; i++ {
		testFiles = append(testFiles, filePattern{int64(i*2 + 1), false})
	}
	for i := 0; i < 64; i++ {
		testFiles = append(testFiles, filePattern{1024, true})
	}
	for i := 0; i < 64; i++ {
		testFiles = append(testFiles, filePattern{1024, false})
	}
	for i := 0; i < 32; i++ {
		testFiles = append(testFiles, filePattern{4 * 1024, false})
	}
	for i := 0; i < 32; i++ {
		testFiles = append(testFiles, filePattern{64 * 1024, false})
	}
	for i := 0; i < 16; i++ {
		testFiles = append(testFiles, filePattern{333 * 1024, false})
	}
	for i := 0; i < 16; i++ {
		testFiles = append(testFiles, filePattern{512 * 1024, false})
	}
	for i := 0; i < 16; i++ {
		testFiles = append(testFiles, filePattern{1024 * 1024, false})
	}
	for i := 0; i < 8; i++ {
		testFiles = append(testFiles, filePattern{4 * 1024 * 1024, false})
	}
	for i := 0; i < 4; i++ {
		testFiles = append(testFiles, filePattern{64 * 1024 * 1024, false})
	}
	for i := 0; i < 2; i++ {
		testFiles = append(testFiles, filePattern{128 * 1024 * 1024, false})
	}
	hashMap := make(map[string]string, 0)
	fileCh := make(chan string, 4)
	for i, fileP := range testFiles {
		var fileSlices []sourcePacket
		if fileP.sparse {
			fileSlices = generateRandomSparseFile(fileP.size)
		} else {
			fileSlices = generateRandomFile(fileP.size)
		}
		name := fmt.Sprintf("%v/%v_%v_%v", testMountPath, "test", fileP.sparse, i)
		localName := fmt.Sprintf("%v/%v_%v_%v", testLocalPath, "test", fileP.sparse, i)

		var fd *os.File
		var ok bool
		var err error

		lock.Lock()
		if fd, ok = fdMap[name]; !ok {
			fd, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}
			fdMap[name] = fd
			fmt.Printf("open fd for write file: %v\n", name)
		}
		lock.Unlock()

		localFd, err := os.OpenFile(localName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer localFd.Close()
		hash := md5.New()
		rand.New(rand.NewSource(time.Now().UnixNano()))
		//skip writing will generate multi extent keys for a single inode
		rand.Shuffle(len(fileSlices), func(i, j int) { fileSlices[i], fileSlices[j] = fileSlices[j], fileSlices[i] })
		for _, data := range fileSlices {
			n, e := fd.WriteAt(data.Data, data.Offset)
			if e != nil {
				panic(e)
			}
			if n != len(data.Data) {
				panic(fmt.Sprintf("illegal write, n:%v, bytes:%v", n, len(data.Data)))
			}
			n, e = localFd.WriteAt(data.Data, data.Offset)
			if e != nil {
				panic(e)
			}
			if n != len(data.Data) {
				panic(fmt.Sprintf("illegal write, n:%v, bytes:%v", n, len(data.Data)))
			}
		}
		io.Copy(hash, localFd)
		hashMap[name] = hex.EncodeToString(hash.Sum(nil))
		md5Fd, _ := os.Create(fmt.Sprintf("%v.md5", localName))
		defer md5Fd.Close()
		md5Fd.Write([]byte(hashMap[name]))
	}

	var read = func(name string) {
		var f *os.File
		var ok bool
		var err error
		if *reOpen {
			f, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			fmt.Printf("open fd for read file: %v\n", name)
		} else {
			lock.Lock()
			if f, ok = fdMap[name]; !ok {
				f, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
				if err != nil {
					panic(err)
				}
				fdMap[name] = f
				fmt.Printf("map missing, open fd for read file: %v\n", name)
			} else {
				f = fdMap[name]
			}
			lock.Unlock()
		}

		if _, err = f.Seek(0, 0); err != nil {
			fmt.Println("Error seeking file:", err)
			return
		}
		hash := md5.New()
		_, _ = io.Copy(hash, f)
		stat, _ := f.Stat()

		if actual := hex.EncodeToString(hash.Sum(nil)); actual != hashMap[name] {
			emptyBuf := make([]byte, stat.Size())
			h := md5.New()
			h.Write([]byte(emptyBuf))
			cipherStr := h.Sum(nil)
			panic(fmt.Sprintf("file: %v, invalid hash code, expect: %v, actrual: %v, size:%v, emptymd5:%v", name, hashMap[name], actual, stat.Size(), hex.EncodeToString(cipherStr)))
		} else {
			fmt.Printf("file check success:%v hash:%v\n", name, hashMap[name])
		}
	}

	for i := 0; i < 16; i++ {
		go func() {
			for {
				select {
				case file := <-fileCh:
					if file == "" {
						return
					}
					read(file)
				}
			}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*time.Duration(*executeMin))
	defer cancel()
	var count int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("all check success")
			close(fileCh)
			for _, fd := range fdMap {
				fd.Close()
			}
			return
		default:
			for i, f := range testFiles {
				name := fmt.Sprintf("%v/%v_%v_%v", testMountPath, "test", f.sparse, i)
				fileCh <- name
			}
			count++
			fmt.Printf("check times: %v\n", count)
		}
		time.Sleep(time.Second)
	}

}

type sourcePacket struct {
	Data   []byte
	Offset int64
}

func generateRandomFile(fileSize int64) (packets []sourcePacket) {
	packets = make([]sourcePacket, 0)
	var bufSlice []int
	if fileSize > proto.PageSize {
		bufSlice = []int{1, 4, 16, 64, 128, 512, 1024, proto.PageSize, 16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, proto.CACHE_BLOCK_SIZE, 4 * proto.CACHE_BLOCK_SIZE}
	} else {
		bufSlice = []int{1, 4, 16, 64, 128}
	}
	var offset int64
	//init test data
	for {
		if offset >= fileSize {
			break
		}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		index := rand.Intn(len(bufSlice))
		if int64(bufSlice[index]) > fileSize {
			continue
		}
		size := int(math.Min(float64(fileSize-offset), float64(bufSlice[index])))
		packets = append(packets, sourcePacket{
			Data:   randBytes(size),
			Offset: offset,
		})
		offset += int64(size)
	}
	return
}

func generateRandomSparseFile(fileSize int64) (packets []sourcePacket) {
	packets = make([]sourcePacket, 0)
	originPackets := generateRandomFile(fileSize)
	for i, p := range originPackets {
		if i%2 == 0 {
			packets = append(packets, p)
		}
	}
	return
}
