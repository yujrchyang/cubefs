package main

import (
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/util/unit"
	diskv1 "github.com/shirou/gopsutil/disk"
	"hash/crc32"
	"math/rand"
	"os"
	"path"
	"sync"
	"syscall"
	"time"
)

// 工具用途：主要用于tiny extent 迁移测试中发生随机写的场景
// 1 将小于1M的小文件写入挂载点，同时保留一份内存拷贝
// 2 随机修改任意点位数据
// 3 读数据，检查修改写后的crc和内存拷贝的crc是否一致

var mountP = flag.String("path", "", "mount point")
var executeMin = flag.Int("minute", 10, "execute time/minutes")
var testMountPath string

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
	err = os.RemoveAll(testMountPath)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	os.Mkdir(testMountPath, 0666)
}

func main() {
	ticker := time.NewTicker(time.Duration(*executeMin) * time.Minute)
	defer ticker.Stop()
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				stopCh <- struct{}{}
				return
			}
		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 2000; i++ {
		go func(id int) {
			defer wg.Done()
			randomWriteTest(path.Join(testMountPath, fmt.Sprintf("tiny_extent_rw_%d", id)), stopCh)
		}(i)
	}
	wg.Wait()
	fmt.Println("all test passed")
}

func randomWriteTest(name string, stopCh chan struct{}) {
	data := randBytes(128 * unit.KB)
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	// write data
	crc := crc32.ChecksumIEEE(data)
	_, err = fd.WriteAt(data, 0)
	if err != nil {
		panic(err)
	}

	// read data
	readBuf := make([]byte, len(data))

	_, err = fd.ReadAt(readBuf, 0)
	if err != nil {
		panic(err)
	}
	readCrc := crc32.ChecksumIEEE(readBuf)
	if readCrc != crc {
		panic(fmt.Sprintf("readCrc(%v) != crc(%v)", readCrc, crc))
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	coldTime := r.Intn(600)
	fmt.Printf("wait %v seconds for migrate\n", coldTime)

	timer := time.NewTimer(time.Second * time.Duration(coldTime))
	defer timer.Stop()
	// 冷却1分钟等待迁移
	ready := false
	for {
		select {
		case <-stopCh:
			return
		case <-timer.C:
			ready = true
		}
		if ready {
			break
		}
	}

	fmt.Printf("start random write: %v\n", name)
	timer.Reset(time.Minute * 2)
	for {
		select {
		case <-timer.C:
			fmt.Printf("end random write: %v\n", name)
			return
		default:
			// 随机位置修改数据
			buf := randBytes(1024)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			off := r.Intn(len(data) - len(buf))
			copy(data[off:off+len(buf)], buf)
			crc = crc32.ChecksumIEEE(data)

			_, err = fd.WriteAt(buf, int64(off))
			if err != nil {
				panic(fmt.Sprintf("file:%v random write fail: %v", name, err))
			}

			readBuf = make([]byte, len(data))
			_, err = fd.ReadAt(readBuf, 0)
			if err != nil {
				panic(fmt.Sprintf("file:%v read fail: %v", name, err))
			}
			readCrc = crc32.ChecksumIEEE(readBuf)
			if readCrc != crc {
				panic(fmt.Sprintf("file:%v readCrc(%v) != crc(%v)", name, readCrc, crc))
			}
			time.Sleep(time.Second)
		}
	}
}
