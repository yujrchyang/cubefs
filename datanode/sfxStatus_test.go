package datanode

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

const (
	testSfxDiskPath = "/cfs/mockdisk/data1"
)

func TestGetSfxStatus(t *testing.T) {

	var (
		dStatus                 sfxStatus
		err                     error
		indexStart              int
		indexEnd                int
		totalPhysicalCapability int
		freePhysicalCapability  int
		physicalUsageRatio      int
		compRatio               int
	)
	t.Skipf("dev %v is not sfx ssd\n", testSfxDiskPath)

	_, err = os.Stat(testSfxDiskPath)
	if os.IsNotExist(err) {
		t.Skipf("%s is not exist\n", testSfxDiskPath)
	}

	isSfx, devName := GetDevCheckSfx(testSfxDiskPath)

	/*非sfx硬盘也测试调用是否会有问题但不需要校验结果*/
	dStatus, err = GetSfxStatus(devName)
	if err != nil {
		t.Fatalf("GetSfxStatus fail:%v", err)
	}

	if isSfx == false {
		t.Skipf("dev %v is not sfx ssd\n", devName)
	}

	//使用sfx-status作为检测标准 如果没有安装工具则直接打印出结果
	_, err = exec.LookPath("sfx-status")
	if err != nil {
		t.Logf("testPath:%v ,isSfx? %v ,dev=%v", testSfxDiskPath, isSfx, devName)
		if isSfx != false {
			t.Logf("disk(%v) totalPhysicalSpace(%v) freePhysicalSpace(%v) physicalUsedRatio(%v) compressionRatio(%v)",
				devName, dStatus.totalPhysicalCapability, dStatus.freePhysicalCapability, dStatus.physicalUsageRatio, dStatus.compRatio)
		}
		t.Skipf("no find 'sfx-status',please install sfx tools or confirm the results\n")
	}

	cmd := exec.Command("sfx-status", devName)

	stdout, _ := cmd.StdoutPipe()
	cmd.Start()
	reader := bufio.NewReader(stdout)

	for {
		line, err := reader.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		indexStart = strings.Index(line, "Provisioned Capacity")
		if indexStart >= 0 {
			indexEnd = strings.Index(line, "GB")
			sTemp := line[indexStart+21 : indexEnd]
			sTemp = strings.TrimSpace(sTemp)
			totalPhysicalCapability, _ = strconv.Atoi(sTemp)
			continue
		}

		indexStart = strings.Index(line, "Free Physical Space")
		if indexStart >= 0 {
			indexEnd = strings.Index(line, "GB")
			sTemp := line[indexStart+21 : indexEnd]
			sTemp = strings.TrimSpace(sTemp)
			freePhysicalCapability, _ = strconv.Atoi(sTemp)
			continue
		}

		indexStart = strings.Index(line, "Physical Used Ratio")
		if indexStart >= 0 {
			indexEnd = strings.Index(line, "%")
			sTemp := line[indexStart+21 : indexEnd]
			sTemp = strings.TrimSpace(sTemp)
			physicalUsageRatio, _ = strconv.Atoi(sTemp)
			continue
		}

		indexStart = strings.Index(line, "Compression Ratio")
		if indexStart >= 0 {
			indexEnd = strings.Index(line, "%")
			sTemp := line[indexStart+21 : indexEnd]
			sTemp = strings.TrimSpace(sTemp)
			compRatio, _ = strconv.Atoi(sTemp)
			continue
		}
	}
	cmd.Wait()

	if (totalPhysicalCapability == 0 && isSfx != false) || (totalPhysicalCapability != 0 && isSfx == false) {
		t.Fatalf("isSfx mismatch by sfx-status: dev %v expect:%v, actual:%v \n", devName, false, isSfx)
	}

	if dStatus.physicalUsageRatio != uint32(physicalUsageRatio) {
		t.Fatalf("physicalUsageRatio mismatch by sfx-status: dev %v expect:%v, actual:%v \n", devName, physicalUsageRatio, dStatus.physicalUsageRatio)
	}

	if dStatus.compRatio != uint32(compRatio) {
		t.Fatalf("compRatio mismatch by sfx-status: dev %v expect:%v, actual:%v \n", devName, compRatio, dStatus.compRatio)
	}

	//Byte to GB by SFF-8447
	var tempGB int = (int)((dStatus.totalPhysicalCapability-50020540416)/1000194048 + 50)
	if totalPhysicalCapability != tempGB {
		t.Fatalf("totalPhysicalCapability mismatch by sfx-status: dev %v expect:%v, actual:%v \n", devName, totalPhysicalCapability, tempGB)
	}

	//Byte to GB by SFF-8447
	tempGB = (int)((dStatus.freePhysicalCapability-50020540416)/1000194048 + 50)
	if freePhysicalCapability != tempGB {
		t.Fatalf("freePhysicalCapability mismatch by sfx-status: dev %v expect:%v, actual:%v \n", devName, freePhysicalCapability, tempGB)
	}

	t.Logf("disk physicalUsageRatio:%v, compRatio:%v, totalPhysicalCapability:%v, freePhysicalCapability:%v", dStatus.physicalUsageRatio, dStatus.compRatio, totalPhysicalCapability, freePhysicalCapability)

}

func TestCheckSfxSramErr(t *testing.T) {

	var (
		err     error
		sramErr bool
	)

	isSfx, devName := GetDevCheckSfx(testSfxDiskPath)

	if isSfx == false {
		t.Skipf("dev %v is not sfx ssd\n", devName)
	}

	//	sramErr, err = CheckSfxSramErr(devName)
	if err != nil {
		t.Fatalf("dev:%s CheckSfxSramErr fail,err: %s\n", devName, err.Error())
		return
	}
	t.Logf("disk %v sramErr is %v", devName, sramErr)
}
