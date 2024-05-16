package cmd

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	cmdColdFileUse   = "coldFile [COMMAND]"
	cmdColdFileShort = "Manage cold file info"
)

func newColdFileCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdColdFileUse,
		Short: cmdColdFileShort,
	}
	cmd.AddCommand(
		newColdFileAnalysisCmd(client),
	)
	return cmd
}

const (
	cmdColdFileAnalysisUse   = "analysis"
	cmdColdFileAnalysisShort = "analysis volume cold file"
	CliSecsAgo               = "secsAgo"
	CliDaysAgo               = "daysAgo"
	CliFlagCheckSSD          = "checkSSD"
	coldFileAnalysisTitle    = "volume,冷数据文件数,总文件数,冷文件数量占比,冷数据使用容量(GB),总文件使用容量(GB),冷数据容量占比\n"
	coldFileAnalysisFileName = "coldFileAnalysis.csv"
)

type ColdFileValue struct {
	coldFileCount      uint64
	allFileCount       uint64
	coldFileCountRatio float64
	coldFileTotalSize  uint64
	allFileTotalSize   uint64
	coldFileUsedRatio  float64
}

var (
	mapMu             sync.Mutex
)

func newColdFileAnalysisCmd(client *master.MasterClient) *cobra.Command {
	var (
		optVolName           string
		optCheckSSD          bool
		optVolumeConcurrency uint64
		optMpConcurrency     uint64
		optDaysAgo           float64
		optSecsAgo           float64
	)
	var cmd = &cobra.Command{
		Use:   cmdColdFileAnalysisUse,
		Short: cmdColdFileAnalysisShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			stdout("check volume cold file begin.\n")
			stdout("volumeConcurrency:%v mpConcurrency:%v\n", optVolumeConcurrency, optMpConcurrency)
			var volNames []string
			if optVolName == all {
				var vols []*proto.VolInfo
				vols, err = client.AdminAPI().ListVols("")
				if err != nil {
					errout("list vols err:%v", err)
				}
				for _, vol := range vols {
					volNames = append(volNames, vol.Name)
				}
			} else {
				if optVolName != "" {
					volNames = strings.Split(optVolName, ",")
				}
			}
			if len(volNames) == 0 && !optCheckSSD {
				return
			}
			if len(volNames) == 0 && optCheckSSD {
				volNames = filterSSDVolume(client)
			}
			if len(volNames) == 0 {
				return
			}
			var coldFileFd *os.File
			coldFileFd, err = os.OpenFile(coldFileAnalysisFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				errout("OpenFile fileName:%v err:%v", coldFileAnalysisFileName, err)
			}
			defer coldFileFd.Close()
			buf := make([]byte, len(coldFileAnalysisTitle))
			copy(buf, coldFileAnalysisTitle)
			_, _ = coldFileFd.Write(buf)
			var (
				wg        sync.WaitGroup
				timePoint = time.Now().Unix()
			)
			ch := make(chan struct{}, optVolumeConcurrency)
			for i, volName := range volNames {
				var mps []*proto.MetaPartitionView
				if mps, err = client.ClientAPI().GetMetaPartitions(volName); err != nil {
					stdout("Volume(%v) got MetaPartitions failed, err:%v\n", volName, err)
					continue
				}
				wg.Add(1)
				ch <- struct{}{}
				go func(i int, volName string, mps []*proto.MetaPartitionView) {
					defer func() {
						wg.Done()
						<-ch
					}()
					coldFileValue := checkMpsColdFile(i, timePoint, volName, mps, client, optMpConcurrency, optDaysAgo, optSecsAgo)
					mapMu.Lock()
					coldFileUsedSizeGb := coldFileValue.coldFileTotalSize/1024/1024/1024
					allFileUsedSizeGb := coldFileValue.allFileTotalSize/1024/1024/1024
					coldFileResult := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v\n",
						volName, coldFileValue.coldFileCount, coldFileValue.allFileCount,
						fmt.Sprintf("%.2f", coldFileValue.coldFileCountRatio),
						coldFileUsedSizeGb,
						allFileUsedSizeGb,
						fmt.Sprintf("%.2f", coldFileValue.coldFileUsedRatio),
					)
					buff := make([]byte, len(coldFileResult))
					copy(buff, coldFileResult)
					_, _ = coldFileFd.Write(buff)
					mapMu.Unlock()
				}(i, volName, mps)
			}
			wg.Wait()
			stdout("check volume cold file end.\n")
			return
		},
	}

	cmd.Flags().StringVar(&optVolName, CliFlagVolName, "", "Specify volume name, can split by comma")
	cmd.Flags().BoolVar(&optCheckSSD, CliFlagCheckSSD, false, "check all ssd volume")
	cmd.Flags().Uint64Var(&optVolumeConcurrency, CliVolumeConcurrency, 2, "volume concurrency")
	cmd.Flags().Uint64Var(&optMpConcurrency, CliMpConcurrency, 5, "mp concurrency")
	cmd.Flags().Float64Var(&optDaysAgo, CliDaysAgo, 1, "a few days ago")
	cmd.Flags().Float64Var(&optSecsAgo, CliSecsAgo, 1, "a few seconds ago， when days appear, seconds expire")
	return cmd
}

func filterSSDVolume(client *master.MasterClient) (volNames []string) {
	vols, _ := client.AdminAPI().ListVols("")
	var (
		wg sync.WaitGroup
		ch = make(chan struct{}, 30)
		mu sync.Mutex
	)
	for _, vol := range vols {
		wg.Add(1)
		ch <- struct{}{}
		go func(volName string) {
			defer func() {
				<- ch
				wg.Done()
			}()
			info, _ := client.AdminAPI().GetVolumeSimpleInfo(volName)
			zNames := strings.Split(info.ZoneName, ",")
			for _, zName := range zNames {
				if strings.Contains(zName, "ssd") {
					mu.Lock()
					volNames = append(volNames, volName)
					mu.Unlock()
					break
				}
			}
		}(vol.Name)
	}
	wg.Wait()
	stdout("filterSSDVolume finished, ssd volume length:%v\n", len(volNames))
	return
}

func checkMpsColdFile(index int, timePoint int64, volName string, mps []*proto.MetaPartitionView, client *master.MasterClient, mpConcurrency uint64, daysAgo, optSecsAgo float64) (coldFileValue ColdFileValue) {
	stdout("index:%v volume:%v\n", index, volName)
	var (
		wg sync.WaitGroup
		ch = make(chan struct{}, mpConcurrency)
		mu sync.Mutex
	)
	if len(mps) > 0 {
		stdout("%v\n", formatColdFileInfoTableHeader())
	}
	for _, mp := range mps {
		wg.Add(1)
		ch <- struct{}{}
		go func(mp *proto.MetaPartitionView) {
			defer func() {
				wg.Done()
				<-ch
			}()
			var (
				mpInfo         *proto.MetaPartitionInfo
				leaderNodeInfo *proto.MetaNodeInfo
				inodeInfos     map[uint64]*proto.MetaInode
				err            error
			)
			if mpInfo, err = client.ClientAPI().GetMetaPartition(mp.PartitionID, ""); err != nil {
				stdout("Volume(%v) mpId(%v) get MetaPartition failed, err:%v\n", volName, mp.PartitionID, err)
				return
			}
			leaderAddr := getLeaderAddr(mpInfo.Replicas)
			if leaderNodeInfo, err = client.NodeAPI().GetMetaNode(leaderAddr); err != nil {
				stdout("Volume(%v) mpId(%v) leaderAddr(%v) get metaNode info failed:%v\n", volName, mp.PartitionID, leaderAddr, err)
				return
			}
			if leaderNodeInfo.ProfPort == "" {
				leaderNodeInfo.ProfPort = "9092"
			}
			leaderIpPort := strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + leaderNodeInfo.ProfPort
			metaAdminApi := meta.NewMetaHttpClient(leaderIpPort, false)
			if inodeInfos, err = metaAdminApi.GetAllInodes(mpInfo.PartitionID); err != nil {
				stdout("Volume(%v) mpId(%v) leaderIpPort(%v) get MpInodeIds info failed:%v\n", volName, mp.PartitionID, leaderIpPort, err)
				return
			}
			inodeColdFileValue := inodeInfoColdFileCheck(timePoint, mp.PartitionID, inodeInfos, volName, daysAgo, optSecsAgo)
			mu.Lock()
			coldFileValue.allFileCount += inodeColdFileValue.allFileCount
			coldFileValue.allFileTotalSize += inodeColdFileValue.allFileTotalSize
			coldFileValue.coldFileCount += inodeColdFileValue.coldFileCount
			coldFileValue.coldFileTotalSize += inodeColdFileValue.coldFileTotalSize
			mu.Unlock()
		}(mp)
	}
	wg.Wait()
	if coldFileValue.allFileCount == 0 {
		coldFileValue.coldFileCountRatio = 0
	} else {
		coldFileValue.coldFileCountRatio = float64(coldFileValue.coldFileCount) / float64(coldFileValue.allFileCount)
		coldFileValue.coldFileUsedRatio = float64(coldFileValue.coldFileTotalSize) / float64(coldFileValue.allFileTotalSize)
	}
	return
}

func inodeInfoColdFileCheck(timePoint int64, mpId uint64, inodeInfos map[uint64]*proto.MetaInode, volName string, daysAgo, optSecsAgo float64) (inodeColdFileValue ColdFileValue) {
	for inodeId, inodeInfo := range inodeInfos {
		if inodeInfo.Size == 0 {
			continue
		}
		inodeColdFileValue.allFileCount++
		inodeColdFileValue.allFileTotalSize += inodeInfo.Size
		var timeToCheck int64
		if inodeInfo.AccessTime <= 0 {
			timeToCheck = inodeInfo.ModifyTime
		} else {
			timeToCheck = inodeInfo.AccessTime
		}
		if daysAgo > 0 {
			if timePoint-timeToCheck > int64(daysAgo*24*60*60) {
				inodeColdFileValue.coldFileCount++
				inodeColdFileValue.coldFileTotalSize += inodeInfo.Size
				stdout("%v\n", formatColdFileInfo(volName, mpId, inodeId, inodeInfo.CreateTime, inodeInfo.ModifyTime, inodeInfo.AccessTime))
			}
		} else {
			if timePoint-timeToCheck > int64(optSecsAgo) {
				inodeColdFileValue.coldFileCount++
				inodeColdFileValue.coldFileTotalSize += inodeInfo.Size
				stdout("%v\n", formatColdFileInfo(volName, mpId, inodeId, inodeInfo.CreateTime, inodeInfo.ModifyTime, inodeInfo.AccessTime))
			}
		}
	}
	return
}
