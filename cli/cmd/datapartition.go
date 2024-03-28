// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/unit"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cli/cmd/data_check"
	"github.com/cubefs/cubefs/cli/cmd/util"
	atomic2 "go.uber.org/atomic"

	"github.com/cubefs/cubefs/sdk/http_client"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

const (
	cmdDataPartitionUse   = "datapartition [COMMAND]"
	cmdDataPartitionShort = "Manage data partition"
	cmdDataPartitionAlias = "dp"
	defaultNodeTimeOutSec = 180
)

func newDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     cmdDataPartitionUse,
		Short:   cmdDataPartitionShort,
		Aliases: []string{cmdDataPartitionAlias},
	}
	cmd.AddCommand(
		newDataPartitionGetCmd(client),
		newDataPartitionCheckCmd(client),
		newResetDataPartitionCmd(client),
		//newDataPartitionDecommissionCmd(client),
		newDataPartitionReplicateCmd(client),
		//newDataPartitionDeleteReplicaCmd(client),
		//newDataPartitionAddLearnerCmd(client),
		//newDataPartitionPromoteLearnerCmd(client),
		newDataPartitionCheckCommitCmd(client),
		//newDataPartitionFreezeCmd(client),
		//newDataPartitionUnfreezeCmd(client),
		//newDataPartitionTransferCmd(client),
		newGetCanEcMigrateCmd(client),
		newGetCanEcDelCmd(client),
		//newDelDpAlreadyEc(client),
		//newMigrateEc(client),
		//newStopMigratingByDataPartition(client),
		newDataPartitionResetRecoverCmd(client),
		newDataPartitionStopCmd(client),
		newDataPartitionReloadCmd(client),
		newDataPartitionCheckReplicaCmd(client),
		newDataPartitionDiffExtentCmd(client),
	)
	return cmd
}

const (
	cmdDataPartitionGetShort             = "Display detail information of a data partition"
	cmdCheckCorruptDataPartitionShort    = "Check and list unhealthy data partitions"
	cmdCheckCommitDataPartitionShort     = "Check the snapshot blocking by analyze commit id in data partitions"
	cmdResetDataPartitionShort           = "Reset corrupt data partition"
	cmdDataPartitionDecommissionShort    = "Decommission a replication of the data partition to a new address"
	cmdDataPartitionDecommissionAlias    = "decom"
	cmdDataPartitionStopShort            = "Stop a data partition progress in a safe way"
	cmdDataPartitionReloadShort          = "Reload a data partition on disk"
	cmdDataPartitionReplicateShort       = "Add a replication of the data partition on a new address"
	cmdDataPartitionDeleteReplicaShort   = "Delete a replication of the data partition on a fixed address"
	cmdDataPartitionAddLearnerShort      = "Add a learner of the data partition on a new address"
	cmdDataPartitionPromoteLearnerShort  = "Promote the learner of the data partition on a fixed address"
	cmdDataPartitionFreezeShort          = "Freezes the DP and does not provide the write service. It is used only for smart Volumes"
	cmdDataPartitionUnFreezeLearnerShort = "Unfreeze the DP to provide write services. It is used only for smart Volumes"
	cmdGetCanEcMigrateShort              = "Display these partitions's detail information of can ec migrate"
	cmdGetCanEcDelShort                  = "Display these partitions's detail information of already finish ec"
	cmdDelDpAlreadyEc                    = "delete the datapartition of already finish ec migration"
	cmdMigrateEc                         = "start ec migration to using ecnode store data"
	cmdStopMigratingEcByDataPartition    = "stop migrating task by data partition"
	cmdDataPartitionResetRecoverShort    = "set the data partition IsRecover value to false"
	cmdDataPartitionCheckReplicaShort    = "Check extents in this data partition"
	cmdDataPartitionCompareExtentShort   = "Compare extents in this data partition"
)

func newDataPartitionTransferCmd(client *master.MasterClient) *cobra.Command {
	var (
		partitionId uint64
		address     string
		destAddress string
		err         error
		result      string
	)

	var cmd = &cobra.Command{
		Use:   CliOpTransfer + " [DATA PARTITION ID ADDRESS  DEST ADDRESS]",
		Short: "",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			defer func() {
				if err != nil {
					errout("transfer data partition failed:%v\n", err.Error())
				}
			}()

			partitionId, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			address = args[1]
			destAddress = args[2]
			result, err = client.AdminAPI().DataPartitionTransfer(partitionId, address, destAddress)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionUnfreezeCmd(client *master.MasterClient) *cobra.Command {
	var (
		volName     string
		partitionId uint64
		err         error
		result      string
	)
	var cmd = &cobra.Command{
		Use:   CliOpUnfreeze + " [VolName PARTITION ID]",
		Short: cmdDataPartitionUnFreezeLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				errout("%v", "both volName and partitionId must be present")
			}
			defer func() {
				if err != nil {
					errout("unfreeze data partition failed:%v\n", err.Error())
				}
			}()
			volName = args[0]
			partitionId, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			result, err = client.AdminAPI().UnfreezeDataPartition(volName, partitionId)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionFreezeCmd(client *master.MasterClient) *cobra.Command {
	var (
		volName     string
		partitionId uint64
		err         error
		result      string
	)
	var cmd = &cobra.Command{
		Use:   CliOpFreeze + " [VolName PARTITION ID]",
		Short: cmdDataPartitionFreezeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				errout("%v", "both volName and partitionId must be present")
			}
			defer func() {
				if err != nil {
					errout("freeze data partition failed:%v\n", err.Error())
				}
			}()
			volName = args[0]
			partitionId, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return
			}
			result, err = client.AdminAPI().FreezeDataPartition(volName, partitionId)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	return cmd
}

func newDataPartitionResetRecoverCmd(client *master.MasterClient) *cobra.Command {
	var (
		partitionID uint64
		confirm     string
		err         error
		result      string
		optYes      bool
		partition   *proto.DataPartitionInfo
	)
	var cmd = &cobra.Command{
		Use:   CliOpResetRecover + " [PARTITION ID]",
		Short: cmdDataPartitionResetRecoverShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			defer func() {
				if err != nil {
					errout("reset data partition recover status failed:%v\n", err.Error())
				}
			}()
			partitionID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			if !optYes {
				stdout(fmt.Sprintf("Set data partition[%v] IsRecover[%v] to false.\n", partition.PartitionID, partition.IsRecover))
				stdout(fmt.Sprintf("The action may risk the danger of losing data, please confirm(y/n):"))
				_, _ = fmt.Scanln(&confirm)
				if "y" != confirm && "yes" != confirm {
					return
				}
			}

			result, err = client.AdminAPI().ResetRecoverDataPartition(partitionID)
			if err != nil {
				return
			}
			stdout("%s\n", result)
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")

	return cmd
}

func newDataPartitionGetCmd(client *master.MasterClient) *cobra.Command {
	var optRaft bool
	var optHuman bool
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [DATA PARTITION ID]",
		Short: cmdDataPartitionGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				return
			}
			stdout(formatDataPartitionInfo(optHuman, partition))
			if optRaft {
				stdout("\n")
				stdout("RaftInfo :\n")
				stdout(fmt.Sprintf("%v\n", dataPartitionRaftTableHeaderInfo))
				for _, p := range partition.Peers {
					var dnPartition *proto.DNDataPartitionInfo
					datanodeAddr := fmt.Sprintf("%s:%d", strings.Split(p.Addr, ":")[0], client.DataNodeProfPort)
					dataClient := http_client.NewDataClient(datanodeAddr, false)
					//check dataPartition by dataNode api
					dnPartition, err = dataClient.GetPartitionFromNode(partitionID)
					if err != nil {
						stdout(fmt.Sprintf("%v\n", formatDataPartitionRaftTableInfo(nil, p.ID, p.Addr)))
						continue
					}
					stdout(fmt.Sprintf("%v\n", formatDataPartitionRaftTableInfo(dnPartition, p.ID, p.Addr)))
				}
			}
		},
	}
	cmd.Flags().BoolVar(&optHuman, CliFlagHuman, true, "show used space by human read way")
	cmd.Flags().BoolVar(&optRaft, CliFlagRaft, false, "show raft peer detail info")
	return cmd
}

func newDataPartitionCheckCmd(client *master.MasterClient) *cobra.Command {
	var optEnableAutoFullfill bool
	var optCheckAll bool
	var optAutoFixUsedSize bool
	var optDiffSizeThreshold = 1
	var optMinGB int
	var optSpecifyDP uint64
	var cmd = &cobra.Command{
		Use:   CliOpCheck,
		Short: cmdCheckCorruptDataPartitionShort,
		Long: `If the data nodes are marked as "Inactive", it means the nodes has been not available for a time. It is suggested to 
eliminate the network, disk or other problems first. Once the bad nodes can never be "active", they are called corrupt 
nodes. The "decommission" command can be used to discard the corrupt nodes. However, if more than half replicas of
a partition are on the corrupt nodes, the few remaining replicas can not reach an agreement with one leader. In this case, 
you can use the "reset" command to fix the problem.The "reset" command may lead to data loss, be careful to do this.
The "reset" command will be released in next version`,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				diagnosis *proto.DataPartitionDiagnosis
				dataNodes []*proto.DataNodeInfo
				err       error
			)
			if optSpecifyDP > 0 {
				peerMsg, errorReports, partition := checkDataPartition("", optSpecifyDP, optDiffSizeThreshold, optMinGB)
				//double check
				if optAutoFixUsedSize && len(errorReports) > 0 {
					time.Sleep(time.Second * 10)
					peerMsg, errorReports, partition = checkDataPartition("", optSpecifyDP, optDiffSizeThreshold, optMinGB)
				}
				if len(errorReports) > 0 {
					stdout("%v", formatDataPartitionInfoRow(partition))
					fmt.Printf(peerMsg)
					for i, msg := range errorReports {
						fmt.Printf(fmt.Sprintf("%-8v\n", fmt.Sprintf("error %v: %v", i+1, msg)))
					}
					if partition.ReplicaNum > 2 && len(partition.MissingNodes) == 0 && len(errorReports) == 1 && errorReports[0] == UsedSizeNotEqualErr {
						err = fixSizeNoEqual(client, partition)
						if err != nil {
							log.LogErrorf("fix size no equal, partition:%v, err:%v", partition.PartitionID, err)
						}
					}
				} else {
					fmt.Printf("partition is healthy")
				}
				return
			}
			if optCheckAll {
				err = checkAllDataPartitions(client, optAutoFixUsedSize, optDiffSizeThreshold, optMinGB)
				if err != nil {
					errout("%v\n", err)
				}
				return
			}
			if diagnosis, err = client.AdminAPI().DiagnoseDataPartition(); err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("[Inactive Data nodes]:\n")
			stdout("%v\n", formatDataNodeDetailTableHeader())
			for _, addr := range diagnosis.InactiveDataNodes {
				var node *proto.DataNodeInfo
				node, err = client.NodeAPI().GetDataNode(addr)
				dataNodes = append(dataNodes, node)
			}
			sort.SliceStable(dataNodes, func(i, j int) bool {
				return dataNodes[i].ID < dataNodes[j].ID
			})
			for _, node := range dataNodes {
				stdout("%v\n", formatDataNodeDetail(node, nil, true))
			}
			/*stdout("\n")
			stdout("[Corrupt data partitions](no leader):\n")
			stdout("%v\n", partitionInfoTableHeader)
			sort.SliceStable(diagnosis.CorruptDataPartitionIDs, func(i, j int) bool {
				return diagnosis.CorruptDataPartitionIDs[i] < diagnosis.CorruptDataPartitionIDs[j]
			})
			for _, pid := range diagnosis.CorruptDataPartitionIDs {
				var partition *proto.DataPartitionInfo
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil {
					stdout("Partition not found, err:[%v]", err)
					return
				}
				stdout("%v\n", formatDataPartitionInfoRow(partition))
			}*/

			stdout("\n")
			stdout("%v\n", "[Partition lack replicas]:")
			sort.SliceStable(diagnosis.LackReplicaDataPartitionIDs, func(i, j int) bool {
				return diagnosis.LackReplicaDataPartitionIDs[i] < diagnosis.LackReplicaDataPartitionIDs[j]
			})
			cv, _ := client.AdminAPI().GetCluster()
			dns := cv.DataNodes
			var sb = strings.Builder{}

			for _, pid := range diagnosis.LackReplicaDataPartitionIDs {
				var (
					partition     *proto.DataPartitionInfo
					leaderRps     map[uint64]*proto.ReplicaStatus
					canAutoRepair bool
					peerStrings   []string
				)
				canAutoRepair = true
				if partition, err = client.AdminAPI().GetDataPartition("", pid); err != nil || partition == nil {
					stdout("get partition error, err:[%v]", err)
					return
				}
				stdout("%v", formatDataPartitionInfoRow(partition))
				sort.Strings(partition.Hosts)
				if len(partition.MissingNodes) > 0 || partition.Status == -1 {
					stdoutRed(fmt.Sprintf("partition not ready to repair"))
					continue
				}
				for i, r := range partition.Replicas {
					var rps map[uint64]*proto.ReplicaStatus
					var dnPartition *proto.DNDataPartitionInfo
					addr := strings.Split(r.Addr, ":")[0]
					if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err != nil {
						fmt.Printf(partitionInfoTablePattern+"\n", r.Addr, fmt.Sprintf("get partition info failed, err:%v", err))
						continue
					}
					sort.Strings(dnPartition.Replicas)
					fmt.Printf(partitionInfoTablePattern+"\n", r.Addr, strings.Join(dnPartition.Replicas, ","))

					if rps = dnPartition.RaftStatus.Replicas; rps != nil {
						leaderRps = rps
					}
					peers := convertPeersToArray(dnPartition.Peers)
					sort.Strings(peers)
					if i == 0 {
						peerStrings = peers
					} else {
						if !isEqualStrings(peers, peerStrings) {
							canAutoRepair = false
						}
					}
					fmt.Printf(partitionInfoTablePattern+"\n", r.Addr, strings.Join(peers, ","))
				}
				if len(leaderRps) != 3 || len(partition.Hosts) != 2 {
					stdoutRed(fmt.Sprintf("raft peer number(expected is 3, but is %v) or replica number(expected is 2, but is %v) not match ", len(leaderRps), len(partition.Hosts)))
					continue
				}
				var lackAddr []string
				for _, dn := range dns {
					if _, ok := leaderRps[dn.ID]; ok {
						if !contains(partition.Hosts, dn.Addr) {
							lackAddr = append(lackAddr, dn.Addr)
						}
					}
				}
				if len(lackAddr) != 1 {
					stdoutRed(fmt.Sprintf("Not classic partition, please check and repair it manually"))
					continue
				}
				stdoutGreen(fmt.Sprintf(" The Lack Address is: %v", lackAddr))
				if canAutoRepair {
					sb.WriteString(fmt.Sprintf("cfs-cli datapartition add-replica %v %v\n", partition.PartitionID, lackAddr[0]))
				}
				if optEnableAutoFullfill && canAutoRepair {
					stdoutGreen("     Auto proposeRepair Begin:")
					if err = client.AdminAPI().AddDataReplica(partition.PartitionID, lackAddr[0], 0); err != nil {
						stdoutRed(fmt.Sprintf("%v err:%v", "     Failed.", err))
						continue
					}
					stdoutGreen("     Done.")
					time.Sleep(2 * time.Second)
				}
				stdoutGreen(strings.Repeat("_ ", partitionInfoTableHeaderLen/2+20) + "\n")
			}
			if !optEnableAutoFullfill {
				stdout(sb.String())
			}
			return
		},
	}
	cmd.Flags().Uint64Var(&optSpecifyDP, CliFlagId, 0, "check data partition by partitionID")
	//cmd.Flags().IntVar(&optDiffSizeThreshold, CliFlagThreshold, 20, "if diff size ratio between replicas larger than threshold, report error")
	cmd.Flags().IntVar(&optMinGB, CliFlagMinGB, 0, "if diff size between replicas larger than minGB, report error")
	//cmd.Flags().BoolVar(&optAutoFixUsedSize, "auto-fix-size", false, "true - auto fix used size not equal")
	//cmd.Flags().BoolVar(&optEnableAutoFullfill, CliFlagEnableAutoFill, false, "true - automatically full fill the missing replica")
	cmd.Flags().BoolVar(&optCheckAll, "all", false, "true - check all partitions and auto fix used size; false - only check partitions which is lack of replica")
	return cmd
}

func fixSizeNoEqual(client *master.MasterClient, dp *proto.DataPartitionInfo) (err error) {
	var (
		minReplica    *proto.DataReplica
		maxReplica    *proto.DataReplica
		normalDiff    uint64
		tinyAvailDiff uint64
		reason        string
		execute       string
	)
	minReplica = dp.Replicas[0]
	maxReplica = dp.Replicas[0]
	for _, r := range dp.Replicas {
		if r.Used == 0 {
			err = fmt.Errorf("used size is 0, maybe restarted not long ago")
			return
		}
		if r.IsRecover {
			err = fmt.Errorf("extent is in repairing")
			return
		}
		if r.Used < minReplica.Used {
			minReplica = r
		}
		if r.Used > maxReplica.Used {
			maxReplica = r
		}
	}
	if minReplica.Used == 0 {
		err = fmt.Errorf("zero used size")
		return
	}
	diff := maxReplica.Used - minReplica.Used
	if diff < unit.GB {
		err = fmt.Errorf("diff size too small")
		return
	}

	if _, tinyAvailDiff, err = getTinyAvailDiff(dp.PartitionID, maxReplica, minReplica); err != nil {
		return
	}
	if normalDiff, err = normalExtentDiff(dp, maxReplica, minReplica); err != nil {
		return
	}

	defer func() {
		log.LogWarnf("fix used size, partition(%v), replica(%v), max(%v), min(%v), tinyAvailDiff(%v), reason(%s), execute(%s)", dp.PartitionID, maxReplica.Addr, maxReplica.Used, minReplica.Used, tinyAvailDiff, reason, execute)
	}()

	if math.Abs(float64(int64(tinyAvailDiff)-int64(diff))) < float64(unit.MB*128) {
		reason, execute, err = executeTinyExtentHoleRepair(client, dp, maxReplica, minReplica)
		return
	}

	if math.Abs(float64(int64(normalDiff)-int64(diff))) < float64(unit.MB*128) {
		reason, execute, err = executeDecommission(client, dp, maxReplica, minReplica)
		return
	}
	reason, execute = excuteReload(client, dp, maxReplica, minReplica)
	return
}

func normalExtentDiff(dp *proto.DataPartitionInfo, maxReplica, minReplica *proto.DataReplica) (diff uint64, err error) {
	var maxReplicaNormalExtentSize uint64
	var minReplicaNormalExtentSize uint64
	if maxReplicaNormalExtentSize, err = calculateNormalExtentDiff(maxReplica.Addr, dp.PartitionID); err != nil {
		return
	}
	if minReplicaNormalExtentSize, err = calculateNormalExtentDiff(minReplica.Addr, dp.PartitionID); err != nil {
		return
	}
	diff = maxReplicaNormalExtentSize - minReplicaNormalExtentSize
	return
}

func excuteReload(client *master.MasterClient, dp *proto.DataPartitionInfo, maxReplica, minReplica *proto.DataReplica) (reason, execute string) {
	/*	partitionPath := fmt.Sprintf("datapartition_%v_%v", dp.PartitionID, maxReplica.Total)
		err = stopReloadReplica(maxReplica.Addr, dp.PartitionID, partitionPath, maxReplica.DiskPath, client.DataNodeProfPort)
		if err != nil {
			log.LogErrorf("action[fixSizeNoEqual] stopReloadReplica max replica failed:%v", err)
		}
		time.Sleep(time.Second * 10)
		err = stopReloadReplica(minReplica.Addr, dp.PartitionID, partitionPath, minReplica.DiskPath, client.DataNodeProfPort)
		if err != nil {
			log.LogErrorf("action[fixSizeNoEqual] stopReloadReplica min replica failed:%v", err)
		}*/
	reason = "size calculation is wrong"
	execute = fmt.Sprintf("stop-reload data partition on:%v,%v", maxReplica.Addr, minReplica.Addr)
	return
}

func executeDecommission(client *master.MasterClient, dp *proto.DataPartitionInfo, maxReplica, minReplica *proto.DataReplica) (reason, execute string, err error) {
	if dp.IsRecover {
		var result string
		result, err = client.AdminAPI().ResetRecoverDataPartition(dp.PartitionID)
		if err != nil {
			log.LogErrorf("reset recover partition:%v, err:%v, result:%v", dp.PartitionID, err, result)
		}
	}
	err = client.AdminAPI().DecommissionDataPartition(dp.PartitionID, minReplica.Addr, "")
	reason = "normal extent repair after deleted, decommission replica with smaller size"
	execute = fmt.Sprintf("decommission replicas with smaller used size, host:%v", minReplica.Addr)
	return
}

func executeTinyExtentHoleRepair(client *master.MasterClient, dp *proto.DataPartitionInfo, maxReplica, minReplica *proto.DataReplica) (reason, execute string, err error) {
	var (
		needRepairTiny []uint64
		newAvailDiff   uint64
		extentResults  map[uint64]string
	)
	dHost := fmt.Sprintf("%v:%v", strings.Split(maxReplica.Addr, ":")[0], client.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	err = dataClient.PlaybackPartitionTinyDelete(dp.PartitionID)
	time.Sleep(time.Second * 30)
	if needRepairTiny, newAvailDiff, err = getTinyAvailDiff(dp.PartitionID, maxReplica, minReplica); err != nil {
		return
	}
	if newAvailDiff < uint64(unit.MB*64) {
		reason = "tiny extent deletion is not synchronized"
		execute = fmt.Sprintf("playback tiny extent delete record on:%v", maxReplica.Addr)
		return
	}

	partition, err := dataClient.GetPartitionFromNode(dp.PartitionID)
	if err != nil {
		return
	}
	extentsStrs := make([]string, 0)
	for _, e := range needRepairTiny {
		extentsStrs = append(extentsStrs, strconv.FormatUint(e, 10))
	}
	extentResults, err = dataClient.RepairExtentBatch(strings.Join(extentsStrs, "-"), partition.Path, dp.PartitionID)
	log.LogDebugf("repair extentBatch, extentResults:%v, err:%v", extentResults, err)
	reason = fmt.Sprintf("tiny extent huge hole")
	execute = fmt.Sprintf("repair tiny extent huge hole on host:%v, extents:%v", maxReplica.Addr, needRepairTiny)
	return
}

func getTinyAvailDiff(partition uint64, maxReplica, minReplica *proto.DataReplica) (needRepairTiny []uint64, availDiff uint64, err error) {
	var maxReplicaTinySizeMap map[uint64][]uint64
	if maxReplicaTinySizeMap, err = getTinyExtentSize(maxReplica.Addr, partition, client.DataNodeProfPort); err != nil {
		return
	}
	var minReplicaTinySizeMap map[uint64][]uint64
	if minReplicaTinySizeMap, err = getTinyExtentSize(minReplica.Addr, partition, client.DataNodeProfPort); err != nil {
		return
	}
	for ext, sizeArr := range maxReplicaTinySizeMap {
		if (sizeArr[0] == 0 || minReplicaTinySizeMap[ext][0] == 0) && sizeArr[0]+minReplicaTinySizeMap[ext][0] > 0 {
			err = fmt.Errorf("tiny extent:%v is in repairing", ext)
			return
		}
		if sizeArr[0] == 0 && minReplicaTinySizeMap[ext][0] == 0 {
			continue
		}
		if sizeArr[0] != minReplicaTinySizeMap[ext][0] {
			continue
		}
		if sizeArr[1] <= minReplicaTinySizeMap[ext][1] {
			continue
		}
		availDiff += sizeArr[1] - minReplicaTinySizeMap[ext][1]
		if sizeArr[1]-minReplicaTinySizeMap[ext][1] >= uint64(16*unit.MB) {
			needRepairTiny = append(needRepairTiny, ext)
		}
	}
	sort.Slice(needRepairTiny, func(i, j int) bool {
		return needRepairTiny[i] < needRepairTiny[j]
	})
	return
}
func calculateNormalExtentDiff(addr string, partition uint64) (sum uint64, err error) {
	var normalExtents []storage.ExtentInfoBlock
	if normalExtents, err = getNormalExtentWaterMark(addr, partition); err != nil {
		return
	}
	for _, e := range normalExtents {
		sum += e[proto.ExtentInfoSize]
	}
	return
}
func getNormalExtentWaterMark(addr string, partitionID uint64) (extents []storage.ExtentInfoBlock, err error) {
	ctx := context.Background()
	var packet = repl.NewPacketToGetAllWatermarks(ctx, partitionID, proto.NormalExtentType)
	var reply *repl.Packet
	if reply, err = util.SendTcpPacket(context.Background(), addr, packet); err != nil {
		return
	}
	extents = make([]storage.ExtentInfoBlock, 0)
	if err = json.Unmarshal(reply.Data[:reply.Size], &extents); err != nil {
		return
	}
	return
}

func getTinyExtentSize(addr string, partitionID uint64, profPort uint16) (sizeMap map[uint64][]uint64, err error) {
	sizeMap = make(map[uint64][]uint64, 0)
	dHost := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPort)
	dataClient := http_client.NewDataClient(dHost, false)
	for ext := uint64(1); ext <= uint64(64); ext++ {
		var extentInfo *proto.ExtentInfoBlock
		var extentHoleInfo *proto.DNTinyExtentInfo
		sizeMap[ext] = make([]uint64, 2)
		if extentInfo, err = dataClient.GetExtentInfo(partitionID, ext); err != nil {
			return
		}
		sizeMap[ext][0] = extentInfo[proto.ExtentInfoSize]

		if extentHoleInfo, err = dataClient.GetExtentHoles(partitionID, ext); err != nil {
			return
		}
		sizeMap[ext][1] = extentHoleInfo.ExtentAvaliSize
	}
	return
}

func stopReloadReplica(addr string, partitionID uint64, partitionPath, diskPath string, profPort uint16) (err error) {
	dHost := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPort)
	dataClient := http_client.NewDataClient(dHost, false)

	err = dataClient.StopPartition(partitionID)
	if err != nil {
		return err
	}
	time.Sleep(time.Second * 10)
	for i := 0; i < 3; i++ {
		if err = dataClient.ReLoadPartition(partitionPath, diskPath); err == nil {
			break
		}
	}
	return
}
func checkAllDataPartitions(client *master.MasterClient, optAutoFixUsedSize bool, optDiffSizeThreshold, minGB int) (err error) {
	var (
		volInfo    []*proto.VolInfo
		dpRepairCh chan *proto.DataPartitionInfo
		failedDps  = new(strings.Builder)
	)
	if volInfo, err = client.AdminAPI().ListVols(""); err != nil {
		stdout("%v\n", err)
		return
	}
	dpRepairCh = make(chan *proto.DataPartitionInfo, 1)
	go func() {
		for {
			select {
			case dp := <-dpRepairCh:
				if dp == nil {
					return
				}
				err = fixSizeNoEqual(client, dp)
				if err != nil {
					log.LogErrorf("fix size no equal, partition:%v, err:%v", dp.PartitionID, err)
				}
				time.Sleep(time.Second * 20)
			}
		}
	}()
	for _, vol := range volInfo {
		var (
			volView *proto.VolView
			volLock sync.Mutex
			wg      sync.WaitGroup
		)
		if volView, err = client.ClientAPI().GetVolume(vol.Name, calcAuthKey(vol.Owner)); err != nil {
			stdout("Found an invalid vol: %v\n", vol.Name)
			continue
		}
		/*		sort.SliceStable(volView.DataPartitions, func(i, j int) bool {
				return volView.DataPartitions[i].PartitionID < volView.DataPartitions[j].PartitionID
			})*/
		dpCh := make(chan bool, 20)
		for _, dp := range volView.DataPartitions {
			wg.Add(1)
			dpCh <- true
			go func(dp *proto.DataPartitionResponse) {
				defer func() {
					wg.Done()
					<-dpCh
				}()
				peerMsg, errorReports, partition := checkDataPartition(vol.Name, dp.PartitionID, optDiffSizeThreshold, minGB)
				//double check
				if optAutoFixUsedSize && len(errorReports) > 0 {
					time.Sleep(time.Second * 20)
					peerMsg, errorReports, partition = checkDataPartition(vol.Name, dp.PartitionID, optDiffSizeThreshold, minGB)
				}
				if len(errorReports) > 0 {
					volLock.Lock()
					if contains(errorReports, "connection refused") || contains(errorReports, "timeout") || contains(errorReports, "404 page not found") {
						failedDps.WriteString(fmt.Sprintf("partition:%v, errors:%v\n", partition.PartitionID, strings.Join(errorReports, ";")))
						volLock.Unlock()
						return
					}
					stdout("%v", formatDataPartitionInfoRow(partition))
					fmt.Printf(peerMsg)
					for i, msg := range errorReports {
						fmt.Printf(fmt.Sprintf("%-8v\n", fmt.Sprintf("error %v: %v", i+1, msg)))
					}
					//stdoutGreen(strings.Repeat("_ ", len(partitionInfoTableHeader)/2+20) + "\n")
					fmt.Printf(strings.Repeat("_ ", partitionInfoTableHeaderLen/2+20) + "\n")
					volLock.Unlock()
					if optAutoFixUsedSize && partition.ReplicaNum > 2 && len(partition.MissingNodes) == 0 && len(errorReports) == 1 && errorReports[0] == UsedSizeNotEqualErr {
						dpRepairCh <- partition
					}
				}
			}(dp)
		}
		wg.Wait()
	}
	dpRepairCh <- nil
	return
}

func checkDataPartition(vol string, pid uint64, optDiffSizeThreshold, minGB int) (peerMsg string, errorReports []string, partition *proto.DataPartitionInfo) {
	var err error
	if partition, err = client.AdminAPI().GetDataPartition(vol, pid); err != nil || partition == nil {
		stdout("get partition error, err:[%v]", err)
		return
	}
	var sb = new(strings.Builder)
	sort.Strings(partition.Hosts)
	if !isSizeEqual(partition, optDiffSizeThreshold, minGB) {
		errorReports = append(errorReports, UsedSizeNotEqualErr)
	}
	if proto.IsDbBack {
		return
	}
	if isBadStatus(partition) {
		errorReports = append(errorReports, PartitionNotHealthyInMaster)
	}
	peerError := checkPeer(partition, sb)
	peerMsg = sb.String()
	if len(peerError) > 0 {
		errorReports = append(errorReports, peerError...)
	}
	return
}

func isBadStatus(partition *proto.DataPartitionInfo) bool {
	return len(partition.MissingNodes) > 0 || partition.Status == -1 || len(partition.Hosts) != int(partition.ReplicaNum)
}

func isSizeEqual(partition *proto.DataPartitionInfo, percent, minGB int) (isEqual bool) {
	if len(partition.Replicas) < 2 {
		return true
	}
	if percent < 1 {
		percent = 1
	}
	if percent > 99 {
		percent = 99
	}
	isEqual = true
	var sizeArr []int
	for _, r := range partition.Replicas {
		sizeArr = append(sizeArr, int(r.Used))
	}
	sort.Ints(sizeArr)
	diff := sizeArr[len(sizeArr)-1] - sizeArr[0]
	if diff*100/percent > sizeArr[len(sizeArr)-1] && diff > minGB*unit.GB {
		isEqual = false
	}
	return
}

func checkPeer(partition *proto.DataPartitionInfo, sb *strings.Builder) (errors []string) {
	var dnPartition *proto.DNDataPartitionInfo
	var err error
	var leaderStatus *proto.Status
	errors = make([]string, 0)
	if len(partition.Hosts) != int(partition.ReplicaNum) {
		errors = append(errors, InvalidHostsNum)
	}
	for _, r := range partition.Replicas {
		addr := strings.Split(r.Addr, ":")[0]
		//check dataPartition by dataNode api
		for i := 0; i < 3; i++ {
			if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, partition.PartitionID); err == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if err != nil || dnPartition == nil {
			errors = append(errors, fmt.Sprintf("get partition[%v] failed in addr[%v], err:%v", partition.PartitionID, addr, err))
			continue
		}
		//RaftStatus Only exists on leader
		if dnPartition.RaftStatus != nil && dnPartition.RaftStatus.NodeID == dnPartition.RaftStatus.Leader {
			leaderStatus = dnPartition.RaftStatus
		}
		//print the hosts,peers,learners detail info
		peerStrings := convertPeersToArray(dnPartition.Peers)
		learnerStrings := convertLearnersToArray(dnPartition.Learners)
		sort.Strings(peerStrings)
		sort.Strings(partition.Hosts)
		sort.Strings(learnerStrings)
		sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n", r.Addr, "(peers) "+strings.Join(peerStrings, ",")))
		if len(dnPartition.Learners) > 0 {
			sb.WriteString(fmt.Sprintf(partitionInfoTablePattern+"\n", "", "(learners) "+strings.Join(learnerStrings, ",")))
		}
		if !isEqualStrings(partition.Hosts, peerStrings) || len(partition.Learners) != len(dnPartition.Learners) {
			errors = append(errors, fmt.Sprintf(ReplicaNotConsistent+" on host[%v]", r.Addr))
		}
	}
	if leaderStatus == nil || len(leaderStatus.Replicas) == 0 {
		errors = append(errors, RaftNoLeader)
	}
	return
}

func newResetDataPartitionCmd(client *master.MasterClient) *cobra.Command {
	var optManualResetAddrs string
	var cmd = &cobra.Command{
		Use:   CliOpReset + " [DATA PARTITION ID]",
		Short: cmdResetDataPartitionShort,
		Long: `If more than half replicas of a partition are on the corrupt nodes, the few remaining replicas can 
not reach an agreement with one leader. In this case, you can use the "reset" command to fix the problem, however 
this action may lead to data loss, be careful to do this.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partitionID uint64
				confirm     string
				err         error
			)
			defer func() {
				if err != nil {
					errout("Error:%v", err)
					OsExitWithLogFlush()
				}
			}()
			partitionID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			stdout(fmt.Sprintf("The action may risk the danger of losing data, please confirm(y/n):"))
			_, _ = fmt.Scanln(&confirm)
			if "y" != confirm && "yes" != confirm {
				return
			}
			if "" != optManualResetAddrs {
				if err = client.AdminAPI().ManualResetDataPartition(partitionID, optManualResetAddrs); err != nil {
					return
				}
			} else {
				if err = client.AdminAPI().ResetDataPartition(partitionID); err != nil {
					return
				}
			}
		},
	}
	cmd.Flags().StringVar(&optManualResetAddrs, CliFlagAddress, "", "reset raft members according to the addr, split by ',' ")

	return cmd
}

func newDataPartitionStopCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStop + " [ADDRESS] [DATA PARTITION ID] ",
		Short: cmdDataPartitionStopShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				dp          *proto.DataPartitionInfo
				partitionID uint64
				err         error
			)
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			address := args[0]
			partitionID, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			dp, err = client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			var exist bool
			for _, h := range dp.Hosts {
				if h == address {
					exist = true
					break
				}
			}
			if !exist {
				err = fmt.Errorf("host[%v] not exist in hosts[%v]", address, dp.Hosts)
				return
			}
			dHost := fmt.Sprintf("%v:%v", strings.Split(address, ":")[0], client.DataNodeProfPort)
			dataClient := http_client.NewDataClient(dHost, false)
			err = dataClient.StopPartition(partitionID)
			if err != nil {
				return
			}
			fmt.Printf("stop partition: %v success\n", partitionID)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionReloadCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpReload + " [ADDRESS] [DATA PARTITION ID] ",
		Short: cmdDataPartitionReloadShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				dp          *proto.DataPartitionInfo
				partitionID uint64
				err         error
			)
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			address := args[0]
			partitionID, err = strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			dp, err = client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			var diskPath string
			var exist bool
			for _, r := range dp.Replicas {
				if r.Addr == address {
					exist = true
					diskPath = r.DiskPath
					break
				}
			}
			if !exist {
				err = fmt.Errorf("host[%v] not exist in hosts[%v]", address, dp.Hosts)
				return
			}
			partitionPath := fmt.Sprintf("datapartition_%v_%v", partitionID, dp.Replicas[0].Total)

			dHost := fmt.Sprintf("%v:%v", strings.Split(address, ":")[0], client.DataNodeProfPort)
			dataClient := http_client.NewDataClient(dHost, false)
			err = dataClient.ReLoadPartition(partitionPath, diskPath)
			if err != nil {
				return
			}
			fmt.Printf("reload partition: %v success\n", partitionID)
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:     CliOpDecommission + " [ADDRESS] [DATA PARTITION ID] [DestAddr] ",
		Short:   cmdDataPartitionDecommissionShort,
		Aliases: []string{cmdDataPartitionDecommissionAlias},
		Args:    cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var destAddr string
			if len(args) >= 3 {
				destAddr = args[2]
			}
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DecommissionDataPartition(partitionID, address, destAddr); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionReplicateCmd(client *master.MasterClient) *cobra.Command {
	var optAddReplicaType string
	var cmd = &cobra.Command{
		Use:   CliOpReplicate + " [DATA PARTITION ID] [ADDRESS]",
		Short: cmdDataPartitionReplicateShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var address string
			if len(args) == 1 && optAddReplicaType == "" {
				stdout("there must be at least 2 args or use add-replica-type flag\n")
				return
			}
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if len(args) >= 2 {
				address = args[1]
			}
			var addReplicaType proto.AddReplicaType
			if optAddReplicaType != "" {
				var addReplicaTypeUint uint64
				if addReplicaTypeUint, err = strconv.ParseUint(optAddReplicaType, 10, 64); err != nil {
					stdout("%v\n", err)
					return
				}
				addReplicaType = proto.AddReplicaType(addReplicaTypeUint)
				if addReplicaType != proto.AutoChooseAddrForQuorumVol && addReplicaType != proto.DefaultAddReplicaType {
					err = fmt.Errorf("region type should be %d(%s) or %d(%s)",
						proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol, proto.DefaultAddReplicaType, proto.DefaultAddReplicaType)
					stdout("%v\n", err)
					return
				}
				stdout("partitionID:%v add replica type:%s\n", partitionID, addReplicaType)
			}
			if err = client.AdminAPI().AddDataReplica(partitionID, address, addReplicaType); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}

	cmd.Flags().StringVar(&optAddReplicaType, CliFlagAddReplicaType, "",
		fmt.Sprintf("Set add replica type[%d(%s)]", proto.AutoChooseAddrForQuorumVol, proto.AutoChooseAddrForQuorumVol))
	return cmd
}

func newDataPartitionDeleteReplicaCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelReplica + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionDeleteReplicaShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().DeleteDataReplica(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionAddLearnerCmd(client *master.MasterClient) *cobra.Command {
	var (
		optAutoPromote bool
		optThreshold   uint8
	)
	const defaultLearnThreshold uint8 = 90
	var cmd = &cobra.Command{
		Use:   CliOpAddLearner + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionAddLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				autoPromote bool
				threshold   uint8
			)
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if optAutoPromote {
				autoPromote = optAutoPromote
			}
			if optThreshold <= 0 || optThreshold > 100 {
				threshold = defaultLearnThreshold
			} else {
				threshold = optThreshold
			}
			if err = client.AdminAPI().AddDataLearner(partitionID, address, autoPromote, threshold); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().Uint8VarP(&optThreshold, CliFlagThreshold, "t", 0, "Specify threshold of learner,(0,100],default 90")
	cmd.Flags().BoolVarP(&optAutoPromote, CliFlagAutoPromote, "a", false, "Auto promote learner to peers")
	return cmd
}

func newDataPartitionPromoteLearnerCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpPromoteLearner + " [ADDRESS] [DATA PARTITION ID]",
		Short: cmdDataPartitionPromoteLearnerShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			address := args[0]
			partitionID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			if err = client.AdminAPI().PromoteDataLearner(partitionID, address); err != nil {
				stdout("%v\n", err)
				return
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}

func newDataPartitionCheckCommitCmd(client *master.MasterClient) *cobra.Command {
	var optSpecifyDP uint64
	var cmd = &cobra.Command{
		Use:   CliOpCheckCommit,
		Short: cmdCheckCommitDataPartitionShort,
		Long:  `if the follower lack too much raft log from leader, the raft may be hang, we should check and resolve it `,
		Run: func(cmd *cobra.Command, args []string) {
			if optSpecifyDP == 0 {
				checkCommit(client)
				return
			}
			partition, err1 := client.AdminAPI().GetDataPartition("", optSpecifyDP)
			if err1 != nil {
				stdout("%v\n", err1)
				return
			}
			for _, r := range partition.Replicas {
				if r.IsLeader && time.Now().Unix()-r.ReportTime <= defaultNodeTimeOutSec {
					isLack, lackID, active, next, firstIdx, err := checkDataPartitionCommit(r.Addr, partition.PartitionID)
					if err != nil || !isLack {
						continue
					}
					var host string
					for _, p := range partition.Peers {
						if p.ID == lackID {
							host = p.Addr
						}
					}
					fmt.Printf("Volume,Partition,BadPeerID,BadHost,IsActive,Next,FirstIndex\n")
					fmt.Printf("%v,%v,%v,%v,%v,%v,%v\n", partition.VolName, optSpecifyDP, lackID, host, active, next, firstIdx)
				}
			}
		},
	}
	cmd.Flags().Uint64Var(&optSpecifyDP, CliFlagId, 0, "check data partition by partitionID")
	return cmd
}

func newGetCanEcMigrateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetCanEcMigrate,
		Short: cmdGetCanEcMigrateShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				partitions = make([]*proto.DataPartitionResponse, 0)
			)
			if partitions, err = client.AdminAPI().GetCanMigrateDataPartitions(); err != nil {
				return
			}
			stdout("%v\n", dataPartitionTableHeader)
			for _, partition := range partitions {
				stdout(formatDataPartitionTableRow(partition))
				stdout("\n")
			}
		},
	}
	return cmd
}

func newGetCanEcDelCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGetCanEcDel,
		Short: cmdGetCanEcDelShort,
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err        error
				partitions = make([]*proto.DataPartitionResponse, 0)
			)
			if partitions, err = client.AdminAPI().GetCanDelDataPartitions(); err != nil {
				return
			}
			stdout("%v\n", dataPartitionTableHeader)
			for _, partition := range partitions {
				stdout(formatDataPartitionTableRow(partition))
				stdout("\n")
			}
		},
	}
	return cmd
}

func newDelDpAlreadyEc(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelAleadyEcDp + " [PARTITION ID]",
		Short: cmdDelDpAlreadyEc,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", client.AdminAPI().DeleteDpAlreadyEc(partitionID))
		},
	}
	return cmd
}

func newMigrateEc(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpMigrateEc + " [PARTITION ID]",
		Short: cmdMigrateEc,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			test := false
			if len(args) == 2 && args[1] == "test" {
				test = true
			}
			stdout("%v\n", client.AdminAPI().MigrateEcById(partitionID, test))
		},
	}
	return cmd
}

func getDataPartitionCrc(dp *proto.DataPartitionInfo, dpCrc map[uint64]map[string]uint32, extentInfo []uint64, dpWg *sync.WaitGroup, client *master.MasterClient, printLog bool) {
	defer dpWg.Done()
	var hostmapLock sync.Mutex
	for _, extentId := range extentInfo {
		if printLog {
			stdout("DataPartition:%v Extent:%v start\n", dp.PartitionID, extentId)
		}
		var wg sync.WaitGroup
		hostmap := make(map[string]uint32)
		for _, host := range dp.Hosts {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				var (
					crc uint32
					err error
				)
				arr := strings.Split(host, ":")
				if printLog {
					stdout("  from DataNode(%v) get crc\n", host)
				}
				if crc, err = client.NodeAPI().DataNodeGetExtentCrc(arr[0], dp.PartitionID, extentId); err != nil {
					if printLog {
						stdout("  DataNode(%v) GetExtentCrc err(%v)\n", host, err)
					}
					return
				}
				hostmapLock.Lock()
				hostmap[host] = crc
				hostmapLock.Unlock()
			}(host)
		}
		wg.Wait()
		dpCrc[extentId] = hostmap
	}
}

func newStopMigratingByDataPartition(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpStopMigratingEc + " [PARTITION ID]",
		Short: cmdStopMigratingEcByDataPartition,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				stdout("%v\n", err)
				return
			}
			stdout("%v\n", client.AdminAPI().StopMigratingByDataPartition(partitionID))
		},
	}
	return cmd
}

func checkCommit(client *master.MasterClient) (err error) {

	f, _ := os.OpenFile(fmt.Sprintf("check_commit_%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer f.Close()
	stat, _ := f.Stat()
	if stat.Size() == 0 {
		f.WriteString("Volume,Partition,BadPeerID,BadHost,IsActive,Next,FirstIndex\n")
	}
	var badDps sync.Map
	var partitionFunc = func(volumeName string, partition *proto.DataPartitionResponse) (err error) {
		isLack, lackID, _, _, _, err := checkDataPartitionCommit(partition.GetLeaderAddr(), partition.PartitionID)
		if err != nil {
			return
		}
		if isLack {
			badDps.Store(partition.PartitionID, lackID)
		}
		return
	}

	var volFunc = func(vol *proto.SimpleVolView) {
		//retry to check
		for i := 0; i < 4; i++ {
			count := 0
			badDps.Range(func(key, value interface{}) bool {
				count++
				id := key.(uint64)
				oldLackID := value.(uint64)
				partition, err1 := client.AdminAPI().GetDataPartition("", id)
				if err1 != nil {
					return true
				}
				for _, r := range partition.Replicas {
					if r.IsLeader {
						isLack, lackID, _, _, _, err2 := checkDataPartitionCommit(r.Addr, partition.PartitionID)
						if err2 != nil {
							continue
						}
						if !isLack {
							badDps.Delete(partition.PartitionID)
						} else if lackID != oldLackID {
							badDps.Store(partition.PartitionID, lackID)
						}
					}
				}
				return true
			})
			if count == 0 {
				break
			}
			time.Sleep(time.Minute)
		}

		//output
		badDps.Range(func(key, value interface{}) bool {
			id := key.(uint64)
			partition, err1 := client.AdminAPI().GetDataPartition("", id)
			if err1 != nil {
				return true
			}
			for _, r := range partition.Replicas {
				if r.IsLeader {
					isLack, lackID, active, next, first, err2 := checkDataPartitionCommit(r.Addr, partition.PartitionID)
					if err2 != nil || !isLack {
						continue
					}
					var host string
					for _, p := range partition.Peers {
						if p.ID == lackID {
							host = p.Addr
						}
					}
					f.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v\n", vol.Name, id, lackID, host, active, next, first))
				}
			}
			return true
		})
		f.Sync()
	}
	vols := util.LoadSpecifiedVolumes()
	ids := util.LoadSpecifiedPartitions()
	rangeAllDataPartitions(20, vols, ids, volFunc, partitionFunc)
	fmt.Println("scan finish, result has been saved to local file")
	return
}

func checkDataPartitionCommit(leader string, pid uint64) (lack bool, lackID uint64, active bool, next, firstIdx uint64, err error) {
	var dnPartition *proto.DNDataPartitionInfo
	addr := strings.Split(leader, ":")[0]
	//check dataPartition by dataNode api
	for i := 0; i < 3; i++ {
		if dnPartition, err = client.NodeAPI().DataNodeGetPartition(addr, pid); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return
	}
	if dnPartition.RaftStatus != nil && dnPartition.RaftStatus.Replicas != nil {
		for id, r := range dnPartition.RaftStatus.Replicas {
			if dnPartition.RaftStatus.Leader == id {
				continue
			}
			if r.Next < dnPartition.RaftStatus.Log.FirstIndex || !r.Active {
				lack = true
				lackID = id
				next = r.Next
				active = r.Active
				firstIdx = dnPartition.RaftStatus.Log.FirstIndex
			}
		}
	}
	return
}

func newDataPartitionCheckReplicaCmd(client *master.MasterClient) *cobra.Command {
	var fromFile bool
	var fromTime string
	var ids []uint64
	var quickCheck bool
	var extentModifyMin time.Time
	var cmd = &cobra.Command{
		Use:   CliOpCheckReplica + " [DATA PARTITION ID]",
		Short: cmdDataPartitionCheckReplicaShort,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var partitionID uint64
			if fromFile {
				ids = util.LoadSpecifiedPartitions()
			} else {
				if len(args) < 1 {
					stdout("please input partition id")
					return
				}
				partitionID, err = strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					stdout("%v\n", err)
					return
				}
				ids = append(ids, partitionID)
			}
			if extentModifyMin, err = parseTime(fromTime); err != nil {
				return
			}
			limitCh := make(chan bool, 50)
			wg := sync.WaitGroup{}
			dpCounter := atomic2.Int64{}
			for _, id := range ids {
				limitCh <- true
				wg.Add(1)
				go func(pid uint64) {
					defer func() {
						wg.Done()
						<-limitCh
						dpCounter.Add(1)
						log.LogInfof("check data partition(%v) finish, progress(%d/%d)", pid, dpCounter.Load(), len(ids))
					}()
					var dpMasterInfo *proto.DataPartitionInfo
					dpMasterInfo, err = client.AdminAPI().GetDataPartition("", pid)
					if err != nil {
						return
					}
					_, _ = data_check.CheckDataPartitionReplica(dpMasterInfo, client, extentModifyMin, nil, quickCheck)
				}(id)
			}
			wg.Wait()
			stdout("finish data partition replica crc check")
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validDataNodes(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().BoolVar(&fromFile, "from-file", false, "check partitions from file, file name:`ids`, format:`partition`")
	cmd.Flags().StringVar(&fromTime, "from-time", "1970-01-01 00:00:00", "specify extent modify from time to check, format:yyyy-mm-dd hh:mm:ss")
	cmd.Flags().BoolVar(&quickCheck, "quick-check", false, "quick check: check crc from meta data first, if not the same, then check md5")
	return cmd
}

func newDataPartitionDiffExtentCmd(client *master.MasterClient) *cobra.Command {
	var opHosts string
	var cmd = &cobra.Command{
		Use:   CliOpCompareExtent + " [DATA PARTITION ID]",
		Short: cmdDataPartitionCompareExtentShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var partitionID uint64
			var err error
			var partition *proto.DataPartitionInfo
			var hosts []string
			defer func() {
				if err != nil {
					stdout(err.Error())
				}
			}()
			partitionID, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			partition, err = client.AdminAPI().GetDataPartition("", partitionID)
			if err != nil {
				return
			}
			if opHosts != "" {
				hosts = strings.Split(opHosts, ",")
				for _, h := range hosts {
					if !existStr(h, partition.Hosts) {
						err = fmt.Errorf("invalid host:%v", h)
						return
					}
				}
			} else {
				hosts = partition.Hosts
			}

			extentCountMap := make(map[uint64]int, 0)
			fileMap := make(map[string][]proto.ExtentInfoBlock, 0)
			for _, h := range hosts {
				var dp *proto.DNDataPartitionInfo
				dataClient := http_client.NewDataClient(fmt.Sprintf("%v:%v", strings.Split(h, ":")[0], client.DataNodeProfPort), false)
				if dp, err = dataClient.GetPartitionFromNode(partitionID); err != nil {
					return
				}
				fileMap[h] = dp.Files
			}
			for _, files := range fileMap {
				for _, f := range files {
					if _, ok := extentCountMap[f[proto.ExtentInfoFileID]]; !ok {
						extentCountMap[f[proto.ExtentInfoFileID]] = 1
					} else {
						extentCountMap[f[proto.ExtentInfoFileID]] += 1
					}
				}
			}
			commonExtents := make([]int, 0)
			for extentID, count := range extentCountMap {
				if count == len(hosts) {
					commonExtents = append(commonExtents, int(extentID))
				}
			}
			stdout("common extent(%v):\n", len(commonExtents))
			sort.Ints(commonExtents)
			stdout("%v\n\n", commonExtents)
			for host, files := range fileMap {
				diffExtents := make([]proto.ExtentInfoBlock, 0)
				var count int
				for _, file := range files {
					if extentCountMap[file[proto.ExtentInfoFileID]] == len(hosts) {
						continue
					}
					count++
					diffExtents = append(diffExtents, file)
				}
				stdout("host(%v), total(%v), diff(%v): \n", host, len(files), len(diffExtents))
				stdout("%v\n\n", diffExtents)
			}
		},
	}
	cmd.Flags().StringVar(&opHosts, "hosts", "", "specify hosts to compare, split by ,")
	return cmd
}

func existStr(str string, strs []string) bool {
	if len(strs) == 0 {
		return false
	}
	for _, s := range strs {
		if s == str {
			return true
		}
	}
	return false
}
