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
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
)

const (
	cmdExtentLockUse       = "extentlock [COMMAND]"
	cmdExtentLockInfoUse   = "info [partitionId] [extentId]"
	cmdExtentLockInfoShort = "show extent lock info"
)

func newExtentLockCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdExtentLockUse,
		Short: cmdExtentLockInfoShort,
	}
	cmd.AddCommand(
		newExtentLockInfo(client),
	)
	return cmd
}

func newExtentLockInfo(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdExtentLockInfoUse,
		Short: cmdExtentLockInfoShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				partition *proto.DataPartitionInfo
			)
			partitionID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				errout("parse partitionId uint failed:\n%v\n", err)
				return
			}
			extentId, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				errout("parse extentId uint failed:\n%v\n", err)
				return
			}
			if partition, err = client.AdminAPI().GetDataPartition("", partitionID); err != nil {
				errout("get data partition failed:\n%v\n", err)
				return
			}
			stdout("ExtentLockInfo:\n%s\n", extentLockInfoTableHeader)
			for _, host := range partition.Hosts {
				dataClient := http_client.NewDataClient(fmt.Sprintf("%s:%d", strings.Split(host, ":")[0], client.DataNodeProfPort), false)
				extentLockMap, err := dataClient.GetExtentLockInfo(partitionID, extentId)
				if err != nil {
					stdout("%v\n", formatExtentLockInfoTableRow(host, partitionID, extentId, "", ""))
					continue
				}
				if lockInfo, ok := extentLockMap[args[1]]; ok {
					stdout("%v\n", formatExtentLockInfoTableRow(host, partitionID, extentId, strconv.FormatInt(lockInfo.TTL, 10), formatTime(lockInfo.ExpireTime)))
				} else {
					stdout("%v\n", formatExtentLockInfoTableRow(host, partitionID, extentId, "", ""))
				}
			}
		},
	}
	return cmd
}
