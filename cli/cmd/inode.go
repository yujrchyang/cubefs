package cmd

import (
	"encoding/json"
	"fmt"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

const (
	cmdInodeUse         = "inode [COMMAND]"
	cmdInodeInfoUse     = "info volumeName inodeId"
	cmdInodeShort       = "Show inode information"
	cmdInodeSearch      = "search volumeName path"
	cmdInodeSearchShort = "search inode id by path"
)

func newInodeCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newInodeInfoCmd(client),
		newSearchInodeByPathCmd(client),
	)
	return cmd
}

func getInodeInfo(client *sdk.MasterClient, addr string, volumeName string, ino uint64) (err error) {
	var (
		leader        string
		mpID          uint64
		result        []byte
		resp          *http.Response
		inodeInfoView *proto.InodeInfoView
	)

	leader, mpID, err = util_sdk.LocateInode(ino, client, volumeName)
	if err != nil {
		return
	}
	if addr == "" {
		addr = leader
	}
	mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], client.MetaNodeProfPort), false)
	inodeInfoView, err = mtClient.GetInode(mpID, ino)
	if err != nil {
		return
	}
	stdout("Summary of inode  :\n%s\n", formatInodeInfoView(inodeInfoView))

	// getExtentsByInode
	path := "getExtentsNoModifyAT"
	if proto.IsDbBack {
		path = "getExtents"
	}
	resp, err = http.Get(fmt.Sprintf("http://%s:%d/%s?pid=%d&ino=%d", strings.Split(addr, ":")[0], client.MetaNodeProfPort, path, mpID, ino))
	if err != nil {
		errout("get inode extents failed:\n%v\n", err)
		return
	}
	result, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		errout("read inode extents failed:\n%v\n", err)
		return
	}
	value := make(map[string]interface{})
	err = json.Unmarshal(result, &value)
	if err != nil {
		errout("unmarshal inode extents failed:\n%s\n%v\n", string(result), err)
		return
	}
	var eks []interface{}
	if proto.IsDbBack {
		if value["Extents"] == nil {
			errout("no extents\n")
			return
		}
		eks = value["Extents"].([]interface{})
	} else {
		if value["msg"] != "Ok" {
			errout("get inode extents failed: %v\n", value["msg"])
			return
		}
		data := value["data"].(map[string]interface{})
		if data["eks"] == nil {
			errout("no extents\n")
			return
		}
		eks = data["eks"].([]interface{})
	}
	stdout("Summary of inodeExtentInfo  :\nEks length: %v\n%s\n", len(eks), inodeExtentInfoTableHeader)
	dataPartitions, err := client.ClientAPI().GetDataPartitions(volumeName, []uint64{})
	if err != nil {
		errout("get DataPartitions failed:\n%v\n", err)
		return
	}
	var (
		fileOffset float64
		crc        float64
		total      uint64
	)
	for _, ek := range eks {
		inodeExtentInfo := ek.(map[string]interface{})
		var inodeExtentInfoView *proto.InodeExtentInfoView
		// to be compatible with dbback cluster
		if proto.IsDbBack {
			fileOffset = float64(total)
		} else {
			fileOffset = inodeExtentInfo["FileOffset"].(float64)
			crc = inodeExtentInfo["CRC"].(float64)
		}
		inodeExtentInfoView = &proto.InodeExtentInfoView{
			FileOffset:   uint64(fileOffset),
			PartitionId:  uint64(inodeExtentInfo["PartitionId"].(float64)),
			ExtentId:     uint64(inodeExtentInfo["ExtentId"].(float64)),
			ExtentOffset: uint64(inodeExtentInfo["ExtentOffset"].(float64)),
			Size:         uint64(inodeExtentInfo["Size"].(float64)),
			CRC:          uint64(crc),
		}
		total += inodeExtentInfoView.Size
		var mediumType string
		for _, partition := range dataPartitions.DataPartitions {
			if partition.PartitionID == inodeExtentInfoView.PartitionId {
				mediumType = partition.MediumType
			}
		}
		stdout("%v\n", formatInodeExtentInfoTableRow(inodeExtentInfoView, mediumType))
	}
	return
}

func newInodeInfoCmd(client *sdk.MasterClient) *cobra.Command {
	var addr string
	var cmd = &cobra.Command{
		Use:   cmdInodeInfoUse,
		Short: cmdInodeShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err           error
				volumeName    = args[0]
				inodeStr      = args[1]
				inode, _      = strconv.Atoi(inodeStr)
				ino           = uint64(inode)
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			err = getInodeInfo(client, addr, volumeName, ino)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "address of metanode")
	return cmd
}

func newSearchInodeByPathCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdInodeSearch,
		Short: cmdInodeSearchShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err           error
				ino           uint64
				volumeName    = args[0]
				filePath       = args[1]
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			ino, err = util_sdk.GetInodeIDByPath(client.Nodes(), volumeName, filePath)

			if err != nil {
				stdout("search inode for path %s failed\n", filePath)
				return
			}

			stdout("Path: %s, Inode: %v\n", filePath, ino)
			err = getInodeInfo(client, "", volumeName, ino)
			return
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return validVols(client, toComplete), cobra.ShellCompDirectiveNoFileComp
		},
	}
	return cmd
}
