package cmd

import (
	"fmt"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/spf13/cobra"
	"sort"
	"strconv"
	"strings"
)

const (
	cmdFlashNodeUse   = "flashNode [COMMAND]"
	cmdFlashNodeShort = "cluster flashNode info"
)

func newFlashNodeCommand(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdFlashNodeUse,
		Short: cmdFlashNodeShort,
	}
	cmd.AddCommand(
		newFlashNodeGetCmd(client),
		newFlashNodeDecommissionCmd(client),
		newFlashNodeListCmd(client),
		newFlashNodeSetStateCmd(client),
		newFlashNodeSetPingCmd(client),
		newFlashNodeSetStackCmd(client),
		newFlashNodeSetTimeoutCmd(client),
		newFlashNodeEvictCmd(client),
		newFlashNodeEvictAllCmd(client),
	)
	return cmd
}

func newFlashNodeGetCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpInfo + " [FlashNodeAddr] ",
		Short: "get flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var stat *proto.FlashNodeStat
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			fn, err := client.NodeAPI().GetFlashNode(args[0])
			if err != nil {
				return
			}
			fClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(args[0], ":")[0], client.FlashNodeProfPort), false)

			stat, err = fClient.GetStat()
			if err != nil {
				return
			}
			stdout("[Flash node]\n")
			stdout("%v\n", formatFlashNodeDetail(fn, stat))
		},
	}
	return cmd
}

func newFlashNodeDecommissionCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDecommission + " [FlashNodeAddr] ",
		Short: "decommission flash node by addr",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			result, err := client.NodeAPI().FlashNodeDecommission(args[0])
			if err != nil {
				return
			}
			stdout("%v\n", result)
		},
	}
	return cmd
}

func newFlashNodeListCmd(client *master.MasterClient) *cobra.Command {
	var detail, showAll, showInFlashGroup bool
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: "list all flash nodes",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			zoneFlashNodes, err := client.AdminAPI().GetAllFlashNodes(showAll)
			if err != nil {
				return
			}
			stdout("[Flash nodes]\n")
			if detail {
				stdout("%v\n", formatFlashNodeViewTableHeader())
			} else {
				stdout("%v\n", formatFlashNodeSimpleViewTableHeader())
			}
			for _, flashNodeViewInfos := range zoneFlashNodes {
				sort.Slice(flashNodeViewInfos, func(i, j int) bool {
					return flashNodeViewInfos[i].ID < flashNodeViewInfos[j].ID
				})
				for _, fn := range flashNodeViewInfos {
					if showInFlashGroup && fn.FlashGroupID == 0 {
						continue
					}
					stdout("%v\n", formatFlashNodeRowInfo(fn, detail))
				}
			}
		},
	}
	cmd.Flags().BoolVar(&showInFlashGroup, "in-group", true, fmt.Sprintf("only show flashNodes in a flash group"))
	cmd.Flags().BoolVar(&showAll, "all", true, fmt.Sprintf("show all, including inactive and disabled nodes"))
	cmd.Flags().BoolVar(&detail, "detail", false, "show detail info")
	return cmd
}

func newFlashNodeSetStateCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpSet + " [FlashNodeAddr] " + " [state]",
		Short: "set flash node state",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			err = client.NodeAPI().SetFlashNodeState(args[0], args[1])
			if err != nil {
				stdout("%v", err)
				return
			}
			stdout("set flashNode state success\n")
		},
	}
	return cmd
}

func newFlashNodeSetPingCmd(client *master.MasterClient) *cobra.Command {
	var setAll bool
	var addr string
	var cmd = &cobra.Command{
		Use:   CliOpSetPing + " [true/false]",
		Short: "enable or disable ping sort in flash node",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var enable bool
			enable, err = strconv.ParseBool(args[0])
			if err != nil {
				return
			}
			if !setAll && addr == "" {
				err = fmt.Errorf("parameter-[addr] or parameter-[all] is required")
				return
			}
			if setAll && addr != "" {
				err = fmt.Errorf("parameter-[addr] should be empty when parameter-[all] is true")
				return
			}
			if addr != "" {
				err = setFlashNodePing(addr, client.FlashNodeProfPort, enable)
				if err != nil {
					return
				}
				stdout("set flashNode:%v ping success\n", addr)
				return
			}

			zoneFlashNodes, err := client.AdminAPI().GetAllFlashNodes(true)
			if err != nil {
				return
			}
			if len(zoneFlashNodes) == 0 {
				stdout("no flashnode found")
				return
			}
			errHosts := make([]string, 0)
			var count int
			for _, flashNodeViewInfos := range zoneFlashNodes {
				for _, fn := range flashNodeViewInfos {
					err = setFlashNodePing(fn.Addr, client.FlashNodeProfPort, enable)
					if err != nil {
						errHosts = append(errHosts, fn.Addr)
						fmt.Printf("Error: %v", err)
						continue
					}
					count++
				}
			}
			stdout("set flashNodes ping finished, success:%v, failed:%v\n", count, errHosts)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "node address")
	cmd.Flags().BoolVar(&setAll, "all", false, "all:true, all nodes will be set")
	return cmd
}

func newFlashNodeSetStackCmd(client *master.MasterClient) *cobra.Command {
	var setAll bool
	var addr string
	var cmd = &cobra.Command{
		Use:   CliOpSetStack + " [true/false]",
		Short: "enable or disable stack read in flash node ",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var enable bool
			enable, err = strconv.ParseBool(args[0])
			if err != nil {
				return
			}
			if !setAll && addr == "" {
				err = fmt.Errorf("parameter-[addr] or parameter-[all] is required")
				return
			}
			if setAll && addr != "" {
				err = fmt.Errorf("parameter-[addr] should be empty when parameter-[all] is true")
				return
			}
			if addr != "" {
				err = setFlashNodeStack(addr, client.FlashNodeProfPort, enable)
				if err != nil {
					return
				}
				stdout("set flashNode:%v stack success\n", addr)
				return
			}

			zoneFlashNodes, err := client.AdminAPI().GetAllFlashNodes(true)
			if err != nil {
				return
			}
			if len(zoneFlashNodes) == 0 {
				stdout("no flashnode found")
				return
			}
			errHosts := make([]string, 0)
			var count int
			for _, flashNodeViewInfos := range zoneFlashNodes {
				for _, fn := range flashNodeViewInfos {
					err = setFlashNodeStack(fn.Addr, client.FlashNodeProfPort, enable)
					if err != nil {
						errHosts = append(errHosts, fn.Addr)
						fmt.Printf("Error: %v", err)
						continue
					}
					count++
				}
			}

			stdout("set flashNodes stack finished, success:%v, errHosts:%v\n", count, errHosts)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "node address")
	cmd.Flags().BoolVar(&setAll, "all", false, "all:true, all nodes will be set")
	return cmd
}

func newFlashNodeSetTimeoutCmd(client *master.MasterClient) *cobra.Command {
	var setAll bool
	var addr string
	var cmd = &cobra.Command{
		Use:   CliOpSetTimeout + " [ms]",
		Short: "set read timeout ms in flash node ",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var ms int
			ms, err = strconv.Atoi(args[0])
			if err != nil {
				return
			}
			if ms < 0 {
				err = fmt.Errorf("negtive ms")
				return
			}
			if !setAll && addr == "" {
				err = fmt.Errorf("parameter-[addr] or parameter-[all] is required")
				return
			}
			if setAll && addr != "" {
				err = fmt.Errorf("parameter-[addr] should be empty when parameter-[all] is true")
				return
			}
			if addr != "" {
				err = setFlashNodeReadTimeout(addr, client.FlashNodeProfPort, ms)
				if err != nil {
					return
				}
				stdout("set flashNode:%v read timeout success\n", addr)
				return
			}

			zoneFlashNodes, err := client.AdminAPI().GetAllFlashNodes(true)
			if err != nil {
				return
			}
			if len(zoneFlashNodes) == 0 {
				stdout("no flashnode found")
				return
			}
			errHosts := make([]string, 0)
			var count int
			for _, flashNodeViewInfos := range zoneFlashNodes {
				for _, fn := range flashNodeViewInfos {
					err = setFlashNodeReadTimeout(fn.Addr, client.FlashNodeProfPort, ms)
					if err != nil {
						errHosts = append(errHosts, fn.Addr)
						fmt.Printf("Error: %v", err)
						continue
					}
					count++
				}
			}
			stdout("set flashNodes read timeout finished, success:%v, failed:%v\n", count, errHosts)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "node address")
	cmd.Flags().BoolVar(&setAll, "all", false, "all:true, all nodes will be set")
	return cmd
}

func newFlashNodeEvictCmd(client *master.MasterClient) *cobra.Command {
	var addr string
	var inode uint64
	var cmd = &cobra.Command{
		Use:   CliOpEvict + " [VOLUME]",
		Short: "evict cache",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var volume string
			var flashnodeZone map[string][]*proto.FlashNodeViewInfo
			defer func() {
				if err != nil {
					errout(err.Error())
				}
			}()
			volume = args[0]
			_, err = client.AdminAPI().GetVolumeSimpleInfo(volume)
			if err != nil {
				return
			}
			if inode == 0 && addr == "" {
				errout("--addr or --inode is needed, evict [volume] --addr={}; evict [volume] --inode={}")
			}
			flashnodeZone, err = client.AdminAPI().GetAllFlashNodes(false)
			if err != nil {
				return
			}
			if inode == 0 {
				for _, fZone := range flashnodeZone {
					for _, node := range fZone {
						if addr != "" && node.Addr != addr {
							continue
						}
						flashClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(node.Addr, ":")[0], client.FlashNodeProfPort), false)
						e := flashClient.EvictVol(volume)
						if e != nil {
							stdout(e.Error())
						}
					}
				}
				return
			}
			var leader string
			var mpID uint64
			leader, mpID, err = util_sdk.LocateInode(inode, client, volume)
			if err != nil {
				return
			}
			mtClient := meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(leader, ":")[0], client.MetaNodeProfPort), false)
			_, err = mtClient.GetInode(mpID, inode)
			if err != nil {
				return
			}
			for _, fZone := range flashnodeZone {
				for _, node := range fZone {
					if addr != "" && node.Addr != addr {
						continue
					}
					flashClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(node.Addr, ":")[0], client.FlashNodeProfPort), false)
					e := flashClient.EvictInode(volume, inode)
					if e != nil {
						stdout(e.Error())
					}
				}
			}
		},
	}
	cmd.Flags().StringVar(&addr, "addr", "", "flash node address")
	cmd.Flags().Uint64Var(&inode, "inode", 0, "inode")
	return cmd
}

func newFlashNodeEvictAllCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpEvictAll + " [ADDRESS]",
		Short: "evict all cache",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var addr string
			defer func() {
				if err != nil {
					errout(err.Error())
				}
			}()
			addr = args[0]
			if addr == "" {
				errout("must specify addr, --addr")
			}
			_, err = client.NodeAPI().GetFlashNode(addr)
			if err != nil {
				return
			}
			flashClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], client.FlashNodeProfPort), false)
			err = flashClient.EvictAll()
			return
		},
	}
	return cmd
}
