package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdZoneUse            = "zone [command]"
	cmdZoneShort          = "Manage zones"
	cmdZoneInfoUse        = "info [Zone Name]"
	cmdZoneInfoShort      = "Show zone information"
	cmdZoneSetRegionShort = "Set region name of the zone"
	cmdZoneListShort      = "List cluster zones"
)

func newZoneCmd(mc *master.MasterClient) *cobra.Command {
	client = mc
	var cmd = &cobra.Command{
		Use:   cmdZoneUse,
		Short: cmdZoneShort,
		Args:  cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(
		newZoneListCmd(client),
		newZoneInfoCmd(client),
		//newZoneSetRegionCmd(client),
	)
	return cmd
}

func newZoneInfoCmd(client *master.MasterClient) *cobra.Command {
	var optMetaDetail, optDataDetail, optEcDetail bool
	var cmd = &cobra.Command{
		Use:   cmdZoneInfoUse,
		Short: cmdZoneInfoShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				zv  *proto.ZoneView
			)
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			var zoneName = args[0]
			zoneViews, err := client.AdminAPI().ZoneList()
			if err != nil {
				return
			}
			for _, zoneView := range zoneViews {
				if zoneView.Name == zoneName {
					zv = zoneView
					break
				}
			}
			if zv != nil {
				stdout("ZoneInfo:\n%s\n", formatZoneView(zv))
			} else {
				stdout("ZoneInfo:%s not exist.\n", zoneName)
			}

			if !optMetaDetail && !optDataDetail && !optEcDetail {
				return
			}

			var view *proto.TopologyView
			if view, err = client.AdminAPI().GetTopology(); err != nil {
				return
			}

			var zoneView *proto.ZoneView
			for _, zoneInfo := range view.Zones {
				if zoneInfo.Name == zoneName {
					zoneView = zoneInfo
					break
				}
			}

			if zoneView == nil {
				err = fmt.Errorf("zone %s not exist", zoneName)
				return
			}


			if optDataDetail {
				dataNodes := make([]proto.NodeView, 0)
				for _, nodeSet := range zoneView.NodeSet {
					dataNodes = append(dataNodes, nodeSet.DataNodes...)
				}
				sort.Slice(dataNodes, func(i, j int) bool {
					return dataNodes[i].ID < dataNodes[j].ID
				})

				fmt.Printf("Data Nodes (cnt: %v):\n", len(dataNodes))
				stdout("%s\n", formatNodeViewTableHeader())
				for _, node := range dataNodes {
					stdout("%s\n", formatNodeView(&node, true))
				}

			}

			if optMetaDetail {
				metaNodes := make([]proto.NodeView, 0)
				for _, nodeSet := range zoneView.NodeSet {
					metaNodes = append(metaNodes, nodeSet.MetaNodes...)
				}
				sort.SliceStable(metaNodes, func(i, j int) bool {
					return metaNodes[i].ID < metaNodes[j].ID
				})

				fmt.Printf("Meta Nodes (cnt: %v):\n", len(metaNodes))
				stdout("%s\n", formatNodeViewTableHeader())
				for _, node := range metaNodes {
					stdout("%s\n", formatNodeView(&node, true))
				}
			}
			return
		},
	}
	cmd.Flags().BoolVarP(&optMetaDetail, "list-meta-nodes", "m", false, "Display all meta node detail information")
	cmd.Flags().BoolVarP(&optDataDetail, "list-data-nodes", "d", false, "Display all meta node detail information")
	return cmd
}

func newZoneListCmd(client *master.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpList,
		Short: cmdZoneListShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			zoneViews, err := client.AdminAPI().ZoneList()
			if err != nil {
				return
			}
			stdout("%v\n", zoneInfoTableHeader)
			for _, zoneView := range zoneViews {
				stdout("%v\n", formatZoneInfoTableRow(zoneView))
			}
			return
		},
	}
	return cmd
}

func newZoneSetRegionCmd(client *master.MasterClient) *cobra.Command {
	var (
		confirmString = new(strings.Builder)
		optYes        bool
	)
	var cmd = &cobra.Command{
		Use:   "setRegion [Zone Name] [Region Name]",
		Short: cmdZoneSetRegionShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var zoneName = args[0]
			var regionName = args[1]
			var isChange = false
			defer func() {
				if err != nil {
					errout("Error: %v", err)
				}
			}()
			confirmString.WriteString("Zone region name change:\n")
			confirmString.WriteString(fmt.Sprintf("  Name                : %v\n", zoneName))
			if regionName != "" {
				isChange = true
				confirmString.WriteString(fmt.Sprintf("  region name         : %s\n", regionName))
			}
			if !isChange {
				stdout("No changes has been set.\n")
				return
			}
			// ask user for confirm
			if !optYes {
				stdout(confirmString.String())
				stdout("\nConfirm (yes/no)[yes]: ")
				var userConfirm string
				_, _ = fmt.Scanln(&userConfirm)
				if userConfirm != "yes" && len(userConfirm) != 0 {
					err = fmt.Errorf("Abort by user.\n")
					return
				}
			}
			err = client.AdminAPI().SetZoneRegion(zoneName, regionName)
			if err != nil {
				return
			}
			stdout("Zone region name has been set successfully.\n")
			return
		},
	}
	cmd.Flags().BoolVarP(&optYes, "yes", "y", false, "Answer yes for all questions")
	return cmd
}
