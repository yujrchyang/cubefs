package cli

import (
	"context"
	"fmt"
	"strings"

	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetFileMigrateConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		msg := fmt.Sprintf("GetFileMigrateConfigList: cluster(%v) operation(%v)", cluster, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%v, err(%v)", msg, err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)
	switch operation {
	case cproto.OpMigrationConfigList:
		return cli.getMigrateConfigList(cluster, operation)
	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}
	return
}

func (cli *CliService) SetFileMigrateConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetFileMigrateConfigList cluster(%v) operation(%v)", cluster, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%v err(%v)", msg, err)
		} else {
			log.LogInfof("%s success", msg)
		}
	}()
	metrics, err = cli.getMigrateConfigChangedMetrics(cluster, operation, metrics)
	if err != nil {
		return
	}
	params := make([]map[string]string, 0)
	for _, metric := range metrics {
		var (
			param   map[string]string
			isEmpty bool
		)
		// 校验非空
		param, isEmpty, err = cproto.ParseValueMetricsToParams(operation, metric)
		if err != nil || isEmpty {
			continue
		}
		// 校验有效
		_, err = cproto.ParseValueMetricsToArgs(operation, metric)
		if err != nil {
			return err
		}
		// 非空，且有效。才加入到list中
		params = append(params, param)
	}

	switch operation {
	case cproto.OpMigrationConfigList:
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.FileMigrateModuleType, operation, metrics, nil, nil, true)
		}
		err = cli.batchUpdateMigrateConfig(cluster, params)

	default:
	}
	return
}

func (cli *CliService) getMigrateConfigChangedMetrics(cluster string, operation int, metrics [][]*cproto.CliValueMetric) ([][]*cproto.CliValueMetric, error) {
	var (
		result = make([][]*cproto.CliValueMetric, 0)
	)
	configList, err := cli.api.MigrateConfigList(cluster)
	if err != nil {
		return nil, err
	}
	configMap := make(map[string]*cproto.MigrateConfig)
	for _, config := range configList {
		configMap[config.VolName] = config
	}
	for _, metric := range metrics {
		args, e1 := cproto.ParseValueMetricsToArgs(operation, metric)
		if e1 != nil {
			return nil, e1
		}
		config := new(cproto.MigrateConfig)
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "volume":
				volume := args[baseMetric.ValueName].(string)
				if volume == "" {
					// 迁移vol必填
					return nil, fmt.Errorf("请指定要设置迁移的vol！")
				}
				config.VolName = volume
			case "smart":
				smart := args[baseMetric.ValueName].(bool)
				if smart {
					config.Smart = 1
				} else {
					config.Smart = 0
				}
			case "compact":
				compact := args[baseMetric.ValueName].(bool)
				if compact {
					config.Compact = 1
				} else {
					config.Compact = 0
				}
			case "migrationBack":
				migBack := args[baseMetric.ValueName].(bool)
				if migBack {
					config.MigrationBack = 1
				} else {
					config.MigrationBack = 0
				}
			case "rules":
				rules := args[baseMetric.ValueName].(string)
				config.SmartRules = rules
			case "hddDirs":
				hddDirs := args[baseMetric.ValueName].(string)
				config.HddDirs = hddDirs
			}
		}
		// 判断是否已有，是否修改
		// 只比较一个vol的情况 批量的直接执行
		log.LogDebugf("origin:%v config:%v", configMap[config.VolName], config)
		if originConfig, ok := configMap[config.VolName]; ok {
			if originConfig.Smart == config.Smart && originConfig.SmartRules == config.SmartRules &&
				originConfig.HddDirs == config.HddDirs && originConfig.Compact == config.Compact && originConfig.MigrationBack == config.MigrationBack {
				continue
			}
		}
		result = append(result, metric)
	}
	log.LogInfof("getMigrateConfigChangedMetrics: len(metrics)=%v, metrics: %v", len(result), result)
	return result, nil
}

func (cli *CliService) getMigrateConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	configList, err := cli.api.MigrateConfigList(cluster)
	if err != nil {
		return nil, err
	}
	result = make([][]*cproto.CliValueMetric, 0)
	if len(configList) == 0 {
		result = append(result, cproto.FormatOperationNilData(operation, "string", "bool", "bool", "bool", "string", "string"))
		return
	}
	for _, config := range configList {
		var (
			smart   bool
			compact bool
			migBack bool
		)
		if config.Smart > 0 {
			smart = true
		}
		if config.Compact > 0 {
			compact = true
		}
		if config.MigrationBack > 0 {
			migBack = true
		}
		result = append(result, cproto.FormatArgsToValueMetrics(operation, config.VolName, smart, compact, migBack, config.SmartRules, config.HddDirs))
	}
	return
}

func (cli *CliService) batchUpdateMigrateConfig(cluster string, params []map[string]string) error {
	var errResult error
	for _, param := range params {
		volList := strings.Split(param["volume"], ",")
		delete(param, "volume")
		for _, vol := range volList {
			err := cli.api.CreateOrUpdateMigrateConfig(cluster, vol, param)
			if err != nil {
				errResult = fmt.Errorf("%v, %v", errResult, err)
			}
		}
	}
	return errResult
}
