package cli

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
)

func (cli *CliService) GetKeyValueConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	table := &model.KeyValueOperation{}
	keyValueOperation, err := table.GetOperation(operation)
	if err != nil {
		return
	}

	defer func() {
		msg := fmt.Sprintf("GetKeyValueConfigList: cluster(%v) operation(%v)", cluster, keyValueOperation.URI)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)
	result = append(result, cproto.GetKeyValueBasicMetric())
	return
}

func (cli *CliService) SetKeyValueConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (operationMsg string, err error) {
	table := &model.KeyValueOperation{}
	keyValueOperation, err := table.GetOperation(operation)
	if err != nil {
		return
	}
	operationMsg = keyValueOperation.URI

	params, err := cproto.ParseKeyValueParams(operation, metrics)
	if err != nil {
		return
	}

	defer func() {
		msg := fmt.Sprintf("SetKeyValueConfigList: cluster[%v] operation(%v) metrics(%v)", cluster, operationMsg, params)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
	}()
	if !skipXbp {
		err = cli.createXbpApply(ctx, cluster, cproto.KeyValueModuleType, operation, metrics, nil, &operationMsg, true)
	}

	err = cli.sendKeyValueRequest(cluster, keyValueOperation, params)
	return operationMsg, err
}

// todo: 无法做参数校验(数据库表定义参数校验规则)
func (cli *CliService) sendKeyValueRequest(cluster string, operation *model.KeyValueOperation, params map[string]string) error {
	clusterInfo := cli.api.GetClusterInfo(cluster)
	if clusterInfo == nil {
		return fmt.Errorf("cluster[%s] not found!", cluster)
	}

	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, operation.URI))
	for key, value := range params {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, clusterInfo.IsRelease)
	if err != nil {
		log.LogErrorf("sendKeyValueRequest failed: cluster(%v) operation(%v) params(%v) err(%v)", cluster, operation.URI, params, err)
		return err
	}
	log.LogInfof("sendKeyValueRequest: cluster(%v) operation(%v) params(%v) msg(%v)", cluster, operation.URI, params, string(data))
	return nil
}
