package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"math"
	"reflect"
	"strconv"
	"strings"
)

type CliModule struct {
	ModuleType int
	Module     string
}

type CliOperation struct {
	OpType         int // 在module下唯一即可
	Short          string
	Desc           string
	IsList         int // setConfig or setConfigList
	ReleaseSupport bool
	SparkSupport   bool
}

func NewCliOperation(opcode int, displayMsg string, isList bool, releaseSupport, sparkSupport bool) *CliOperation {
	cliOp := &CliOperation{
		OpType:         opcode,
		Short:          displayMsg,
		ReleaseSupport: releaseSupport,
		SparkSupport:   sparkSupport,
	}
	if isList {
		cliOp.IsList = 1
	}
	return cliOp
}

type valueForm int

const (
	Slider valueForm = iota
	InputBox
	DropDown
	MultiSelect
)

func manualSetValueForm(form valueForm) string {
	return strconv.Itoa(int(form))
}

type CliValueConfigList struct {
	Data  [][]*CliValueMetric
	Total int
}

type CliValueMetric struct {
	ValueName string `json:"name"`
	ValueDesc string `json:"-"`
	ValueType string `json:"type"`
	Value     string `json:"value"`
	ValueForm string `json:"-"`
	IsFilter  int32  `json:"-" gql:"output"` // 该配置项能否作为过滤条件
}

func (metric *CliValueMetric) String() string {
	if metric == nil {
		return ""
	}
	return fmt.Sprintf("%s=%s", metric.ValueName, metric.Value)
}

func (metric *CliValueMetric) SetValueName(name string) {
	metric.ValueName = name
}

func (metric *CliValueMetric) SetValueDesc(desc string) {
	metric.ValueDesc = desc
}

func (metric *CliValueMetric) SetValueType(vtype string) {
	metric.ValueType = vtype
}

func (metric *CliValueMetric) SetValue(v string) {
	metric.Value = v
}

func GetKeyValueBasicMetric() []*CliValueMetric {
	baseMetric := make([]*CliValueMetric, 0)
	keyMetric := &CliValueMetric{"key", "master接口支持的key，请依照提示类型输入value", "string", "", manualSetValueForm(DropDown), 0}
	valueMetric := &CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox), 0}
	baseMetric = append(baseMetric, keyMetric, valueMetric)
	return baseMetric
}

func GetCliOperationBaseMetrics(operation int) []CliValueMetric {
	return CliOperations[operation]
}

func FormatOperationNilData(operation int, paramTypes ...string) []*CliValueMetric {
	metrics := CliOperations[operation]
	if len(paramTypes) != len(metrics) {
		log.LogErrorf("FormatOperationNilData failed: op[%v] expect %v args but %v", GetOperationShortMsg(operation), len(metrics), len(paramTypes))
		return nil
	}
	result := make([]*CliValueMetric, 0, len(metrics))
	for index, paramType := range paramTypes {
		metric := metrics[index]
		metric.SetValueType(paramType)

		result = append(result, &metric)
	}
	log.LogDebugf("FormatOperationNilData: %v:%v metrics:%v", operation, GetOperationShortMsg(operation), result)
	return result
}

func FormatArgsToValueMetrics(operation int, args ...interface{}) []*CliValueMetric {
	// args长度及顺序和op中metrics必须一一对应
	metrics := CliOperations[operation]
	if len(args) != len(metrics) {
		log.LogErrorf("FormatArgsToValueMetrics failed: op[%v] expect %v args but %v", GetOperationShortMsg(operation), len(metrics), len(args))
		return nil
	}
	results := make([]*CliValueMetric, 0, len(args))
	for index, arg := range args {
		metric := metrics[index]

		value := reflect.ValueOf(arg)
		typeof := value.Type().String()
		metric.SetValueType(typeof)

		if strings.Contains(typeof, BasicTypeUintPrefix) {
			metric.SetValue(strconv.FormatUint(value.Uint(), 10))
		} else if strings.Contains(typeof, BasicTypeIntPrefix) {
			metric.SetValue(strconv.FormatInt(value.Int(), 10))
		}
		if strings.Contains(typeof, BasicTypeFloatPrefix) {
			metric.SetValue(strconv.FormatFloat(value.Float(), 'f', 2, 64))
		}
		if strings.Contains(typeof, BasicTypeBool) {
			metric.SetValue(strconv.FormatBool(value.Bool()))
		}
		if strings.Contains(typeof, BasicTypeString) {
			metric.SetValue(value.String())
		}
		results = append(results, &metric)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FormatArgsToValueMetrics: %v:%v, metrics:%v", operation, GetOperationShortMsg(operation), results)
	}
	return results
}

func ParseValueMetricsToArgs(operation int, metrics []*CliValueMetric) (map[string]interface{}, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("empty metrics, operation: %v:%v", operation, GetOperationShortMsg(operation))
	}
	expectArgsNum := len(CliOperations[operation])
	if expectArgsNum == 0 {
		return nil, fmt.Errorf("undefined operation code: %v:%v", operation, GetOperationShortMsg(operation))
	}
	if len(metrics) != expectArgsNum {
		return nil, fmt.Errorf("args count is inconsistent, except:actural= %v:%v, operation: %v:%v",
			expectArgsNum, len(metrics), operation, GetOperationShortMsg(operation))
	}
	args := make(map[string]interface{})
	for _, metric := range metrics {
		value := metric.Value
		vType := metric.ValueType

		var (
			arg interface{}
			err error
		)
		if strings.Contains(vType, BasicTypeUintPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeUintPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			arg, err = strconv.ParseUint(value, 10, bits)
		} else if strings.Contains(vType, BasicTypeIntPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeIntPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			arg, err = strconv.ParseInt(value, 10, bits)
		}
		if strings.Contains(vType, BasicTypeFloatPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeFloatPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			var f float64
			f, err = strconv.ParseFloat(value, bits)
			if err == nil {
				arg = math.Trunc(math.Round(f*100)) / 100
			}
		}
		if strings.Contains(vType, BasicTypeBool) {
			arg, err = strconv.ParseBool(value)
		}
		if strings.Contains(vType, BasicTypeString) {
			arg = value
		}

		if arg == nil || err != nil {
			return nil, fmt.Errorf("ParseValueMetricsToArgs: parse arg failed: %v, args(%v) err(%v)", metric, arg, err)
		}
		args[metric.ValueName] = arg
	}
	log.LogInfof("ParseValueMetricsToArgs: %v:%v, args:%v", operation, GetOperationShortMsg(operation), args)
	return args, nil
}

func ParseValueMetricsToParams(operation int, metrics []*CliValueMetric) (params map[string]string, isEmpty bool, err error) {
	if len(metrics) != len(CliOperations[operation]) {
		err = fmt.Errorf("args count is inconsistent, except:%v actural:%v, operation:%v",
			len(CliOperations[operation]), len(metrics), GetOperationShortMsg(operation))
		return
	}
	if checkEmptyMetric(metrics) {
		isEmpty = true
		return
	}
	params = make(map[string]string)
	for _, metric := range metrics {
		params[metric.ValueName] = metric.Value
	}
	return
}

func ParseKeyValueParams(operation int, metrics [][]*CliValueMetric) (params map[string]string, err error) {
	params = make(map[string]string)
	for _, metric := range metrics {
		if checkKeyValueEmpty(metric) {
			continue
		}
		var (
			key   string
			value string
		)
		for _, entry := range metric {
			entry.ValueName = strings.TrimSpace(entry.ValueName)
			entry.Value = strings.TrimSpace(entry.Value)
			switch entry.ValueName {
			case "key":
				key = entry.Value
			case "value":
				value = entry.Value
			}
		}
		params[key] = value
	}
	if len(params) == 0 {
		return nil, fmt.Errorf("empty key-value list")
	}
	return params, nil
}

func checkKeyValueEmpty(metrics []*CliValueMetric) bool {
	for _, metric := range metrics {
		if metric.Value == "" {
			return true
		}
	}
	return false
}

func checkEmptyMetric(metrics []*CliValueMetric) bool {
	for _, metric := range metrics {
		if metric.Value != "" {
			return false
		}
	}
	return true
}

// 非批量模块支持批量的的value
func IsMultiSelectValue(operation int) bool {
	switch operation {
	case OpClientReadVolRateLimit, OpClientWriteVolRateLimit:
		return true
	default:
		return false
	}
}

type CliOpMetric struct {
	OpCode string
	OpMsg  string
}

func NewCliOpMetric(code, msg string) *CliOpMetric {
	return &CliOpMetric{
		OpCode: code,
		OpMsg:  msg,
	}
}

type CliValueFilter struct {
	Key   string
	Value string
}

func FormatFiltersToMap(filters []*CliValueFilter) map[string]string {
	result := make(map[string]string)
	for _, filter := range filters {
		filter.Value = strings.TrimSpace(filter.Value)
		if filter.Value == "" {
			continue
		}
		result[filter.Key] = filter.Value
	}
	return result
}

type CliOperationHistory struct {
	XbpTicketId     uint64
	XbpUrl          string
	Cluster         string
	Module          string
	Operation       string
	Params          string // 鼠标悬浮显示
	XbpStatus       string // 审批状态
	OperationStatus int    // 执行结果
	Pin             string
	Message         string //执行失败的提示信息
	CreateTime      string
	UpdateTime      string
}

type CliOperationHistoryResponse struct {
	Total   int
	Records []*CliOperationHistory
}
