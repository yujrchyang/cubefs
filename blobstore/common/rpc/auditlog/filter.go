// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package auditlog

import (
	"container/heap"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// LogFilter filter log with queries.
// Filter 返回 true 则跳过不输出这条日志
type LogFilter interface {
	Filter(*AuditLog) bool
}

// FilterConfig config of log filter.
// Returns Must && MustNot && Should.
type FilterConfig struct {
	Must    Conditions `json:"must"`     // all conditions must be true.
	MustNot Conditions `json:"must_not"` // all conditions must be false.
	Should  Conditions `json:"should"`   // true only if any of condition is true.
}

// Conditions compare condition.
// term   // equal
// match  // contains
// regexp // regexp
// range  // int range, multi-ranges like "200-203,410-420,500-"
// map[field]string or map[field][]string
type Conditions map[string]map[string]interface{}

type condFunc func(*AuditLog) bool

type condition struct {
	Func     condFunc
	Priority int64
}

type condQueue []condition

func (q condQueue) Len() int            { return len(q) }
func (q condQueue) Less(i, j int) bool  { return q[i].Priority > q[j].Priority }
func (q condQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *condQueue) Push(x interface{}) { *q = append(*q, x.(condition)) }
func (q *condQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

type filters struct {
	filters []*filter
}

func newLogFilter(cfgs []FilterConfig) (LogFilter, error) {
	fs := &filters{}
	// 遍历最外层 filters 数组
	for _, cfg := range cfgs {
		// filters 数组中每个元素生成一个过滤函数，添加到 filters 切片中
		f, err := newFilter(cfg)
		if err != nil {
			return nil, err
		}
		fs.filters = append(fs.filters, f)
	}
	return fs, nil
}

// 多个 filter 块之间，只要一个 filter 返回 ture，则不输出这条日志，即为 或 的关系
// 一个 filter 中，满足 (must && !mustnot) || should 则返回 true，否则返回 false
func (fs *filters) Filter(log *AuditLog) bool {
	// 遍历所有的过滤条件，有一个成立则返回 true，即该条 log 不显示
	for idx := range fs.filters {
		if fs.filters[idx].Filter(log) {
			return true
		}
	}
	return false
}

// 带优先级动态调整的审计日志过滤器
type filter struct {
	lock  sync.RWMutex
	and   condQueue
	or    condQueue
	count int64
	reset int64
}

func getConditions(operation, field string, value interface{}) ([]condFunc, error) {
	var values []string
	switch val := value.(type) {
	case string:
		values = []string{val}
	case []string:
		values = val[:]
	case []interface{}:
		for idx := range val {
			if x, ok := val[idx].(string); ok {
				values = append(values, x)
			}
		}
	default:
		return nil, fmt.Errorf("invalid value:%v", value)
	}
	funcs := make([]condFunc, 0, len(values))
	for _, val := range values {
		cond, err := parse(operation, field, val)
		if err != nil {
			return nil, err
		}
		funcs = append(funcs, cond)
	}
	return funcs, nil
}

// FilterConfig 是一个 and/or 条件组合
// operation - term/match/regexp/range
// field - status_code/duration/path
// value - ...
//
// must 和 mustnot 为 and
// should 为 or
func newFilter(cfg FilterConfig) (*filter, error) {
	f := new(filter)
	f.lock.Lock()
	defer f.lock.Unlock()
	for operation, fields := range cfg.Must {
		for field, value := range fields {
			conds, err := getConditions(operation, field, value)
			if err != nil {
				return nil, err
			}
			for idx := range conds {
				f.and = append(f.and, condition{Func: conds[idx]})
			}
		}
	}
	for operation, fields := range cfg.MustNot {
		for field, value := range fields {
			conds, err := getConditions(operation, field, value)
			if err != nil {
				return nil, err
			}
			for idx := range conds {
				f.and = append(f.and, condition{
					Func: func(log *AuditLog) bool { return !conds[idx](log) },
				})
			}
		}
	}
	for operation, fields := range cfg.Should {
		for field, value := range fields {
			conds, err := getConditions(operation, field, value)
			if err != nil {
				return nil, err
			}
			for idx := range conds {
				f.or = append(f.or, condition{Func: conds[idx]})
			}
		}
	}
	return f, nil
}

func (f *filter) Filter(log *AuditLog) bool {
	// 不是首要条件命中时会给 count+1，当非首要条件到达 10000 时通过 heap.Init 根据
	// 每个条件的 Priority 重新排序，命中次数多的排在前边，以优化条件判断
	if atomic.LoadInt64(&f.count) > 10000 {
		atomic.StoreInt64(&f.count, 0)
		atomic.AddInt64(&f.reset, 1)
		// reset priority
		f.lock.Lock()
		heap.Init(&f.and)
		heap.Init(&f.or)
		f.lock.Unlock()
	}

	f.lock.RLock()
	defer f.lock.RUnlock()

	// and 条件，只要有一个条件不满足则返回 false，此时会输出这条 log
	for idx := range f.and {
		if !f.and[idx].Func(log) {
			atomic.AddInt64(&f.and[idx].Priority, 1)
			if idx > 0 {
				atomic.AddInt64(&f.count, 1)
			}
			return false
		}
	}

	// and 所有条件都满足，再来判断 or 条件，如果没有指定 or，则直接返回 true，即不输出这条日志
	// 只要有一个 or 条件满足则返回 true，即不输出这条日志
	// or 条件都不满足，返回 false，即输出这条日志
	hasor := len(f.or) > 0
	for idx := range f.or {
		if f.or[idx].Func(log) {
			atomic.AddInt64(&f.or[idx].Priority, 1)
			if idx > 0 {
				atomic.AddInt64(&f.count, 1)
			}
			return true
		}
	}
	return !hasor
}

func parse(operation, field, value string) (condFunc, error) {
	var getStrField func(log *AuditLog) string
	var getIntField func(log *AuditLog) int64

	switch field {
	case "req_type", "ReqType":
		getStrField = func(log *AuditLog) string { return log.ReqType }
	case "module", "Module":
		getStrField = func(log *AuditLog) string { return log.Module }
	case "method", "Method":
		getStrField = func(log *AuditLog) string { return log.Method }
	case "path", "Path":
		getStrField = func(log *AuditLog) string { return log.Path }
	case "req_header", "ReqHeader":
		getStrField = func(log *AuditLog) string { return string(log.ReqHeader.Encode()) }
	case "req_params", "ReqParams":
		getStrField = func(log *AuditLog) string { return log.ReqParams }
	case "resp_header", "RespHeader":
		getStrField = func(log *AuditLog) string { return string(log.RespHeader.Encode()) }
	case "resp_body", "RespBody":
		getStrField = func(log *AuditLog) string { return log.RespBody }

	case "start_time", "StartTime":
		getIntField = func(log *AuditLog) int64 { return log.StartTime }
	case "status_code", "StatusCode":
		getIntField = func(log *AuditLog) int64 { return int64(log.StatusCode) }
	case "resp_length", "RespLength":
		getIntField = func(log *AuditLog) int64 { return log.RespLength }
	case "duration", "Duration":
		getIntField = func(log *AuditLog) int64 { return log.Duration }
	default:
		// do nothing
	}

	if getStrField != nil {
		switch operation {
		case "term":
			return func(log *AuditLog) bool { return getStrField(log) == value }, nil
		case "match":
			return func(log *AuditLog) bool { return strings.Contains(getStrField(log), value) }, nil
		case "regexp":
			reg, err := regexp.Compile(value)
			if err != nil {
				return nil, err
			}
			return func(log *AuditLog) bool { return reg.MatchString(getStrField(log)) }, nil
		default:
			return nil, fmt.Errorf("unsupported operation:%s", operation)
		}
	}
	if getIntField != nil {
		switch operation {
		case "term":
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid value:%s", value)
			}
			return func(log *AuditLog) bool { return getIntField(log) == val }, nil
		case "range":
			strs := strings.Split(value, ",")
			ranges := make([][2]int64, 0, len(strs))
			for _, str := range strs {
				se := strings.Split(str, "-")
				if len(se) != 2 {
					return nil, fmt.Errorf("invalid range:%s", value)
				}

				// start 未指定时为 int64 的最小值
				// end 未指定时为 int64 的最大值
				start := int64(math.MinInt64)
				if se[0] != "" {
					i, err := strconv.ParseInt(se[0], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("invalid range:%s", str)
					}
					start = i
				}
				end := int64(math.MaxInt64)
				if se[1] != "" {
					i, err := strconv.ParseInt(se[1], 10, 64)
					if err != nil {
						return nil, fmt.Errorf("invalid range:%s", str)
					}
					end = i
				}
				ranges = append(ranges, [2]int64{start, end})
			}

			return func(log *AuditLog) bool {
				val := getIntField(log)
				for idx := range ranges {
					// 范围内返回 true，左闭右闭区间，因此 start 和 end 可以是同一个值
					if val >= ranges[idx][0] && val <= ranges[idx][1] {
						return true
					}
				}
				return false
			}, nil
		default:
			return nil, fmt.Errorf("unsupported operation:%s", operation)
		}
	}

	return nil, fmt.Errorf("unsupported field:%s operation:%s", field, operation)
}
