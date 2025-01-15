package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func AddCheckRule(tableName string, rule *proto.CheckRule) (err error) {
	sqlCmd := fmt.Sprintf("insert into %s (task_type, cluster_id, rule_type, rule_value)" +
		" values(?, ?, ?, ?)", tableName)
	args := make([]interface{}, 0)
	args = append(args, int8(rule.WorkerType))
	args = append(args, rule.ClusterID)
	args = append(args, rule.RuleType)
	args = append(args, rule.RuleValue)


	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("")
		return
	}
	return
}

func SelectCheckRule(tableName string, clusterID string) (rules []*proto.CheckRule, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select task_type, cluster_id, rule_type, rule_value from %s where cluster_id = ? ", tableName)
	rows, err = db.Query(sqlCmd, clusterID)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		rule := &proto.CheckRule{}
		err = rows.Scan(&rule.WorkerType, &rule.ClusterID, &rule.RuleType, &rule.RuleValue)
		if err != nil {
			return
		}
		rules = append(rules, rule)
	}
	return
}

func UpdateCheckRule(tableName string, id int, ruleValue string) (err error) {
	sqlCmd := fmt.Sprintf("update %s set rule_value = ? where id = ?", tableName)
	args := make([]interface{}, 0)
	args = append(args, id)
	args = append(args, ruleValue)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("execute update check rule failed, id: %v, rule_value: %v, err: %v", id, ruleValue, err)
		return
	}
	return
}

func DeleteCheckRule(tableName string, id int) (err error) {
	sqlCmd := fmt.Sprintf("delete from %s where id = ?", tableName)
	args := make([]interface{}, 0)
	args = append(args, id)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("delete check rule failed, id: %v, err: %v", id, err)
		return
	}
	return
}