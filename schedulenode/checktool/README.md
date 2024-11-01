## StorageBot

StorageBot（或叫checktool）提供集群健康检查，风险预警，故障自动处理等功能

## 依赖的插件

### 1 mysql

- 高负载机器重启 （目前该功能已经废弃）
- 查询mdc存入storage_sre数据库的机器信息，对磁盘使用率超过一定比例的报警
- 继承自父类BaseWorker的数据库schedule_db（目前只读取了配置，并未连接和使用数据库，以后可能用到）

测试库：mysql -h 11.22.154.86 -u test1 -P 3306 -p --ssl-mode=DISABLED

### 2 ump

- 自定义方法报警
- 报警记录查询功能(openAPI:api.jcd-gateway.jd.com)

### 3 Open Api

- 京Me: open.timline.jd.com,提供咚咚报警功能，只要邀请存储机器人进群，就可以指定群号将报警推送到指定群
- JDOS: http://api.jcd-gateway.jd.com,检查MasterLB和ObjectNode pod信息, 因为只有查询功能，开发和生产环境可以暂时用同一个账号
- XBP: xbp-api.jd.com xbp-api-pre.jd.com（预发环境）(创建下线磁盘xbp申请、查询xbp申请单状态)


### 4 Email

邮件发送可用tiny extent检查结果
- smtpHost: mx.jd.local 
- smtpPort: 25