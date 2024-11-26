package mqsender

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"time"
)

const (
	CfgKeyMQProducerState = "mqProducerState"
	CfgKeyMQTopic         = "mqTopic"
	CfgKeyMQAddress       = "mqAddr"
	CfgKeyMQAppName       = "mqAppName"
)

const (
	MsgChanLength       = 1000000
	WarnChanLength      = 10000
	ProducerSendTimeout = 5 // unit: second
)

type MetadataMQProducer struct {
	ClusterName string
	ModuleName  string
	Topic       string
	MqAddr      string
	MqAppName   string
	MsgChan     chan *RaftCmdWithIndex
	Producer    sarama.SyncProducer
}

type RaftCmdWithIndex struct {
	Cmd         []byte
	OpCode      uint32
	CmdKey      string
	Index       uint64
	ClusterName string
	ModuleName  string
	NodeIp      string
	IsLeader    bool
	VolName     string
	MpId        uint64
	DpId        uint64
}

func CreateMetadataMqProducer(clusterName, moduleName string, cfg *config.Config) (mqProducer *MetadataMQProducer, err error) {
	mqAddr := cfg.GetString(CfgKeyMQAddress)
	mqTopic := cfg.GetString(CfgKeyMQTopic)
	mqAppName := cfg.GetString(CfgKeyMQAppName)
	if mqAddr == "" || mqAppName == "" || mqTopic == "" {
		return nil, fmt.Errorf("mq config is empty, MqAddr(%v), mqTopic(%v), MqAppName(%v)", mqAddr, mqTopic, mqAppName)
	}

	mqConfig := sarama.NewConfig()
	mqConfig.Producer.RequiredAcks = sarama.WaitForAll
	mqConfig.Producer.Return.Successes = true
	mqConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	mqConfig.Producer.Timeout = time.Second * ProducerSendTimeout
	mqConfig.ClientID = mqAppName

	producer, err := sarama.NewSyncProducer([]string{mqAddr}, mqConfig)
	if err != nil {
		return
	}
	mqProducer = &MetadataMQProducer{
		ClusterName: clusterName,
		ModuleName:  moduleName,
		MqAddr:      mqAddr,
		MqAppName:   mqAppName,
		Topic:       mqTopic,
		Producer:    producer,
		MsgChan:     make(chan *RaftCmdWithIndex, MsgChanLength),
	}
	mqProducer.startMQSendingTask()
	return
}

func (mp *MetadataMQProducer) startMQSendingTask() {
	go func() {
		for {
			select {
			case msg, ok := <-mp.MsgChan:
				if !ok {
					log.LogWarnf("[MQSendingTask] message channel closed.")
					return
				}
				if msg == nil {
					log.LogWarnf("[MQSendingTask] received invalid message from message channel.")
					continue
				}
				mp.SendMessage(msg)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}

func (mp *MetadataMQProducer) Shutdown() {
	log.LogInfof("[MetadataMQProducer shutdown] receive kill signal, msgChanLength(%v)", len(mp.MsgChan))
	for {
		if len(mp.MsgChan) > 0 {
			log.LogWarnf("[MetadataMQProducer shutdown] message channel still has message, msgChanLength(%v)", len(mp.MsgChan))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if mp.Producer != nil {
		_ = mp.Producer.Close()
	}
	if mp.MsgChan != nil {
		close(mp.MsgChan)
	}
}

func GetMqProducerState(cfg *config.Config) (res bool, err error) {
	cfgMqProducerState := cfg.GetString(CfgKeyMQProducerState)
	if len(cfgMqProducerState) == 0 {
		return true, nil
	}
	return strconv.ParseBool(cfgMqProducerState)
}

func (mp *MetadataMQProducer) AddMasterNodeCommand(cmd []byte, Op uint32, cmdKey string, index uint64, clusterName, ip string, isLeader bool) {
	if cmd == nil || index == 0 {
		log.LogErrorf("[AddMasterNodeCommand] invalid message, cmdEmpty(%v), index(%v), clusterName(%v), ip(%v), isLeader(%v)", cmd == nil, index, clusterName, ip, isLeader)
		warnMsg := "meta node have invalid command when sending message"
		exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), warnMsg)
		return
	}

	raftCmd := &RaftCmdWithIndex{
		Cmd:         cmd,
		OpCode:      Op,
		CmdKey:      cmdKey,
		Index:       index,
		ClusterName: clusterName,
		ModuleName:  mp.ModuleName,
		NodeIp:      ip,
		IsLeader:    isLeader,
	}
	mp.addCommandToChanel(raftCmd)
}

func (mp *MetadataMQProducer) AddMetaNodeCommand(cmd []byte, Op uint32, cmdKey string, index uint64, clusterName, volName, ip string, isLeader bool, mpId uint64) {
	if cmd == nil || index == 0 || len(volName) == 0 || mpId == 0 {
		log.LogErrorf("[AddMetaNodeCommand] invalid message, cmdEmpty(%v), index(%v), clusterName(%v), volName(%v), ip(%v), isLeader(%v), mpId(%v)", cmd == nil, index, clusterName, volName, ip, isLeader, mpId)
		warnMsg := "meta node have invalid command when sending message"
		exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), warnMsg)
		return
	}
	raftCmd := &RaftCmdWithIndex{
		Cmd:         cmd,
		OpCode:      Op,
		CmdKey:      cmdKey,
		Index:       index,
		ClusterName: clusterName,
		ModuleName:  mp.ModuleName,
		VolName:     volName,
		MpId:        mpId,
		NodeIp:      ip,
		IsLeader:    isLeader,
	}
	mp.addCommandToChanel(raftCmd)
}

func (mp *MetadataMQProducer) AddDataNodeCommand(cmd []byte, Op uint8, index uint64, clusterName, volName, ip string, isLeader bool, dpId uint64) {
	if cmd == nil || index == 0 || len(volName) == 0 || dpId == 0 {
		log.LogErrorf("[AddMetaNodeCommand] invalid message, cmdEmpty(%v), index(%v), clusterName(%v), volName(%v), ip(%v), isLeader(%v), dpId(%v)", cmd == nil, index, clusterName, volName, ip, isLeader, dpId)
		warnMsg := "meta node have invalid command when sending message"
		exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), warnMsg)
		return
	}
	raftCmd := &RaftCmdWithIndex{
		Cmd:         cmd,
		OpCode:      uint32(Op),
		Index:       index,
		ClusterName: clusterName,
		ModuleName:  mp.ModuleName,
		VolName:     volName,
		DpId:        dpId,
		NodeIp:      ip,
		IsLeader:    isLeader,
	}
	mp.addCommandToChanel(raftCmd)
}

func (mp *MetadataMQProducer) addCommandToChanel(cmd *RaftCmdWithIndex) {
	select {
	case mp.MsgChan <- cmd:
		log.LogDebugf("[addCommandMessage] add new command to channel, opCode(%v), opKey(%v), logIndex(%v), cluster(%v), module(%v), nodeIp(%v), isLeader(%v), volName(%v), mpId(%v), dpId(%v), msgChanLength(%v)",
			cmd.OpCode, cmd.CmdKey, cmd.Index, cmd.ClusterName, cmd.ModuleName, cmd.NodeIp, cmd.IsLeader, cmd.VolName, cmd.MpId, cmd.DpId, len(mp.MsgChan))
		if len(mp.MsgChan) >= WarnChanLength {
			warnMsg := fmt.Sprintf("message channel watermark has been reached %v", WarnChanLength)
			log.LogWarnf(warnMsg)
			exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), warnMsg)
		}
	default:
		warnMsg := fmt.Sprintf("message channel has been full, chanelLength(%v)", len(mp.MsgChan))
		log.LogErrorf(warnMsg)
		exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), warnMsg)
	}
}

func (mp *MetadataMQProducer) SendMessage(cmdWithIndex *RaftCmdWithIndex) {
	var err error
	var cmdData []byte

	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("[SendMessage] occurred panic, err[%v]", r)
			exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), "master send message of metadata occurred panic")
		}
		if err != nil {
			log.LogErrorf("[SendMessage] send message has exception, err[%v]", err.Error())
			exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_mqProducer", mp.ClusterName, mp.ModuleName), fmt.Sprintf("master send message of metadata has exception: %v", err.Error()))
		}
	}()

	if cmdData, err = json.Marshal(cmdWithIndex); err != nil {
		log.LogErrorf("[SendMessage] unmarshall raft command failed, err(%v)", err.Error())
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: mp.Topic,
		Value: sarama.ByteEncoder(cmdData),
	}
	pid, offset, e := mp.Producer.SendMessage(msg)
	if e != nil {
		log.LogErrorf("[SendMessage] send message has exception, logIndex(%v), pid(%v), offset(%v), error(%v)", cmdWithIndex.Index, pid, offset, e.Error())
		err = e
		return
	}
	log.LogDebugf("[SendMessage] send message success, logIndex(%v), isLeader(%v), msgChanLength(%v)", cmdWithIndex.Index, cmdWithIndex.IsLeader, len(mp.MsgChan))
}
