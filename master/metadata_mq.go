package master

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"time"
)

const (
	MsgChanLength       = 1000000
	WarnChanLength      = 10000
	ProducerSendTimeout = 5 // unit: second
)

type MetadataMQProducer struct {
	s         *Server
	topic     string
	mqAddr    string
	mqAppName string
	msgChan   chan *RaftCmdWithIndex
	producer  sarama.SyncProducer
}

type RaftCmdWithIndex struct {
	Cmd          *RaftCmd
	Index        uint64
	IsLeader     bool
	ClusterName  string
	MasterNodeIp string
}

func (m *Server) initMetadataMqProducer(cfg *config.Config) (err error) {
	mqAddr := cfg.GetString(CfgKeyMQAddress)
	mqTopic := cfg.GetString(CfgKeyMQTopic)
	mqAppName := cfg.GetString(CfgKeyMQAppName)
	if mqAddr == "" || mqAppName == "" || mqTopic == "" {
		return fmt.Errorf("mq config is empty, mqAddr(%v), mqTopic(%v), mqAppName(%v)", mqAddr, mqTopic, mqAppName)
	}

	mqConfig := sarama.NewConfig()
	mqConfig.Producer.RequiredAcks = sarama.WaitForAll
	mqConfig.Producer.Return.Successes = true
	mqConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	mqConfig.Producer.Timeout = time.Second * ProducerSendTimeout
	mqConfig.ClientID = mqAppName

	producer, err := sarama.NewSyncProducer([]string{mqAddr}, mqConfig)
	if err != nil {
		return err
	}
	mqProducer := &MetadataMQProducer{
		s:         m,
		mqAddr:    mqAddr,
		mqAppName: mqAppName,
		topic:     mqTopic,
		producer:  producer,
		msgChan:   make(chan *RaftCmdWithIndex, MsgChanLength),
	}
	m.mqProducer = mqProducer
	m.mqProducer.startMQSendingTask()
	return nil
}

func (mp *MetadataMQProducer) startMQSendingTask() {
	go func() {
		for {
			select {
			case msg, ok := <-mp.msgChan:
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

func (mp *MetadataMQProducer) shutdown() {
	log.LogInfof("[MetadataMQProducer shutdown] receive kill signal, msgChanLength(%v)", len(mp.msgChan))
	for {
		if len(mp.msgChan) > 0 {
			log.LogWarnf("[MetadataMQProducer shutdown] message channel still has message, msgChanLength(%v)", len(mp.msgChan))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if mp.producer != nil {
		_ = mp.producer.Close()
	}
	if mp.msgChan != nil {
		close(mp.msgChan)
	}
}

func (m *Server) getMqProducerState(cfg *config.Config) (res bool, err error) {
	cfgMqProducerState := cfg.GetString(CfgKeyMQProducerState)
	if len(cfgMqProducerState) == 0 {
		return true, nil
	}
	return strconv.ParseBool(cfgMqProducerState)
}

func (mp *MetadataMQProducer) addCommandMessage(cmd *RaftCmd, index uint64, clusterName, ip string, isLeader bool) {
	cmdIndex := &RaftCmdWithIndex{
		Cmd:          cmd,
		Index:        index,
		IsLeader:     isLeader,
		ClusterName:  clusterName,
		MasterNodeIp: ip,
	}

	select {
	case mp.msgChan <- cmdIndex:
		log.LogDebugf("[addCommandMessage] add new command to channel, cmdOp(%v), cmdKey(%v), index(%v), isLeader(%v), msgChanLength(%v)",
			cmd.Op, cmd.K, index, isLeader, len(mp.msgChan))
		if len(mp.msgChan) >= WarnChanLength {
			warnMsg := fmt.Sprintf("message channel watermark has been reached %v", WarnChanLength)
			log.LogWarnf(warnMsg)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_mqProducer", mp.s.clusterName, ModuleName), warnMsg)
		}
	default:
		warnMsg := fmt.Sprintf("message channel has been full, chanelLength(%v)", len(mp.msgChan))
		log.LogErrorf(warnMsg)
		WarnBySpecialKey(fmt.Sprintf("%v_%v_mqProducer", mp.s.clusterName, ModuleName), warnMsg)
	}
}

func (mp *MetadataMQProducer) SendMessage(cmdWithIndex *RaftCmdWithIndex) {
	var err error
	var cmdData []byte

	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("[SendMessage] occurred panic, err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_mqProducer", mp.s.clusterName, ModuleName), "master send message of metadata occurred panic")
		}
		if err != nil {
			log.LogErrorf("[SendMessage] send message has exception, err[%v]", err.Error())
			WarnBySpecialKey(fmt.Sprintf("%v_%v_mqProducer", mp.s.clusterName, ModuleName), fmt.Sprintf("master send message of metadata has exception: %v", err.Error()))
		}
	}()

	if cmdData, err = json.Marshal(cmdWithIndex); err != nil {
		log.LogErrorf("[SendMessage] unmarshall raft command failed, err(%v)", err.Error())
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: mp.topic,
		Value: sarama.ByteEncoder(cmdData),
	}
	pid, offset, e := mp.producer.SendMessage(msg)
	if e != nil {
		log.LogErrorf("[SendMessage] send message has exception, msg(%v), pid(%v), offset(%v), error(%v)", msg, pid, offset, e.Error())
	}
	log.LogDebugf("[SendMessage] send message success, cmdOp(%v), cmdKey(%v), index(%v), isLeader(%v), msgChanLength(%v)",
		cmdWithIndex.Cmd.Op, cmdWithIndex.Cmd.K, cmdWithIndex.Index, cmdWithIndex.IsLeader, len(mp.msgChan))
}
