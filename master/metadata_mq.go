package master

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
)

type MetadataMQProducer struct {
	s         *Server
	topic     string
	mqAddr    string
	mqAppName string
	producer  sarama.SyncProducer
}

type RaftCmdWithIndex struct {
	Cmd   *RaftCmd
	Index uint64
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
	}
	m.mqProducer = mqProducer
	return nil
}

func (mp *MetadataMQProducer) SendMessage(cmd *RaftCmd, index uint64) {
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

	cmdIndex := RaftCmdWithIndex{
		Cmd:   cmd,
		Index: index,
	}
	if cmdData, err = json.Marshal(cmdIndex); err != nil {
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
}
