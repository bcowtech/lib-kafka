package kafka

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KAFKA_CONF_BOOTSTRAP_SERVERS = "bootstrap.servers"
	KAFKA_CONF_GROUP_ID          = "group.id"

	LOGGER_PREFIX string = "[bcowtech/lib-kafka] "

	PartitionAny = kafka.PartitionAny
)

var (
	logger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	ConfigMap             = kafka.ConfigMap
	ConsumerGroupMetadata = kafka.ConsumerGroupMetadata
	Error                 = kafka.Error
	ErrorCode             = kafka.ErrorCode
	Event                 = kafka.Event
	Message               = kafka.Message
	Offset                = kafka.Offset
	RebalanceCb           = kafka.RebalanceCb
	TopicPartition        = kafka.TopicPartition

	MessageHandleProc func(worker *ConsumeWorker, message *Message)
	ErrorHandleProc   func(err kafka.Error) (disposed bool)
)

type (
	ForwarderOption = ProducerOption
)
