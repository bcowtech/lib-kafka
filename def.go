package kafka

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KAFKA_CONF_BOOTSTRAP_SERVERS = "bootstrap.servers"

	PartitionAny = kafka.PartitionAny
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

func init() {
	log.SetOutput(os.Stdout)
	log.SetPrefix("[bcowtech/kafka] ")
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lmicroseconds | log.Llongfile | log.Lmsgprefix)
}
