package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

func LibraryVersion() (int, string) {
	return kafka.LibraryVersion()
}
