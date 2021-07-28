package test

import (
	"os"
	"testing"
	"time"

	kafka "github.com/bcowtech/lib-kafka"
)

func TestProducer(t *testing.T) {
	p, err := kafka.NewProducer(&kafka.ProducerOption{
		FlushTimeout: 3 * time.Second,
		PingTimeout:  3 * time.Second,
		ConfigMap: &kafka.ConfigMap{
			"client.id":         "gotest",
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.WriteMessage(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}
	p.Close()
}
