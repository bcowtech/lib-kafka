package test

import (
	"context"
	"os"
	"testing"
	"time"

	kafka "github.com/bcowtech/lib-kafka"
)

func TestConsumer(t *testing.T) {

	c := &kafka.Consumer{
		PollingTimeout: 30 * time.Millisecond,
		MessageHandler: func(worker *kafka.ConsumeWorker, message *kafka.Message) {
			t.Logf("Message on %s: %s: %s\n", message.TopicPartition, string(message.Key), string(message.Value))
		},
		ConfigMap: &kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
			"group.id":          "gotest",
			"auto.offset.reset": "earliest",
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	err := c.Subscribe([]string{"myTopic"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer %+v", c)

	select {
	case <-ctx.Done():
		t.Logf("Consumer stopping")
		c.Close()
		return
	}
}
