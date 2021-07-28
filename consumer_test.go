package kafka

import (
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	var (
		c   *Consumer
		err error
	)
	c = &Consumer{
		ConfigMap: &ConfigMap{},
	}
	err = c.Subscribe([]string{"gotest1", "gotest2"}, nil)
	t.Logf("%+v\n", err)
	if err == nil {
		t.Fatalf("Expected c.Subscribe() to fail without group.id")
	}

	c = &Consumer{
		PollingTimeout: 30 * time.Millisecond,
		ConfigMap: &ConfigMap{
			"group.id":                 "gotest",
			"socket.timeout.ms":        10,
			"session.timeout.ms":       10,
			"enable.auto.offset.store": false, // permit StoreOffsets()
		},
	}
	err = c.Subscribe([]string{"gotest1", "gotest2", "gotest3"}, nil)
	if err != nil {
		t.Fatalf("%s", err)
	}
	t.Logf("Consumer %+v", c)
	err = c.Subscribe([]string{"gotest1", "gotest2"}, nil)
	if err == nil {
		t.Fatal("Expected Subscribe() to fail")
	}
	t.Logf("%+v", err)

	c.Stop()
	err = c.Subscribe([]string{"gotest1", "gotest2"}, nil)
	if err == nil {
		t.Fatal("Expected Subscribe() to fail")
	}
	t.Logf("%s", err)
	c.Stop()
}
