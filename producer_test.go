package kafka

import (
	"testing"
	"time"
)

func TestProduce_DryTest(t *testing.T) {
	// see https://github.com/confluentinc/confluent-kafka-go/blob/master/kafka/producer_test.go
	p, err := NewProducer(&ProducerOption{
		FlushTimeout: 3 * time.Second,
		PingTimeout:  3 * time.Second,
		ConfigMap: &ConfigMap{
			"socket.timeout.ms":  10,
			"message.timeout.ms": 10,
			// "go.delivery.report.fields": "key,value,headers",
		},
	})
	if err != nil {
		t.Errorf("%s", err)
	}

	expMsgCnt := 0

	drChan := make(chan Event, 10)

	topic1 := "gotest"
	topic2 := "gotest2"

	// Produce with function, DR on passed drChan
	err = p.WriteMessage(&Message{TopicPartition: TopicPartition{Topic: &topic1, Partition: 0},
		Value: []byte("Own drChan"), Key: []byte("This is my key")},
		drChan)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}

	// Produce with function, use default DR channel (Events)
	err = p.WriteMessage(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0},
		Value: []byte("Events DR"), Key: []byte("This is my key")},
		nil)
	if err != nil {
		t.Errorf("Produce failed: %s", err)
	}
	expMsgCnt++

	// Produce with function and timestamp,
	// success depends on librdkafka version
	err = p.WriteMessage(&Message{TopicPartition: TopicPartition{Topic: &topic2, Partition: 0}, Timestamp: time.Now()}, nil)
	numver, strver := LibraryVersion()
	t.Logf("Produce with timestamp on %s returned: %s", strver, err)
	if numver < 0x00090400 {
		if err == nil || err.(Error).Code() != ErrNotImplemented {
			t.Errorf("Expected Produce with timestamp to fail with ErrNotImplemented on %s (0x%x), got: %s", strver, numver, err)
		}
	} else {
		if err != nil {
			t.Errorf("Produce with timestamp failed on %s: %s", strver, err)
		}
	}
	if err == nil {
		expMsgCnt++
	}

	//
	// Now wait for messages to time out so that delivery reports are triggered
	//

	// drChan (1 message)
	ev := <-drChan
	m := ev.(*Message)
	if string(m.Value) != "Own drChan" {
		t.Errorf("DR for wrong message (wanted 'Own drChan'), got %s",
			string(m.Value))
	} else if m.TopicPartition.Error == nil {
		t.Errorf("Expected error for message")
	} else {
		t.Logf("Message %s", m.TopicPartition)
	}
	close(drChan)

	p.Close()
}
