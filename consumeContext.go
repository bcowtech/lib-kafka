package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var _ MessageHandleProc = StopRecursiveForwardUnhandledMessageHandler

func StopRecursiveForwardUnhandledMessageHandler(ctx *ConsumeContext, message *Message) {
	logger.Fatal("invalid forward; it might be recursive forward message to unhandledMessageHandler")
}

type ConsumeContext struct {
	unhandledMessageHandler MessageHandleProc
	handle                  *kafka.Consumer
}

func (c *ConsumeContext) Handle() *kafka.Consumer {
	return c.handle
}

func (c *ConsumeContext) Commit() ([]TopicPartition, error) {
	return c.handle.Commit()
}

func (c *ConsumeContext) CommitMessage(m *Message) ([]TopicPartition, error) {
	return c.handle.CommitMessage(m)
}

func (c *ConsumeContext) CommitOffsets(offsets []TopicPartition) ([]TopicPartition, error) {
	return c.handle.CommitOffsets(offsets)
}

func (c *ConsumeContext) Committed(partitions []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return c.handle.Committed(partitions, timeoutMs)
}

func (c *ConsumeContext) Pause(partitions []TopicPartition) error {
	return c.handle.Pause(partitions)
}

func (c *ConsumeContext) Resume(partitions []TopicPartition) error {
	return c.handle.Resume(partitions)
}

func (c *ConsumeContext) StoreOffsets(partitions []TopicPartition) (storedOffsets []TopicPartition, err error) {
	return c.handle.StoreOffsets(partitions)
}

func (c *ConsumeContext) Wait(partitions []TopicPartition, duration time.Duration, callback func() error) error {
	var err error
	err = c.Pause(partitions)
	if err != nil {
		return err
	}

	time.AfterFunc(duration, func() {
		defer c.Resume(partitions)
		err = callback()
		if err != nil {
			return
		}
	})
	return err
}

func (c *ConsumeContext) ForwardUnhandledMessage(message *Message) {
	if c.unhandledMessageHandler != nil {
		ctx := &ConsumeContext{
			unhandledMessageHandler: StopRecursiveForwardUnhandledMessageHandler,
			handle:                  c.handle,
		}
		c.unhandledMessageHandler(ctx, message)
	}
}
