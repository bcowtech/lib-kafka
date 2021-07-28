package kafka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var _ MessageHandleProc = StopRecursiveForwardUnhandledMessageHandler

func StopRecursiveForwardUnhandledMessageHandler(worker *ConsumeWorker, message *Message) {
	panic("invalid forward; it might be recursive forward message to unhandledMessageHandler")
}

type ConsumeWorker struct {
	unhandledMessageHandler MessageHandleProc
	handle                  *kafka.Consumer
}

func (w *ConsumeWorker) Handle() *kafka.Consumer {
	return w.handle
}

func (w *ConsumeWorker) Commit() ([]TopicPartition, error) {
	return w.handle.Commit()
}

func (w *ConsumeWorker) CommitMessage(m *Message) ([]TopicPartition, error) {
	return w.handle.CommitMessage(m)
}

func (w *ConsumeWorker) CommitOffsets(offsets []TopicPartition) ([]TopicPartition, error) {
	return w.handle.CommitOffsets(offsets)
}

func (w *ConsumeWorker) Committed(partitions []TopicPartition, timeoutMs int) (offsets []TopicPartition, err error) {
	return w.handle.Committed(partitions, timeoutMs)
}

func (w *ConsumeWorker) Pause(partitions []TopicPartition) error {
	return w.handle.Pause(partitions)
}

func (w *ConsumeWorker) Resume(partitions []TopicPartition) error {
	return w.handle.Resume(partitions)
}

func (w *ConsumeWorker) StoreOffsets(partitions []TopicPartition) (storedOffsets []TopicPartition, err error) {
	return w.handle.StoreOffsets(partitions)
}

func (w *ConsumeWorker) Wait(partitions []TopicPartition, duration time.Duration, callback func() error) error {
	var err error
	err = w.Pause(partitions)
	if err != nil {
		return err
	}

	time.AfterFunc(duration, func() {
		defer w.Resume(partitions)
		err = callback()
		if err != nil {
			return
		}
	})
	return err
}

func (w *ConsumeWorker) ForwardUnhandledMessage(message *Message) {
	if w.unhandledMessageHandler != nil {
		worker := &ConsumeWorker{
			unhandledMessageHandler: StopRecursiveForwardUnhandledMessageHandler,
			handle:                  w.handle,
		}
		w.unhandledMessageHandler(worker, message)
	}
}
