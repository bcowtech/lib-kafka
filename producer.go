package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/bcowtech/lib-kafka/internal"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	handle *kafka.Producer

	errorHandler   ErrorHandleProc
	flushTimeoutMs int
	pingTimeout    time.Duration

	locker   internal.Locker
	disposed bool
}

func NewProducer(opt *ProducerOption) (*Producer, error) {
	instance := &Producer{
		errorHandler:   opt.ErrorHandler,
		flushTimeoutMs: int(opt.FlushTimeout / time.Millisecond),
		pingTimeout:    opt.PingTimeout,
	}

	var err error
	err = instance.init(opt.ConfigMap)
	if err != nil {
		return nil, err
	}
	instance.initEventLoop()
	return instance, nil
}

func (p *Producer) Handle() *kafka.Producer {
	return p.handle
}

func (p *Producer) Write(topic string, key, value []byte) error {
	return p.WriteMessage(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value,
		}, nil)
}

func (p *Producer) WriteWithTimeout(topic string, key, value []byte, timeout time.Duration) error {
	return p.WriteMessageWithTimeout(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value,
		}, nil, timeout)
}

func (p *Producer) WriteMessage(message *Message, deliveryChan chan Event) error {
	return p.writeMessageWithTimeout(message, deliveryChan, p.flushTimeoutMs)
}

func (p *Producer) WriteMessageWithTimeout(message *Message, deliveryChan chan Event, timeout time.Duration) error {
	return p.writeMessageWithTimeout(message, deliveryChan, int(timeout/time.Millisecond))
}

func (p *Producer) Close() {
	if p.disposed {
		return
	}

	defer func() {
		p.locker.Lock(
			func() {
				p.disposed = true
			})
	}()

	var (
		h = p.handle
	)

	h.Close()
}

func (p *Producer) writeMessageWithTimeout(message *Message, deliveryChan chan Event, timeoutMs int) error {
	if p.disposed {
		return fmt.Errorf("the Producer has been disposed")
	}

	var (
		h = p.handle
	)
	// Wait for message deliveries before shutting down
	err := h.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	h.Flush(timeoutMs)
	return nil
}

func (p *Producer) isRetriableError(err kafka.Error) bool {
	if err.IsRetriable() {
		return false
	}

	switch err.Code() {
	case kafka.ErrNotEnoughReplicas,
		kafka.ErrNotEnoughReplicasAfterAppend:
		return true
	}
	return false
}

func (p *Producer) handleError(err kafka.Error) (disposed bool) {
	if p.errorHandler != nil {
		return p.errorHandler(err)
	}
	return false
}

func (p *Producer) init(conf *ConfigMap) error {
	{
		// ping address
		v, _ := conf.Get(KAFKA_CONF_BOOTSTRAP_SERVERS, nil)
		if v != nil {
			bootstrapServers := v.(string)
			addrs := strings.Split(bootstrapServers, ",")
			err := internal.Ping(addrs, p.pingTimeout)
			if err != nil {
				return err
			}
		}
	}

	producer, err := kafka.NewProducer(conf)
	if err != nil {
		return err
	}
	p.handle = producer
	return nil
}

func (p *Producer) initEventLoop() {
	var (
		h = p.handle
	)

	// Delivery report handler for produced messages
	go func() {
		for ev := range h.Events() {
			for {
				switch e := ev.(type) {
				case kafka.Error:
					switch e.Code() {
					case kafka.ErrUnknownTopic, kafka.ErrUnknownTopicOrPart:
						if !p.handleError(e) {
							logger.Fatalf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)
						}

					/* NOTE: https://github.com/edenhill/librdkafka/issues/64
					Currently the only error codes signaled through the error_cb are:
					- RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN - all brokers are down
					- RD_KAFKA_RESP_ERR__FAIL - generic low level errors (socket failures because of lacking ipv4/ipv6 support)
					- RD_KAFKA_RESP_ERR__RESOLVE - failure to resolve the broker address
					- RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE - failed to create new thread
					- RD_KAFKA_RESP_ERR__FS - various file errors in the consumer offset management code
					- RD_KAFKA_RESP_ERR__TRANSPORT - failed to connect to single broker, or connection error for single broker
					- RD_KAFKA_RESP_ERR__BAD_MSG - received malformed packet from broker (version mismatch?)
					I guess you could treat all but .._TRANSPORT as fatal.
					*/
					case kafka.ErrTransport:
						if !p.handleError(e) {
							logger.Printf(" %% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)
						}

					case kafka.ErrAllBrokersDown,
						kafka.ErrFail,
						kafka.ErrResolve,
						kafka.ErrCritSysResource,
						kafka.ErrFs,
						kafka.ErrBadMsg:
						logger.Fatalf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)

					default:
						if !p.handleError(e) {
							logger.Printf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)
						}
						return
					}
				case *kafka.Message:
					if e.TopicPartition.Error != nil {
						if err, ok := e.TopicPartition.Error.(*kafka.Error); ok {
							ev = *err
							if p.isRetriableError(*err) {
								// TODO implement retry
								break
							}
							continue
						} else if err, ok := e.TopicPartition.Error.(kafka.Error); ok {
							ev = err
							if p.isRetriableError(err) {
								// TODO implement retry
								break
							}
							continue
						}
						logger.Printf("%% Error: (%s) (%T): %[2]v\n",
							e.TopicPartition,
							e.TopicPartition.Error)
					}
				default:
					logger.Printf("%% Notice: Ignored %v\n", e)
				}
				break
			}
		}
	}()
}
