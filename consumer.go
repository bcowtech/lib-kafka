package kafka

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bcowtech/lib-kafka/internal"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	MessageHandler          MessageHandleProc
	UnhandledMessageHandler MessageHandleProc
	ErrorHandler            ErrorHandleProc
	ConfigMap               *ConfigMap
	PollingTimeout          time.Duration
	PingTimeout             time.Duration

	consumers []*kafka.Consumer
	stopChan  chan bool
	wg        sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(topics []string, rebalanceCb RebalanceCb) error {
	if c.disposed {
		return fmt.Errorf("the Consumer has been disposed")
	}
	if c.running {
		return fmt.Errorf("the Consumer is running")
	}

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.init()
	c.running = true

	{
		// ping address
		var conf = c.ConfigMap
		v, _ := conf.Get(KAFKA_CONF_BOOTSTRAP_SERVERS, nil)
		if v != nil {
			bootstrapServers := v.(string)
			addrs := strings.Split(bootstrapServers, ",")
			err = internal.Ping(addrs, c.PingTimeout)
			if err != nil {
				return err
			}
		}
	}

	for _, topic := range topics {
		var consumer *kafka.Consumer
		consumer, err = kafka.NewConsumer(c.ConfigMap)
		if err != nil {
			return err
		}

		err = consumer.SubscribeTopics([]string{topic}, rebalanceCb)
		if err != nil {
			return err
		}

		c.consumers = append(c.consumers, consumer)
	}

	var (
		pollingTimeoutMs = int(c.PollingTimeout / time.Millisecond)
	)

	for _, consumer := range c.consumers {
		var (
			worker *ConsumeWorker
		)
		worker = &ConsumeWorker{
			unhandledMessageHandler: c.UnhandledMessageHandler,
			handle:                  consumer,
		}

		go func(consumer *kafka.Consumer, stopChan chan bool) {
			c.wg.Add(1)
			defer func() {
				defer c.wg.Done()

				consumer.Unassign()
				consumer.Unsubscribe()

				consumer.Close()
			}()

			for {
				select {
				case <-stopChan:
					return

				default:
					var ev kafka.Event
					if ev == nil {
						// hold up polling until got non-nil kafka.Event
						ev = consumer.Poll(pollingTimeoutMs)
						if ev == nil {
							continue
						}
					} else {
						ev = consumer.Poll(0)
					}

					switch e := ev.(type) {
					case kafka.AssignedPartitions:
						consumer.Assign(e.Partitions)

					case kafka.RevokedPartitions:
						consumer.Unassign()

					case kafka.PartitionEOF:
						logger.Printf("%% Notice: Reached %v\n", e)

					case *kafka.Message:
						c.processMessage(worker, e)

					case kafka.Error:
						switch e.Code() {
						case kafka.ErrUnknownTopic, kafka.ErrUnknownTopicOrPart:
							if !c.processKafkaError(e) {
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
							if !c.processKafkaError(e) {
								logger.Printf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)
							}
							continue
						case kafka.ErrAllBrokersDown,
							kafka.ErrFail,
							kafka.ErrResolve,
							kafka.ErrCritSysResource,
							kafka.ErrFs,
							kafka.ErrBadMsg:
							logger.Fatalf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)

						default:
							if !c.processKafkaError(e) {
								logger.Printf("%% Error: (%#v) %+v: %v\n", e.Code(), e.Code(), e)
							}
							return
						}
					default:
						// TODO: catch GroupCoordinator: Disconnected (after %dms in state UP)
						logger.Printf("%% Notice: Ignored %v\n", e)
						logger.Printf("%% Notice: Ignored %#v\n", e)
					}
				}
			}
		}(consumer, c.stopChan)
	}
	return nil
}

func (c *Consumer) Close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	defer func() {
		c.running = false
		c.disposed = true
		// dispose
		c.consumers = nil
		c.stopChan = nil
		c.mutex.Unlock()
	}()

	count := len(c.consumers)
	for i := 0; i < count; i++ {
		c.stopChan <- true
	}
	close(c.stopChan)

	c.wg.Wait()
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.stopChan == nil {
		c.stopChan = make(chan bool, 1)
	}
	c.initialized = true
}

func (c *Consumer) processKafkaError(err kafka.Error) (disposed bool) {
	if c.ErrorHandler != nil {
		return c.ErrorHandler(err)
	}
	return false
}

func (c *Consumer) processMessage(worker *ConsumeWorker, message *kafka.Message) {
	if c.MessageHandler != nil {
		c.MessageHandler(worker, message)
	} else {
		worker.ForwardUnhandledMessage(message)
	}
}
