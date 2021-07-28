package kafka

import "time"

type ProducerOption struct {
	FlushTimeout time.Duration
	PingTimeout  time.Duration
	ConfigMap    *ConfigMap
	ErrorHandler ErrorHandleProc
}
