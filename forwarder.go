package kafka

type Forwarder struct {
	*Producer
}

func NewForwarder(opt *ForwarderOption) (*Forwarder, error) {
	producer, err := NewProducer(opt)
	if err != nil {
		return nil, err
	}
	instance := &Forwarder{
		Producer: producer,
	}
	return instance, nil
}

func (f *Forwarder) Runner() *ForwarderRunner {
	return &ForwarderRunner{
		handle: f,
	}
}
