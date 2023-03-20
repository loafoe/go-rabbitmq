package rabbitmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/cloudfoundry-community/gautocloud"
	_ "github.com/cloudfoundry-community/gautocloud/connectors/amqp/client"
	"github.com/streadway/amqp"
)

type Consumer interface {
	Connect() error
	Start() error
	Handle(d <-chan amqp.Delivery, f ConsumerHandlerFunc, threads int, queue string, routingKey string)
}

type Producer interface {
	Publish(exchange, routingKey string, msg amqp.Publishing) error
	Close()
}

type AMQPConsumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	handlerFunc  ConsumerHandlerFunc
	done         chan error
	consumerTag  string // Name that consumer identifies itself to the server with
	exchange     string // exchange that we will bind to
	exchangeType string // topic, direct, etc...
	bindingKey   string // routing key that we are using
	queueName    string // queue name
	config       Config // Configuration
}

type AMQPProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error
}

type Config struct {
	ServiceName  string
	Exchange     string
	ExchangeType string
	Durable      bool
	AutoDelete   bool
	QueueName    string
	RoutingKey   string
	CTag         string
	Qos          *Qos
	HandlerFunc  ConsumerHandlerFunc
}

type Qos struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

func (p *AMQPProducer) Publish(exchange, routingKey string, msg amqp.Publishing) error {
	if err := p.channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		msg,
	); err != nil {
		return fmt.Errorf("exchange publish error: %s", err)
	}
	return nil
}

func (p *AMQPProducer) Close() {
	_ = p.conn.Close()
}

func NewProducer(config Config) (*AMQPProducer, error) {
	var err error

	p := &AMQPProducer{
		conn:    nil,
		channel: nil,
		done:    make(chan error),
	}

	err = gautocloud.InjectFromId("amqp", &p.conn)
	if err != nil {
		return nil, fmt.Errorf("dial error: %s", err)
	}
	p.channel, err = p.conn.Channel()
	if err != nil {
		_ = p.conn.Close()
		return nil, fmt.Errorf("channel error: %s", err)
	}
	if err = p.channel.ExchangeDeclare(
		config.Exchange,     // name
		config.ExchangeType, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		_ = p.conn.Close()
		return nil, fmt.Errorf("exchange declare error: %s", err)
	}
	return p, nil
}

func NewConsumer(config Config) (*AMQPConsumer, error) {
	var err error

	c := &AMQPConsumer{
		conn:         nil,
		channel:      nil,
		handlerFunc:  config.HandlerFunc,
		consumerTag:  config.CTag,
		exchange:     config.Exchange,
		exchangeType: config.ExchangeType,
		bindingKey:   config.RoutingKey,
		queueName:    config.QueueName,
		done:         make(chan error),
		config:       config,
	}
	return c, err
}

func (c *AMQPConsumer) Start() error {
	if err := c.Connect(); err != nil {
		return err
	}

	deliveries, err := c.announceQueue(c.queueName, c.bindingKey)
	if err != nil {
		return err
	}
	go c.Handle(deliveries, c.handlerFunc, 1, c.queueName, c.bindingKey)
	return nil
}

// reConnect is called in places where NotifyClose() channel is called
// wait 30 seconds before trying to reconnect. Any shorter amount of time
// will  likely destroy the error log while waiting for servers to come
// back online. This requires two parameters which is just to satisfy
// the AccounceQueue call and allows greater flexability
func (c *AMQPConsumer) reConnect(queueName, bindingKey string) (<-chan amqp.Delivery, error) {
	time.Sleep(30 * time.Second)

	if err := c.Connect(); err != nil {
		fmt.Printf("could not connect in reconnect call: %v\n", err.Error())
	}

	deliveries, err := c.announceQueue(queueName, bindingKey)
	if err != nil {
		return deliveries, fmt.Errorf("couldn't connect: %w", err)
	}

	return deliveries, nil
}

// Connect to RabbitMQ server
func (c *AMQPConsumer) Connect() error {

	var err error

	err = gautocloud.InjectFromId("amqp", &c.conn)
	if err != nil {
		return fmt.Errorf("dial error: %s", err)
	}

	go func() {
		// Waits here for the channel to be closed
		fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
		// Let Handle know it's not time to reconnect
		c.done <- errors.New("channel Closed")
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel error: %s", err)
	}

	if err = c.channel.ExchangeDeclare(
		c.exchange,     // name of the exchange
		c.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return fmt.Errorf("exchange declare error: %s", err)
	}

	return nil
}

// announceQueue sets the queue that will be listened to for this
// connection...
func (c *AMQPConsumer) announceQueue(queueName, bindingKey string) (<-chan amqp.Delivery, error) {
	queue, err := c.channel.QueueDeclare(
		queueName,           // name of the queue
		c.config.Durable,    // durable
		c.config.AutoDelete, // delete when usused
		false,               // exclusive
		false,               // noWait
		nil,                 // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("queue declare error: %s", err)
	}

	// Qos determines the amount of messages that the queue will pass to you before
	// it waits for you to ack them. This will slow down queue consumption but
	// give you more certainty that all messages are being processed. As load increases
	// I would recommend upping the number of Threads and Processors the go process
	// uses before changing this, so you will eventually need to reach some
	// balance between threads, processes, and Qos.
	if c.config.Qos != nil {
		err = c.channel.Qos(c.config.Qos.PrefetchCount, c.config.Qos.PrefetchSize, c.config.Qos.Global)
		if err != nil {
			return nil, fmt.Errorf("error setting qos: %s", err)
		}
	}

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		bindingKey, // bindingKey
		c.exchange, // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("queue bind error: %s", err)
	}

	deliveries, err := c.channel.Consume(
		queue.Name,    // name
		c.consumerTag, // consumerTag,
		false,         // noAck
		false,         // exclusive
		false,         // noLocal
		false,         // noWait
		nil,           // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue consume error: %s", err)
	}

	return deliveries, nil
}

// Handle has all the logic to make sure your program keeps running
// d should be a delievey channel as created when you call announceQueue
// fn should be a function that handles the processing of deliveries
// this should be the last thing called in main as code under it will
// become unreachable unless put int a goroutine. The q and rk params
// are redundent but allow you to have multiple queue listeners in main
// without them you would be tied into only using one queue per connection
func (c *AMQPConsumer) Handle(
	d <-chan amqp.Delivery,
	fn ConsumerHandlerFunc,
	threads int,
	queue string,
	routingKey string) {

	var err error

	for {
		doneChans := make([]chan bool, threads)
		for i := 0; i < threads; i++ {
			doneChans[i] = make(chan bool)
			go fn(d, doneChans[i])
		}
		// Go into reconnect loop when
		// c.done is passed non nil values
		if <-c.done != nil {
			// Terminate old workers
			for i := 0; i < threads; i++ {
				doneChans[i] <- true
			}
			d, err = c.reConnect(queue, routingKey)
			if err != nil {
				// Very likely chance of failing
				// should not cause worker to terminate
			}

		}
	}
}

type ConsumerHandlerFunc func(deliveries <-chan amqp.Delivery, done <-chan bool)
