package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/cloudfoundry-community/gautocloud"
	_ "github.com/cloudfoundry-community/gautocloud/connectors/amqp/client"
	"github.com/streadway/amqp"
	"time"
)

type Consumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	handlerFunc  ConsumerHandlerFunc
	done         chan error
	consumerTag  string // Name that consumer identifies itself to the server with
	uri          string // uri of the rabbitmq server
	exchange     string // exchange that we will bind to
	exchangeType string // topic, direct, etc...
	bindingKey   string // routing key that we are using
	queueName    string // queue name
}

type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error
}

type Config struct {
	ServiceName  string
	Exchange     string
	ExchangeType string
	QueueName    string
	RoutingKey   string
	CTag         string
	HandlerFunc  ConsumerHandlerFunc
}

func (p *Producer) Publish(exchange, routingKey string, msg amqp.Publishing) error {
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

func (p *Producer) Close() {
	p.conn.Close()
}

func NewProducer(config Config) (*Producer, error) {
	var err error

	p := &Producer{
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
		p.conn.Close()
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
		p.conn.Close()
		return nil, fmt.Errorf("exchange declare error: %s", err)
	}
	return p, nil
}

func NewConsumer(config Config) (*Consumer, error) {
	var err error

	c := &Consumer{
		conn:         nil,
		channel:      nil,
		handlerFunc:  config.HandlerFunc,
		consumerTag:  config.CTag,
		exchange:     config.Exchange,
		exchangeType: config.ExchangeType,
		bindingKey:   config.RoutingKey,
		queueName:    config.QueueName,
		done:         make(chan error),
	}
	return c, err
}

func (c *Consumer) Start() error {
	if err := c.Connect(); err != nil {
		return err
	}

	deliveries, err := c.AnnounceQueue(c.queueName, c.bindingKey)
	if err != nil {
		return err
	}
	go c.Handle(deliveries, c.handlerFunc, 1, c.queueName, c.bindingKey)
	return nil
}

// ReConnect is called in places where NotifyClose() channel is called
// wait 30 seconds before trying to reconnect. Any shorter amount of time
// will  likely destroy the error log while waiting for servers to come
// back online. This requires two parameters which is just to satisfy
// the AccounceQueue call and allows greater flexability
func (c *Consumer) reConnect(queueName, bindingKey string) (<-chan amqp.Delivery, error) {
	time.Sleep(30 * time.Second)

	if err := c.Connect(); err != nil {
		fmt.Printf("could not connect in reconnect call: %v\n", err.Error())
	}

	deliveries, err := c.AnnounceQueue(queueName, bindingKey)
	if err != nil {
		return deliveries, errors.New("Couldn't connect")
	}

	return deliveries, nil
}

// Connect to RabbitMQ server
func (c *Consumer) Connect() error {

	var err error

	err = gautocloud.InjectFromId("amqp", &c.conn)
	if err != nil {
		return fmt.Errorf("dial error: %s", err)
	}

	go func() {
		// Waits here for the channel to be closed
		fmt.Printf("closing: %s\n", <-c.conn.NotifyClose(make(chan *amqp.Error)))
		// Let Handle know it's not time to reconnect
		c.done <- errors.New("Channel Closed")
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

// AnnounceQueue sets the queue that will be listened to for this
// connection...
func (c *Consumer) AnnounceQueue(queueName, bindingKey string) (<-chan amqp.Delivery, error) {
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("queue declare error: %s", err)
	}

	// Qos determines the amount of messages that the queue will pass to you before
	// it waits for you to ack them. This will slow down queue consumption but
	// give you more certainty that all messages are being processed. As load increases
	// I would reccomend upping the about of Threads and Processors the go process
	// uses before changing this although you will eventually need to reach some
	// balance between threads, procs, and Qos.
	err = c.channel.Qos(1, 0, false)
	if err != nil {
		return nil, fmt.Errorf("error setting qos: %s", err)
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
// d should be a delievey channel as created when you call AnnounceQueue
// fn should be a function that handles the processing of deliveries
// this should be the last thing called in main as code under it will
// become unreachable unless put int a goroutine. The q and rk params
// are redundent but allow you to have multiple queue listeners in main
// without them you would be tied into only using one queue per connection
func (c *Consumer) Handle(
	d <-chan amqp.Delivery,
	fn ConsumerHandlerFunc,
	threads int,
	queue string,
	routingKey string) {

	var err error

	for {
		for i := 0; i < threads; i++ {
			go fn(d)
		}

		// Go into reconnect loop when
		// c.done is passed non nil values
		if <-c.done != nil {
			d, err = c.reConnect(queue, routingKey)
			if err != nil {
				// Very likely chance of failing
				// should not cause worker to terminate
			}
		}
	}
}

type ConsumerHandlerFunc func(deliveries <-chan amqp.Delivery) error
