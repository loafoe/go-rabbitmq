package main

import (
	"fmt"
	"os"
	"time"

	rabbitmq "github.com/loafoe/go-rabbitmq"
	"github.com/streadway/amqp"
)

func worker(delivery <-chan amqp.Delivery, done <-chan bool) {
	for {
		select {
		case d := <-delivery:
			fmt.Printf("Something was delivered: \"%s\"\n", string(d.Body))
			d.Ack(true)
		case <-time.After(1 * time.Second):
			fmt.Printf("Working..\n")
		case <-done:
			return
		}
	}
}

func main() {
	consumer, err := rabbitmq.NewConsumer(rabbitmq.Config{
		RoutingKey:   "app_key",
		Exchange:     "app_exchange",
		ExchangeType: "topic",
		Durable:      false,
		AutoDelete:   true,
		QueueName:    "app_queue",
		CTag:         "go-rabbitmq",
		HandlerFunc:  worker,
	})

	if err != nil {
		fmt.Printf("Error defining consumer")
		return
	}
	fmt.Printf("Starting consumer\n")
	err = consumer.Start()
	if err != nil {
		fmt.Printf("Fatal: %v\n", err)
		os.Exit(1)
	}

	wait := make(chan bool)

	go func(c chan bool) {
		producer, err := rabbitmq.NewProducer(rabbitmq.Config{
			Exchange:     "app_exchange",
			ExchangeType: "topic",
			Durable:      false,
		})
		fmt.Printf("Publishing..\n")
		for i := 0; i < 10; i++ {
			err = producer.Publish("app_exchange", "app_key", amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "application/octet-stream",
				ContentEncoding: "",
				Body:            []byte(fmt.Sprintf("hello world %d", i)),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
			})
			if err != nil {
				fmt.Printf("Error publishing: %v\n", err)
			}
		}
		time.Sleep(4 * time.Second)
		c <- true // Exit
	}(wait)

	fmt.Printf("Waiting..\n")
	<-wait
	fmt.Printf("Done!\n")
}
