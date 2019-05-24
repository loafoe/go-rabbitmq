package rabbitmq

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestNewProducer(t *testing.T) {
	producer, err := NewProducer(Config{
		Exchange:     "app_exchange",
		ExchangeType: "topic",
		Durable:      false,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	err = producer.Publish("app_exchange", "app_key", amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		Body:            []byte("hello world"),
		DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
		Priority:        0,              // 0-9
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}
