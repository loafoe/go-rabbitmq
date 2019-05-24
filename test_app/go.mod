module test_app

go 1.12

replace github.com/loafoe/go-rabbitmq => ./..

require (
	github.com/loafoe/go-rabbitmq v0.0.0
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
)
