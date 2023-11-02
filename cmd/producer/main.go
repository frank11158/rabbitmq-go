package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/frank11158/eventdrivenrabbitmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	caCert     = "path/to/ca_certificate.pem"
	clientCert = "path/to/<hostname>_certificate.pem"
	clientKey  = "path/to/<hostname>_key.pem"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("frank", "secret", "localhost:5671", "customers", caCert, clientCert, clientKey)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// All consume will be done in this connection
	consumeConn, err := internal.ConnectRabbitMQ("frank", "secret", "localhost:5671", "customers", caCert, clientCert, clientKey)
	if err != nil {
		panic(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	messageBus, err := consumeClient.Consume(queue.Name, "customer-api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback: %v\n", message.CorrelationId)
		}
	}()

	// Publish a message
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
			Body:          []byte(`An cool msg between services`),
		}); err != nil {
			panic(err)
		}
	}

	var blocking chan struct{}
	<-blocking
}
