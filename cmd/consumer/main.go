package main

import (
	"context"
	"log"
	"time"

	"github.com/frank11158/eventdrivenrabbitmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
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

	publishConn, err := internal.ConnectRabbitMQ("frank", "secret", "localhost:5671", "customers", caCert, clientCert, clientKey)
	if err != nil {
		panic(err)
	}
	defer publishConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		panic(err)
	}
	defer publishClient.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := client.Consume(queue.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}

	// set a timout for 15 secs
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Apply a hard limit on the server
	if err := client.ApplyQoS(10, 0, true); err != nil {
		panic(err)
	}

	// errgroup allows us concurrent tasks
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			// Spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New Message: %v", msg)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}

				if err := publishClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp.Persistent,
					Body:          []byte(`RPC completed`),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					panic(err)
				}

				log.Printf("Acknowledge message %s", msg.MessageId)
				return nil
			})
		}
	}()

	log.Println("Consume messages..., use CTRL+C to stop")

	<-blocking
}
