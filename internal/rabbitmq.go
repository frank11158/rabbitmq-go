package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// The connection used by the client
	conn *amqp.Connection
	// The channel is used to process / Send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost, caCert, clientCert, clientKey string) (*amqp.Connection, error) {
	ca, err := os.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	// Load Keypair
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, err
	}

	// Add the rootCA to the cert pool
	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(ca)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}

	return amqp.DialTLS(fmt.Sprintf("amqps://%s:%s@%s/%s", username, password, host, vhost), tlsConfig)
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

// CreateQueue creates a queue with based on given cfgs
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	queue, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, nil
	}
	return queue, err
}

// CreateBinding creates a binding between a queue and an exchange using the routing key provided
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	// Leaving nowait to false, which will make the channel return an error if its failed to bind
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Send is used to publish payloads onto an exchange with a routing key
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		// Mandatory is used to determine if an error should be returned upon failure to deliver
		true,
		// Immediate
		false,
		options,
	)
	if err != nil {
		return err
	}
	log.Println(confirmation.Wait())
	return nil
}

// Consume is used to consume messages from a queue
func (rc RabbitClient) Consume(queue, consumer string, autoAct bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAct, false, false, false, nil)
}

// ApplyQoS applies the QoS to the channel
// prefetch count - an integer count of how many unacknowledge messages the server can send
// prefetch size - an integer of how many bytes the queue can have before we are allowed to send more
// gloabl - determins if the rule should be applied globally or per consumer
func (rc RabbitClient) ApplyQoS(count, size int, global bool) error {
	return rc.ch.Qos(count, size, global)
}
