package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/codingconcepts/env"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	etwitch "github.com/golden-vcr/schemas/twitch-events"
	"github.com/golden-vcr/server-common/entry"
	"github.com/golden-vcr/server-common/rmq"
)

type Config struct {
	RmqHost     string `env:"RMQ_HOST" required:"true"`
	RmqPort     int    `env:"RMQ_PORT" required:"true"`
	RmqVhost    string `env:"RMQ_VHOST" required:"true"`
	RmqUser     string `env:"RMQ_USER" required:"true"`
	RmqPassword string `env:"RMQ_PASSWORD" required:"true"`
}

func main() {
	app, ctx := entry.NewApplication("dispatch-consumer")
	defer app.Stop()

	// Parse config from environment variables
	err := godotenv.Load()
	if err != nil && !os.IsNotExist(err) {
		app.Fail("Failed to load .env file", err)
	}
	config := Config{}
	if err := env.Set(&config); err != nil {
		app.Fail("Failed to load config", err)
	}

	// Initialize an AMQP client
	amqpConn, err := amqp.Dial(rmq.FormatConnectionString(config.RmqHost, config.RmqPort, config.RmqVhost, config.RmqUser, config.RmqPassword))
	if err != nil {
		app.Fail("Failed to connect to AMQP server", err)
	}
	defer amqpConn.Close()
	consumer, err := NewConsumer(amqpConn, "twitch-events")
	if err != nil {
		app.Fail("Failed to initialize AMQP consumer", err)
	}

	// Start receiving incoming messages from the twitch-events exchange
	deliveries, err := consumer.Recv(ctx)
	if err != nil {
		app.Fail("Failed to init recv channel on consumer", err)
	}

	// Each time we read a message from the queue, spin up a new goroutine for that
	// message, parse it according to our twitch-events schema, then handle it
	wg, ctx := errgroup.WithContext(ctx)
	done := false
	for !done {
		select {
		case <-ctx.Done():
			app.Log().Info("Consumer context canceled; exiting main loop")
			done = true
		case d, ok := <-deliveries:
			if ok {
				app.Log().Info("Handling delivery", "d", d)
				wg.Go(func() error {
					var ev etwitch.Event
					if err := json.Unmarshal(d.Body, &ev); err != nil {
						return err
					}
					app.Log().Info("Consumed from twitch-events", "twitchEvent", ev)
					return nil
				})
			} else {
				app.Log().Info("Channel is closed; exiting main loop")
				done = true
			}
		}
	}

	if err := wg.Wait(); err != nil {
		app.Fail("Encountered an error during message handling", err)
	}
}

// Consumer can receive AMQP messages from a single message queue
type Consumer interface {
	Close()
	Recv(ctx context.Context) (<-chan amqp.Delivery, error)
}

// NewConsumer initializes a Consumer from an AMQP client connection, configuring it to
// receive messages from an exchange with the given name
func NewConsumer(conn *amqp.Connection, exchange string) (Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	if err := declareFanoutExchange(ch, exchange); err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	q, err := declareConsumerQueue(ch, exchange)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to declare consumer queue: %w", err)
	}

	return &consumer{
		conn:     conn,
		ch:       ch,
		q:        q,
		exchange: exchange,
	}, nil
}

// producer is a concrete implementation that uses AMQP under the hood, receiving
// messages from a unique queue that is declared (and bound to a named exchange) for the
// lifetime of the consumer procexx
type consumer struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	q        *amqp.Queue
	exchange string
}

func (c *consumer) Close() {
	c.ch.Close()
	c.conn.Close()
}

func (c *consumer) Recv(ctx context.Context) (<-chan amqp.Delivery, error) {
	autoAck := true
	exclusive := false
	noLocal := false
	noWait := false
	return c.ch.ConsumeWithContext(ctx, c.q.Name, "", autoAck, exclusive, noLocal, noWait, nil)
}

// declareFanoutExchange uses an AMQP client to declare an fanout exchange for a simple
// message queueing scheme in which any number of producers can send messages to a named
// exchange, and any number of consumers can receive messages by binding their own
// temporary queue to that exchange
func declareFanoutExchange(ch *amqp.Channel, exchange string) error {
	durable := true
	autoDelete := false
	internal := false
	noWait := false
	return ch.ExchangeDeclare(exchange, "fanout", durable, autoDelete, internal, noWait, nil)
}

// declareConsumerQueue uses an AMQP client to declare a temporary queue for a consumer
// process, then bind it to the exchange with the given name
func declareConsumerQueue(ch *amqp.Channel, exchange string) (*amqp.Queue, error) {
	durable := false
	autoDelete := false
	exclusive := true
	noWait := false
	q, err := ch.QueueDeclare("", durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		return nil, err
	}

	noWait = false
	if err := ch.QueueBind(q.Name, "", exchange, noWait, nil); err != nil {
		return nil, err
	}
	return &q, nil
}
