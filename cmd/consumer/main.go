package main

import (
	"encoding/json"
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
	consumer, err := rmq.NewConsumer(amqpConn, "twitch-events")
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
