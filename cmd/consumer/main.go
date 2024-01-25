package main

import (
	"encoding/json"
	"os"

	"github.com/codingconcepts/env"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/golden-vcr/auth"
	"github.com/golden-vcr/dispatch/internal/processing"
	"github.com/golden-vcr/ledger"
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

	AuthURL          string `env:"AUTH_URL" default:"http://localhost:5002"`
	AuthSharedSecret string `env:"AUTH_SHARED_SECRET" required:"true"`
	LedgerURL        string `env:"LEDGER_URL" default:"http://localhost:5003"`
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

	// We need an auth service client so that when we handle a Twitch event that was
	// initiated by a specific user, we can request a JWT that will authorize requests
	// to the Golden VCR backend that modify that user's state
	authServiceClient := auth.NewServiceClient(config.AuthURL, config.AuthSharedSecret)

	// We need a ledger client in order to credit Golden VCR Fun Points to a user's
	// balance whenenver they interact with the Twitch channel in a way that we wish to
	// reward with points
	ledgerClient := ledger.NewClient(config.LedgerURL)

	// Initialize an AMQP client
	amqpConn, err := amqp.Dial(rmq.FormatConnectionString(config.RmqHost, config.RmqPort, config.RmqVhost, config.RmqUser, config.RmqPassword))
	if err != nil {
		app.Fail("Failed to connect to AMQP server", err)
	}
	defer amqpConn.Close()

	// Prepare a producer that we can use to send messages to the onscreen-events queue,
	// whenenver we determine that a new alert needs to be displayed
	onscreenEventsProducer, err := rmq.NewProducer(amqpConn, "onscreen-events")
	if err != nil {
		app.Fail("Failed to initialize AMQP producer for onscreen-events", err)
	}

	// Prepare a second producer that lets us send messages to the generation-requests
	// queue, so that we can kick off the required background processing for
	// cheers/alerts that involve image generation etc.
	generationRequestsProducer, err := rmq.NewProducer(amqpConn, "generation-requests")
	if err != nil {
		app.Fail("Failed to initialize AMQP producer for generation-requests", err)
	}

	// Prepare a consumer and start receiving incoming messages from the twitch-events
	// exchange
	consumer, err := rmq.NewConsumer(amqpConn, "twitch-events")
	if err != nil {
		app.Fail("Failed to initialize AMQP consumer", err)
	}
	deliveries, err := consumer.Recv(ctx)
	if err != nil {
		app.Fail("Failed to init recv channel on consumer", err)
	}

	// Prepare a handler that has the state necessary to respond to incoming
	// twitch-events messages by crediting points and then producing events to other,
	// downstream queues
	h := processing.NewHandler(
		authServiceClient,
		ledgerClient,
		onscreenEventsProducer,
		generationRequestsProducer,
	)

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
					logger := app.Log().With("twitchEvent", ev)
					app.Log().Info("Consumed from twitch-events")
					if err := h.Handle(ctx, logger, &ev); err != nil {
						app.Log().Error("Failed to handle event", "error", err)
					}
					return err
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
