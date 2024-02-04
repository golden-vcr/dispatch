# dispatch

The **dispatch** service is responsible for consuming messages from the `twitch-events`
queue and making stuff happen in response. It currently consists of a single consumer
process.

## Development Guide

On a Linux or WSL system:

1. Install [Go 1.21](https://go.dev/doc/install)
2. Clone the [**terraform**](https://github.com/golden-vcr/terraform) repo alongside
   this one, and from the root of that repo:
    - Ensure that the module is initialized (via `terraform init`)
    - Ensure that valid terraform state is present
    - Run `terraform output -raw env_dispatch_local > ../dispatch/.env` to populate an
      `.env` file.
    - Run [`./local-rmq.sh up`](https://github.com/golden-vcr/terraform/blob/main/local-rmq.sh)
      to ensure that a RabbitMQ server is running locally (requires
      [Docker](https://docs.docker.com/engine/install/)).
3. Ensure that the [**auth**](https://github.com/golden-vcr/auth?tab=readme-ov-file#development-guide)
   and [**ledger**](https://github.com/golden-vcr/ledger?tab=readme-ov-file#development-guide)
   servers are running locally.
4. From the root of this repository:
    - Run [`go run cmd/consumer/main.go`](./cmd/consumer/main.go) to start up the
      consumer process.

Once running, the consumer should respond when events that are produced to the
`twitch-events` queue.
