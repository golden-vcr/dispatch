# dispatch

The **dispatch** service is responsible for consuming messages from the `twitch-events`
queue and making stuff happen in response. It currently consists of a single consumer
process.

## Prerequisites

Install [Go 1.21](https://go.dev/doc/install). If successful, you should be able to run:

```
> go version
go version go1.21.0 windows/amd64
```

## Initial setup

Create a file in the root of this repo called `.env` that contains the environment
variables required in [`main.go`](./cmd/server/main.go). If you have the
[`terraform`](https://github.com/golden-vcr/terraform) repo cloned alongside this one,
simply open a shell there and run:

- `terraform output -raw auth_shared_secret_env >> ../dispatch/.env`
- `./local-rmq.sh env >> ../dispatch/.env`

The dispatch server depends on RabbitMQ: you can start up a local RabbitMQ server for
development (in a docker container) by running `./local-rmq.sh up`.
