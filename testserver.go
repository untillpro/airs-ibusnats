/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

const embeddedNATSServerKey embeddedNATSServerKeyType = "embeddedNATSServer"

var DefaultEmbeddedNATSServerURL = NATSServers([]string{"nats://127.0.0.1:4222"})

type embeddedNATSServerKeyType string

type embeddedNATSServer struct {
	s *server.Server
}

func getEmbeddedNATSServer(ctx context.Context) *embeddedNATSServer {
	return ctx.Value(embeddedNATSServerKey).(*embeddedNATSServer)
}

func (s *embeddedNATSServer) Start(ctx context.Context) (context.Context, error) {
	opts := natsserver.DefaultTestOptions
	s.s = natsserver.RunServer(&opts)
	return context.WithValue(ctx, embeddedNATSServerKey, s), nil
}

func (s *embeddedNATSServer) Stop(ctx context.Context) {
	getEmbeddedNATSServer(ctx).s.Shutdown()
}
