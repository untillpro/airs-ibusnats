/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"

	"github.com/nats-io/gnatsd/server"
	natsserver "github.com/nats-io/nats-server/test"
)

type testServerKeyType string

const testServerKey testServerKeyType = "testServer"

type testServer struct {
	s *server.Server
}

func getTestServer(ctx context.Context) *testServer {
	return ctx.Value(testServerKey).(*testServer)
}

func (s *testServer) Start(ctx context.Context) (context.Context, error) {
	opts := natsserver.DefaultTestOptions
	s.s = natsserver.RunServer(&opts)
	return context.WithValue(ctx, testServerKey, s), nil
}

func (s *testServer) Stop(ctx context.Context) {
	getTestServer(ctx).s.Shutdown()
}
