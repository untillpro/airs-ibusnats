/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package ibusnats

import (
	"context"
	"github.com/nats-io/go-nats"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/gochips"
)

// Service s.e.
type Service struct {
	// Comma separated list of servers
	NATSServers      string
	Queues           map[string]int
	CurrentQueueName string
	Parts            int
	CurrentPart      int
	// In router subs are nil, in app publisher nil
	nATSPublisher   *nATSPublisher
	nATSSubscribers map[int]*nATSSubscriber
}

type contextKeyType string

const (
	nATSKey = contextKeyType("nATSKey")
	// DefaultNATSHost s.e.
	DefaultNATSHost = "0.0.0.0"
)

func getService(ctx context.Context) *Service {
	return ctx.Value(nATSKey).(*Service)
}

// Start s.e.
func (s *Service) Start(ctx context.Context) (context.Context, error) {
	var err error
	s.nATSSubscribers, err = connectSubscribers(s)
	if err != nil {
		disconnectSubscribers(s.nATSSubscribers)
		return ctx, err
	}
	s.nATSPublisher, err = connectPublisher(s.NATSServers)
	if err != nil {
		disconnectSubscribers(s.nATSSubscribers)
		return ctx, err
	}
	actx := context.WithValue(ctx, nATSKey, s)
	for _, v := range s.nATSSubscribers {
		var natsHandler nats.MsgHandler
		natsHandler = v.invokeNATSHandler(actx, ibus.RequestHandler)
		err := v.subscribe(natsHandler)
		if err != nil {
			v.worker.conn.Close()
			gochips.Info(err)
		}
	}
	return actx, nil
}

// Stop s.e.
func (s *Service) Stop(ctx context.Context) {
	unsubscribe(s.nATSSubscribers)
	disconnectSubscribers(s.nATSSubscribers)
	s.nATSPublisher.conn.Close()
}
