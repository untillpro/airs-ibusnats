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
	"github.com/untillpro/gochips"
	"math/rand"
	"sync"
	"time"
)

type ChunkedRequestBody []byte

type Service struct {
	Servers            string
	Parts, CurrentPart int
	natsPublisher      *natsPublisher
	natsSubscribers    []*natsSubscriber
}

type contextKeyType string

const (
	natsKey         = contextKeyType("natsKey")
	QueuesPrefix    = "queues"
	DefaultNatsHost = "0.0.0.0"
)

func getService(ctx context.Context) *Service {
	return ctx.Value(natsKey).(*Service)
}

// Start s.e.
func (s *Service) Start(ctx context.Context) (context.Context, error) {
	rand.Seed(time.Now().UnixNano())
	var err error
	s.natsSubscribers, err = connectSubscribers(s)
	if err != nil {
		disconnectSubscribers(s.natsSubscribers)
		return ctx, err
	}
	s.natsPublisher, err = connectPublisher(s.Servers)
	if err != nil {
		disconnectSubscribers(s.natsSubscribers)
		return ctx, err
	}
	for _, v := range s.natsSubscribers {
		var natsHandler nats.MsgHandler
		if v.partitionsNumber == 0 {
			// TODO think about it
			natsHandler = v.invokeNatsNonPartyHandler(ctx, handler)
		} else {
			factory := partitionHandlerFactories[v.queueID]
			natsHandler = v.createNatsPartitionedHandler(ctx, factory)
		}
		err := v.subscribe(natsHandler)
		if err != nil {
			v.worker.conn.Close()
			gochips.Info(err)
		}
	}
	return context.WithValue(ctx, natsKey, s), nil
}

// Stop s.e.
func (s *Service) Stop(ctx context.Context) {
	unsubscribe(s.natsSubscribers)
	disconnectSubscribers(s.natsSubscribers)
	s.natsPublisher.conn.Close()
}
