/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
)

// Service s.e.
type Service struct {
	// Comma separated list of servers
	NATSServers      string
	Queues           map[string]int
	CurrentQueueName string
	Parts            int
	CurrentPart      int
	Verbose          bool // verbose debug log if true
	nATSPublisher    *nats.Conn
	nATSSubscribers  map[int]*nATSSubscriber // partitionNumber->subscriber
}

type contextKeyType string

const nATSKey = contextKeyType("nATSKey")

func getService(ctx context.Context) *Service {
	return ctx.Value(nATSKey).(*Service)
}

// Start s.e.
func (s *Service) Start(ctx context.Context) (newCtx context.Context, err error) {
	if s.nATSSubscribers, err = connectSubscribers(s); err != nil {
		return
	}
	if s.nATSPublisher, err = connectToNATS(s.NATSServers, "NATSPublisher"); err != nil {
		return
	}
	newCtx = context.WithValue(ctx, nATSKey, s)
	for _, v := range s.nATSSubscribers {
		var natsHandler nats.MsgHandler
		natsHandler = generateNATSHandler(newCtx)
		if err = v.subscribe(natsHandler); err != nil {
			v.conn.Close()
			return nil, err
		}
	}
	return
}

func generateNATSHandler(ctx context.Context) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var req ibus.Request
		sender := senderImpl{partNumber: req.PartitionNumber, replyTo: msg.Reply}
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			ibus.SendResponse(ctx, sender,
				ibus.CreateErrorResponse(http.StatusBadRequest, fmt.Errorf("request unmarshal failed: %w", err)))
			return
		}
		ibus.RequestHandler(ctx, sender, req)
	}
}

// Stop s.e.
func (s *Service) Stop(ctx context.Context) {
	unsubscribe(s.nATSSubscribers)
	disconnectSubscribers(s.nATSSubscribers)
	s.nATSPublisher.Close()
}
