/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
)

var srv *Service

// Service s.e.
type Service struct {
	NATSServers      NATSServers
	Queues           QueuesPartitionsMap
	CurrentQueueName CurrentQueueName // queue to subscribe on. Empty -> no subscriptions (e.g. router has nothing to subscribe on)
	Parts            int
	CurrentPart      int
	Verbose          Verbose // verbose debug log if true
	nATSPublisher    *nats.Conn
	nATSSubscribers  map[int]*nATSSubscriber // partitionNumber->subscriber
}

type contextKeyType string

const nATSKey = contextKeyType("nATSKey")

// Start s.e.
func (s *Service) Start(ctx context.Context) (newCtx context.Context, err error) {
	srv = s
	if err = s.connectSubscribers(); err != nil {
		return
	}
	if s.nATSPublisher, err = connectToNATS(s.NATSServers, "NATSPublisher", bool(s.Verbose)); err != nil {
		return
	}
	newCtx = context.WithValue(ctx, nATSKey, s)
	for _, v := range s.nATSSubscribers {
		natsHandler := func(msg *nats.Msg) {
			nATSMsgHandler(newCtx, msg)
		}
		if err = v.subscribe(natsHandler); err != nil {
			v.conn.Close()
			return nil, err
		}
	}
	return
}

func (s *Service) connectSubscribers() error {
	if len(s.CurrentQueueName) == 0 {
		// router has no subscribers
		return nil
	}
	numOfSubjects, ok := s.Queues[string(s.CurrentQueueName)] // 100 (hardcoded at airs-bp2 main())
	if !ok {
		return errors.New("can't find number of subjects in queues map")
	}
	minPart := 0
	maxPart := numOfSubjects
	// these are zero -> publisher only, e.g. router
	if s.Parts != 0 && s.CurrentPart != 0 {
		if s.Parts > numOfSubjects {
			if s.CurrentPart >= numOfSubjects {
				minPart = 0
				maxPart = 0
			} else {
				minPart = s.CurrentPart - 1
				maxPart = s.CurrentPart
			}
		} else {
			partitionsInOnePart := numOfSubjects / s.Parts
			if s.Parts == s.CurrentPart {
				maxPart = numOfSubjects
			} else {
				maxPart = s.CurrentPart * partitionsInOnePart
			}
			minPart = partitionsInOnePart * (s.CurrentPart - 1)
		}
	}
	log.Println("Partition range:", minPart, "-", maxPart-1)
	s.nATSSubscribers = map[int]*nATSSubscriber{}
	for i := minPart; i < maxPart; i++ {
		conn, err := connectToNATS(s.NATSServers, string(s.CurrentQueueName)+strconv.Itoa(i), bool(s.Verbose))
		if err != nil {
			return err
		}
		s.nATSSubscribers[i] = &nATSSubscriber{conn, nil}
	}
	return nil
}

func nATSMsgHandler(ctx context.Context, msg *nats.Msg) {
	var req ibus.Request
	json.Unmarshal(msg.Data, &req) // assuming error is impossible because request came to NATS through ibus.SendRequest2() only which properly marshals it to JSON
	sender := senderImpl{partNumber: req.PartitionNumber, replyTo: msg.Reply}
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			ibus.SendResponse(ctx, sender, ibus.CreateErrorResponse(http.StatusInternalServerError,
				fmt.Errorf("ibus.RequestHandler paniced: %v\n%s", r, string(stack))))
		}
	}()
	ibus.RequestHandler(ctx, sender, req)
}

func connectToNATS(servers NATSServers, subjName string, verbose bool) (conn *nats.Conn, err error) {
	opts := setupConnOptions([]nats.Option{nats.Name(subjName)}, verbose)
	opts = setupConnOptions(opts, verbose)
	conn, err = nats.Connect(strings.Join(servers, ","), opts...)
	return
}

// Stop s.e.
func (s *Service) Stop(ctx context.Context) {
	// unsubscribe
	for _, s := range s.nATSSubscribers {
		if err := s.subscription.Unsubscribe(); err != nil {
			log.Printf("unsubscribe %s failed: %v\n", s.subscription.Subject, err)
		}
	}
	// disconnect subscribers
	for _, s := range s.nATSSubscribers {
		s.conn.Close()
	}
	// disconnect publisher
	s.nATSPublisher.Close()
}
