/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

func TestServiceStartErrors(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	// unknown CurrentQueueName -> error
	service := &Service{
		NATSServers:      "nats://127.0.0.1:4222",
		Parts:            1,
		CurrentPart:      1,
		Queues:           map[string]int{"airs-bp": 1},
		CurrentQueueName: "unknown",
	}
	Declare(service)
	godif.Require(&ibus.SendRequest2)
	ctx, err := services.ResolveAndStart()
	require.NotNil(t, err)
	require.Nil(t, ctx)
	godif.Reset()

	// failed to connect subscribers -> error
	service.CurrentQueueName = "airs-bp"
	service.NATSServers = "nats://127.0.0.1:4222"
	patches := gomonkey.ApplyFuncSeq(nats.Connect, []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("test error")}},
	})
	Declare(service)
	godif.Require(&ibus.SendRequest2)
	ctx, err = services.ResolveAndStart()
	require.NotNil(t, err)
	require.Nil(t, ctx)
	patches.Reset()
	godif.Reset()

	// failed to connect publisher -> error
	patches = gomonkey.ApplyFuncSeq(nats.Connect, []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, nil}},
		{Values: gomonkey.Params{nil, errors.New("test error")}},
	})
	Declare(service)
	godif.Require(&ibus.SendRequest2)
	ctx, err = services.ResolveAndStart()
	require.NotNil(t, err)
	require.Nil(t, ctx)
	patches.Reset()
	godif.Reset()
}

func TestNoSubscribersOnEmptyCurrentQueueName(t *testing.T) {
	DeclareTest(1)
	service := &Service{
		NATSServers:      "nats://127.0.0.1:4222",
		Parts:            1,
		CurrentPart:      1,
		Queues:           map[string]int{"airs-bp": 1},
		CurrentQueueName: "",
	}
	Declare(service)
	godif.Require(&ibus.SendRequest2)
	ctx, err := services.ResolveAndStart()
	require.Nil(t, err)
	defer services.StopAndReset(ctx)
	require.NotNil(t, ctx)
	require.Empty(t, srv.nATSSubscribers)
}

func TestServiceStartErrors2(t *testing.T) {
	t.Skip("gomonkey bug: works in debug mode only")
	opts := natsserver.DefaultTestOptions
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	// empty CurrentQueueName -> service start error
	service := &Service{
		NATSServers:      "nats://127.0.0.1:4222",
		Parts:            1,
		CurrentPart:      1,
		Queues:           map[string]int{"airs-bp": 1},
		CurrentQueueName: "airs-bp",
	}

	// error on nats.Conn.QueueSubscribe
	var conn *nats.Conn
	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueueSubscribe", func(c *nats.Conn, subj, queue string,
		cb nats.MsgHandler) (*nats.Subscription, error) {
		return nil, errors.New("test error")
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "QueueSubscribe", func(c *nats.Conn, subj, queue string,
		cb nats.MsgHandler) (*nats.Subscription, error) {
		return nil, errors.New("test error")
	})
	Declare(service)
	godif.Require(&ibus.SendRequest2)
	ctx, err := services.ResolveAndStart()
	require.NotNil(t, err)
	require.Nil(t, ctx)
	patches.Reset()
	godif.Reset()
}

func TestReconnect(t *testing.T) {
	ch := make(chan struct{})
	reconnectCh := make(chan struct{})
	reconnectedCh := make(chan struct{})
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		defer rs.Close(nil)
		if err := rs.ObjectSection("obj1", nil, 42); err != nil {
			t.Fatal("nil expected on ObjectSection:", err)
		}
		<-ch // wait for reconnection is done
		// communicate after reconnect
		require.Nil(t, rs.ObjectSection("obj2", nil, 43))
	})

	onReconnect = func() {
		// will be called 2 times: 1 subcriber and 1 publisher
		reconnectCh <- struct{}{}
	}
	onBeforeMiscSend = func() {
		// wait for publisher and subscribers reconnection
		<-reconnectedCh
		onBeforeMiscSend = nil
	}

	onBeforeContinuationReceive = func() {
		// wait for publisher and subscribers reconnection
		<-reconnectedCh
		onBeforeContinuationReceive = nil
	}

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err)

	sec := <-sections
	secObj := sec.(ibus.IObjectSection)
	require.Equal(t, "42", string(secObj.Value()))

	// now stop the server
	ts := getTestServer(ctx)
	ts.s.Shutdown()

	// start server
	ctx, err = ts.Start(ctx)
	require.Nil(t, err)

	// check reconnect is handled
	// wait for 2 reconnections: 1 subscriber and publisher
	<-reconnectCh
	<-reconnectCh
	go func() {
		// signal for requester and handler to send and receive continuation respectively
		reconnectedCh <- struct{}{}
		reconnectedCh <- struct{}{}
	}()

	ch <- struct{}{} // signal to write next section
	sec = <-sections
	secObj = sec.(ibus.IObjectSection)
	require.Equal(t, "43", string(secObj.Value()))

	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func TestPartsAssigning(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	srvTests := map[*[]int]*Service{
		{0, 1, 2}: {
			Parts:       2,
			CurrentPart: 1,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{}: {
			Parts:       12,
			CurrentPart: 10,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{4}: {
			Parts:       12,
			CurrentPart: 5,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{4}: {
			Parts:       24,
			CurrentPart: 5,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{3, 4, 5}: {
			Parts:       2,
			CurrentPart: 2,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{0, 1, 2, 3, 4, 5}: {
			Parts:       1,
			CurrentPart: 1,
			Queues:      map[string]int{"airs-bp": 6},
		},
		{0}: {
			Parts:       6,
			CurrentPart: 1,
			Queues:      map[string]int{"airs-bp": 6},
		},
	}
	for partNumbers, srv := range srvTests {
		srv.NATSServers = "nats://127.0.0.1:4222"
		srv.CurrentQueueName = "airs-bp"
		Declare(srv)
		godif.Require(&ibus.SendRequest2)
		ctx, err := services.ResolveAndStart()
		require.Nil(t, err)
		require.Len(t, srv.nATSSubscribers, len(*partNumbers))
		services.StopAndReset(ctx)
	}
}

func TestCover(t *testing.T) {
	logStack("test", nil)
	logStack("test", ibus.ErrTimeoutExpired)
	_ = busPacketType(len(_busPacketType_index)).String()

}

func TestGetService(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	expectedService := &Service{
		NATSServers:      "nats://127.0.0.1:4222",
		Parts:            1,
		CurrentPart:      1,
		Queues:           map[string]int{"airs-bp": 1},
		CurrentQueueName: "airs-bp",
	}
	Declare(expectedService)
	godif.Require(&ibus.SendRequest2)
	ctx, err := services.ResolveAndStart()
	require.Nil(t, err)
	require.Equal(t, expectedService, GetService(ctx))
	services.StopAndReset(ctx)
}
