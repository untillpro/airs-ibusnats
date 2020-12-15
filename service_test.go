/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

/*

	Test service start/stop here

*/

package ibusnats

//
//import (
//	"context"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//	"github.com/untillpro/airs-iqueues"
//	"github.com/untillpro/godif"
//	"testing"
//
//	"github.com/untillpro/godif/services"
//)
//
//func start(t *testing.T) context.Context {
//	godif.Require(&iqueues.InvokeFromHTTPRequest)
//	godif.Require(&iqueues.Invoke)
//	// Declare own service
//	Declare(Service{NATSServers: "0.0.0.0"})
//	godif.ProvideKeyValue(&iqueues.NonPartyHandlers, "air-bo-view:0", iqueues.AirBoView)
//	godif.ProvideKeyValue(&iqueues.PartitionHandlerFactories, "air-bo:10", iqueues.Factory)
//	ctx, err := services.ResolveAndStart()
//	require.Nil(t, err)
//	return ctx
//}
//
//func TestService_Start(t *testing.T) {
//	testServices := []struct {
//		serv             Service
//		numOfSubscribers int
//	}{
//		//1 sub for air-bo-view:0, others for air-bo:10
//		{Service{NATSServers: "0.0.0.0", Parts: 4, CurrentPart: 4}, 5},
//		{Service{NATSServers: "0.0.0.0", Parts: 4, CurrentPart: 3}, 3},
//		{Service{NATSServers: "0.0.0.0", Parts: 10, CurrentPart: 5}, 2},
//		{Service{NATSServers: "0.0.0.0", Parts: 15, CurrentPart: 5}, 2},
//		{Service{NATSServers: "0.0.0.0", Parts: 15, CurrentPart: 12}, 1},
//		{Service{NATSServers: "0.0.0.0", Parts: 11, CurrentPart: 11}, 1},
//		{Service{NATSServers: "0.0.0.0", Parts: 0, CurrentPart: 0}, 11},
//	}
//	for _, v := range testServices {
//		godif.Require(&iqueues.InvokeFromHTTPRequest)
//		godif.Require(&iqueues.Invoke)
//		// Declare own service
//		Declare(v.serv)
//		godif.ProvideKeyValue(&iqueues.NonPartyHandlers, "air-bo-view:0", iqueues.AirBoView)
//		godif.ProvideKeyValue(&iqueues.PartitionHandlerFactories, "air-bo:10", iqueues.Factory)
//		ctx, err := services.ResolveAndStart()
//		require.Nil(t, err)
//		v.serv = *getService(ctx)
//		//4 for air-bo and 1 for air-bo-view
//		assert.Equal(t, v.numOfSubscribers, len(v.serv.nATSSubscribers))
//		services.StopAndReset(ctx)
//	}
//}
//
//func stop(ctx context.Context, t *testing.T) {
//	services.StopServices(ctx)
//	godif.Reset()
//}
