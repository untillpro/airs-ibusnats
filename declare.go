/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

// Declare s.e.
func Declare(service *Service) {
	godif.ProvideSliceElement(&services.Services, service)
	godif.Provide(&ibus.SendRequest2, implSendRequest2)
	godif.Provide(&ibus.SendResponse, implSendResponse)
	godif.Provide(&ibus.SendParallelResponse2, implSendParallelResponse2)
	// godif.Require(&ibus.RequestHandler) - for router should not be here (no implementation), for bp - required at main()
}

// DeclareTest declares test NATS server. Useful for implement tests using the real NATS server
func DeclareTest(partitionsAmount int) *Service {
	godif.ProvideSliceElement(&services.Services, &testServer{})
	return &Service{
		NATSServers:               "nats://127.0.0.1:4222",
		Parts:                     1,
		CurrentPart:               1,
		Queues:                    map[string]int{"airs-bp": partitionsAmount},
		CurrentQueueName:          "airs-bp",
		AllowedSectionKBitsPerSec: 1000,
	}
}
