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
func Declare(srv *Service) {
	godif.ProvideSliceElement(&services.Services, srv)
	godif.Provide(&ibus.SendRequest2, implSendRequest2)
	godif.Provide(&ibus.SendResponse, implSendResponse)
	godif.Provide(&ibus.SendParallelResponse2, implSendParallelResponse2)
	// godif.Require(&ibus.RequestHandler) - for router should not be here (no implementation), for bp - required at main()
}

// DeclareEmbeddedNATSServer declares test NATS server. Useful for implement tests using the real NATS server
func DeclareEmbeddedNATSServer() {
	godif.ProvideSliceElement(&services.Services, &embeddedNATSServer{})
}
