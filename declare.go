/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package ibusnats

import (
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

// Declare s.e.
func Declare(service Service) {
	godif.ProvideSliceElement(&services.Services, &service)
	godif.Provide(&ibus.SendRequest, implSendRequest)
	godif.Provide(&ibus.SendResponse, implSendResponse)
	godif.Provide(&ibus.SendParallelResponse, implSendParallelResponse)
	godif.Require(&ibus.RequestHandler)
}
