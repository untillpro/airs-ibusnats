/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusnats

import "context"

// GetService use in tests only
func GetService(ctx context.Context) *Service {
	return ctx.Value(nATSKey).(*Service)
}
