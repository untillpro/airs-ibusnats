/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

// BenchmarkSectionedRequestResponse/#00-4         	    1888	    651566 ns/op	      1535 rps	  145574 B/op	     288 allocs/op

func BenchmarkSectionedRequestResponse(b *testing.B) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(b, rs.ObjectSection("secObj", []string{"meta"}, expectedTotal))
		rs.StartMapSection("secMap", []string{"classifier", "2"})
		require.Nil(b, rs.SendElement("id1", expected1))
		require.Nil(b, rs.SendElement("id2", expected2))
		rs.StartArraySection("secArr", []string{"classifier", "4"})
		require.Nil(b, rs.SendElement("", "arrEl1"))
		require.Nil(b, rs.SendElement("", "arrEl2"))
		rs.StartMapSection("deps", []string{"classifier", "3"})
		require.Nil(b, rs.SendElement("id3", expected3))
		require.Nil(b, rs.SendElement("id4", expected4))

		// failed to marshal an element
		require.NotNil(b, rs.SendElement("", func() {}))
		require.NotNil(b, rs.ObjectSection("", nil, func() {}))

		rs.Close(errors.New("test error"))
	})

	setUp()
	defer tearDown()

	srv.Verbose = false
	services.SetVerbose(false)

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
	}

	b.Run("", func(b *testing.B) {
		start := time.Now()
		for i := 0; i < b.N; i++ {
			_, sections, _, _ := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)

			section := <-sections
			secObj := section.(ibus.IObjectSection)
			secObj.Value()

			section = <-sections
			secMap := section.(ibus.IMapSection)
			secMap.Next()
			secMap.Next()

			section = <-sections
			secArr := section.(ibus.IArraySection)
			secArr.Next()
			secArr.Next()

			section = <-sections
			secMap = section.(ibus.IMapSection)
			secMap.Next()
			secMap.Next()

			if _, ok := <-sections; ok {
				b.Fatal()
			}
		}
		elapsed := time.Since(start).Seconds()
		b.ReportMetric(float64(b.N)/elapsed, "rps")
	})
}
