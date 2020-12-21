/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
)

func TestBasic(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		dataBytes, err := json.Marshal(expected1)
		require.Nil(t, err)
		resp := ibus.Response{
			ContentType: "application/json",
			StatusCode:  http.StatusOK,
			Data:        dataBytes,
		}
		ibus.SendResponse(ctx, sender, resp)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
	}
	resp, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.Nil(t, secErr)
	require.Nil(t, sections)
	require.Equal(t, "application/json", resp.ContentType)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	m := map[string]interface{}{}
	require.Nil(t, json.Unmarshal(resp.Data, &m))
	require.Equal(t, expected1, m)
}

func TestEmpty(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		resp := ibus.Response{
			StatusCode: http.StatusOK,
		}
		ibus.SendResponse(ctx, sender, resp)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
	}
	resp, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.Nil(t, secErr)
	require.Nil(t, sections)
	require.Equal(t, "", resp.ContentType)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Nil(t, resp.Data)
}

func TestRequestUnmarshalFailed(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		t.Fatal()
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
		Body:            []byte{0}, // unmarshallable
	}
	worker := getService(ctx)
	qName := req.QueueID + strconv.Itoa(req.PartitionNumber)
	resp, _, _, err := sendToNATSAndGetResp(ctx, worker.nATSPublisher, []byte("unmarshallable"), qName, ibus.DefaultTimeout, false)
	require.Nil(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	fmt.Println(string(resp.Data))
}

func TestWrongSenderProvidedOnResponse(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		resp := ibus.Response{
			StatusCode: http.StatusOK,
		}
		ibus.SendResponse(ctx, "wrong sender", resp)
		ibus.SendResponse(ctx, sender, resp)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
		Body:            []byte{0}, // unmarshallable
	}

	resp, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Nil(t, sections)
	require.Nil(t, secErr)
	require.Nil(t, err)
}

func TestMultipleResponse(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		resp := ibus.Response{
			StatusCode: http.StatusOK,
		}
		ibus.SendResponse(ctx, sender, resp)
		resp = ibus.Response{
			StatusCode: http.StatusInternalServerError,
		}
		// no error, the response is actually sent but skipped because publisher unsubscribes from the topic after first response
		ibus.SendResponse(ctx, sender, resp)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
		Body:            []byte{0}, // unmarshallable
	}

	resp, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Nil(t, sections)
	require.Nil(t, secErr)
	require.Nil(t, err)
}

func TestMultipleParallelResponse(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.ObjectSection("obj1", []string{"meta"}, 42)

		// nothing happens, no error. Publisher will receive sections as they're sent
		rs = ibus.SendParallelResponse2(ctx, sender)
		rs.ObjectSection("obj2", []string{"meta"}, 43)
		rs.Close(nil)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
		Body:            []byte{0}, // unmarshallable
	}

	_, sections, secErr, _ := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)

	sec := <-sections
	secObj := sec.(ibus.IObjectSection)
	require.NotNil(t, secObj.Value())

	sec = <-sections
	secObj = sec.(ibus.IObjectSection)
	require.NotNil(t, secObj.Value())

	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func TestNormalResponseAfterParallelResponse(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.ObjectSection("obj1", []string{"meta"}, 42)

		resp := ibus.Response{
			StatusCode: http.StatusInternalServerError,
		}
		ibus.SendResponse(ctx, sender, resp)

		rs.Close(nil)
	})

	setUp()
	defer tearDown()

	req := ibus.Request{
		Method:          ibus.HTTPMethodPOST,
		QueueID:         "airs-bp",
		WSID:            1,
		PartitionNumber: 0,
		Resource:        "none",
		Body:            []byte{0}, // unmarshallable
	}

	_, sections, secErr, _ := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)

	sec := <-sections
	secObj := sec.(ibus.IObjectSection)
	require.NotNil(t, secObj.Value())

	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}
