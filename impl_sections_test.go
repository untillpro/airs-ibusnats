/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/gochips"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

var (
	expected1 = map[string]interface{}{
		"fld1": "fld1Val",
	}
	expected2 = map[string]interface{}{
		"fld2": "fld2Val",
	}
	expected3 = map[string]interface{}{
		"fld3": "fld3Val",
	}
	expected4 = map[string]interface{}{
		"fld4": "fld4Val",
	}
	expectedTotal = map[string]interface{}{
		"total": float64(1),
	}
	ctx context.Context
)

func TestSectionedCommunicationBasic(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		// wrong sender -> no error, nil is returned, error is logged
		require.Nil(t, ibus.SendParallelResponse2(ctx, 42))

		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(t, rs.ObjectSection("secObj", []string{"meta"}, expectedTotal))
		rs.StartMapSection("secMap", []string{"classifier", "2"})
		require.Nil(t, rs.SendElement("id1", expected1))
		require.Nil(t, rs.SendElement("id2", expected2))
		rs.StartArraySection("secArr", []string{"classifier", "4"})
		require.Nil(t, rs.SendElement("", "arrEl1"))
		require.Nil(t, rs.SendElement("", "arrEl2"))
		rs.StartMapSection("deps", []string{"classifier", "3"})
		require.Nil(t, rs.SendElement("id3", expected3))
		require.Nil(t, rs.SendElement("id4", expected4))

		// failed to marshal an element
		require.NotNil(t, rs.SendElement("", func() {}))
		require.NotNil(t, rs.ObjectSection("", nil, func() {}))

		rs.Close(errors.New("test error"))
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
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)

	section := <-sections
	secObj := section.(ibus.IObjectSection)
	require.Equal(t, "secObj", secObj.Type())
	require.Equal(t, []string{"meta"}, secObj.Path())
	valMap := map[string]interface{}{}
	require.Nil(t, json.Unmarshal(secObj.Value(), &valMap))
	require.Equal(t, expectedTotal, valMap)

	// no value on further .Value() calls
	require.Nil(t, secObj.Value())

	section = <-sections
	secMap := section.(ibus.IMapSection)
	require.Equal(t, "secMap", secMap.Type())
	require.Equal(t, []string{"classifier", "2"}, secMap.Path())
	name, value, ok := secMap.Next()
	require.True(t, ok)
	require.Equal(t, "id1", name)
	valMap = map[string]interface{}{}
	require.Nil(t, json.Unmarshal(value, &valMap))
	require.Equal(t, expected1, valMap)
	name, value, ok = secMap.Next()
	require.True(t, ok)
	require.Equal(t, "id2", name)
	valMap = map[string]interface{}{}
	require.Nil(t, json.Unmarshal(value, &valMap))
	require.Equal(t, expected2, valMap)

	// no more elements
	name, value, ok = secMap.Next()
	require.False(t, ok)
	require.Empty(t, name)
	require.Nil(t, value)

	section = <-sections
	secArr := section.(ibus.IArraySection)
	require.Equal(t, "secArr", secArr.Type())
	require.Equal(t, []string{"classifier", "4"}, secArr.Path())
	value, ok = secArr.Next()
	require.True(t, ok)
	val := ""
	require.Nil(t, json.Unmarshal(value, &val))
	require.Equal(t, "arrEl1", val)
	value, ok = secArr.Next()
	require.True(t, ok)
	val = ""
	require.Nil(t, json.Unmarshal(value, &val))
	require.Equal(t, "arrEl2", val)
	value, ok = secArr.Next()

	// no more elements
	require.False(t, ok)
	require.Nil(t, value)

	section = <-sections
	secMap = section.(ibus.IMapSection)
	require.Equal(t, "deps", secMap.Type())
	require.Equal(t, []string{"classifier", "3"}, secMap.Path())
	name, value, ok = secMap.Next()
	require.True(t, ok)
	require.Equal(t, "id3", name)
	valMap = map[string]interface{}{}
	require.Nil(t, json.Unmarshal(value, &valMap))
	require.Equal(t, expected3, valMap)
	name, value, ok = secMap.Next()
	require.True(t, ok)
	require.Equal(t, "id4", name)
	valMap = map[string]interface{}{}
	require.Nil(t, json.Unmarshal(value, &valMap))
	require.Equal(t, expected4, valMap)

	// no more elements
	name, value, ok = secMap.Next()
	require.False(t, ok)
	require.Empty(t, name)
	require.Nil(t, value)

	_, ok = <-sections
	require.False(t, ok)
	require.NotNil(t, *secErr) // test error
}

func TestSectionedEmpty(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)

		// nothingness will not be transferred
		require.Nil(t, rs.ObjectSection("", nil, nil))
		rs.StartMapSection("", nil)
		require.Nil(t, rs.SendElement("", nil))
		rs.StartArraySection("", nil)
		require.Nil(t, rs.SendElement("", nil))

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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, secErr)

	// nothingness will not be transmitted
	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func TestSectionedEmptyButElements(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)

		require.Nil(t, rs.ObjectSection("", nil, 42))
		rs.StartMapSection("", nil)
		require.Nil(t, rs.SendElement("", 42))
		rs.StartArraySection("", nil)
		require.Nil(t, rs.SendElement("", 42))

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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, secErr)

	sec := <-sections
	secObj := sec.(ibus.IObjectSection)
	require.Empty(t, secObj.Type())
	require.Nil(t, secObj.Path())
	require.Equal(t, "42", string(secObj.Value()))

	sec = <-sections
	secMap := sec.(ibus.IMapSection)
	require.Empty(t, secMap.Type())
	require.Nil(t, secMap.Path())
	name, val, ok := secMap.Next()
	require.Empty(t, name)
	require.Equal(t, []byte("42"), val)
	require.True(t, ok)

	sec = <-sections
	secArr := sec.(ibus.IArraySection)
	require.Empty(t, secArr.Type())
	require.Nil(t, secArr.Path())
	val, ok = secArr.Next()
	require.Equal(t, []byte("42"), val)
	require.True(t, ok)

	_, ok = <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func TestSectionedEmptyButElementsAndType(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)

		require.Nil(t, rs.ObjectSection("obj", nil, 42))
		rs.StartMapSection("map", nil)
		require.Nil(t, rs.SendElement("", 42))
		rs.StartArraySection("arr", nil)
		require.Nil(t, rs.SendElement("", 42))

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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, secErr)

	sec := <-sections
	secObj := sec.(ibus.IObjectSection)
	require.Equal(t, "obj", secObj.Type())
	require.Nil(t, secObj.Path())
	require.Equal(t, "42", string(secObj.Value()))

	sec = <-sections
	secMap := sec.(ibus.IMapSection)
	require.Equal(t, "map", secMap.Type())
	require.Nil(t, secMap.Path())
	name, val, ok := secMap.Next()
	require.Empty(t, name)
	require.Equal(t, []byte("42"), val)
	require.True(t, ok)

	sec = <-sections
	secArr := sec.(ibus.IArraySection)
	require.Equal(t, "arr", secArr.Type())
	require.Nil(t, secArr.Path())
	val, ok = secArr.Next()
	require.Equal(t, []byte("42"), val)
	require.True(t, ok)
}

func TestReadFirstPacketTimeout(t *testing.T) {
	ch := make(chan byte)
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		ibus.SendParallelResponse2(ctx, sender)
		time.Sleep(600 * time.Millisecond)
		ch <- 1
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
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, 300*time.Millisecond)
	require.Nil(t, sections)
	require.True(t, errors.Is(err, ibus.ErrTimeoutExpired))
	fmt.Println(err)
	require.Nil(t, secErr)
	<-ch // to avoid writting to ibus.SendParallelResponse2 on godif.Reset() and ibus.RequestHandler() working -> datarace
}

func TestReadSectionPacketTimeout(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(t, rs.ObjectSection("", nil, 42))
		time.Sleep(300 * time.Millisecond)
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
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, 150*time.Millisecond)
	sec, ok := <-sections
	sec.(ibus.IObjectSection).Value()
	require.True(t, ok)
	_, ok = <-sections
	require.False(t, ok)
	require.True(t, errors.Is(*secErr, ibus.ErrTimeoutExpired))
	fmt.Println(*secErr)
	require.Nil(t, err)
}

func TestSectionedCommunicationTimeout(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(t, rs.ObjectSection("", nil, 42))
		time.Sleep(100 * time.Millisecond)
		require.Nil(t, rs.ObjectSection("", nil, 43))
		time.Sleep(100 * time.Millisecond)
		require.Nil(t, rs.ObjectSection("", nil, 44))
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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, 150*time.Millisecond)
	sec := <-sections
	sec.(ibus.IObjectSection).Value()
	sec = <-sections
	sec.(ibus.IObjectSection).Value()
	sec = <-sections
	sec.(ibus.IObjectSection).Value()
	_, ok := <-sections
	require.False(t, ok)
	require.True(t, errors.Is(*secErr, ibus.ErrTimeoutExpired))
	fmt.Println(*secErr)
	require.Nil(t, err)

}

func TestStopOnMapSectionNextElemOnTimeout(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartMapSection("secArr", []string{"class"})
		require.Nil(t, rs.SendElement("f1", "v1"))
		require.Nil(t, rs.SendElement("f1", "v2"))
		require.Nil(t, rs.SendElement("f1", "v3"))

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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, 150*time.Millisecond)
	require.Nil(t, err, err)
	require.NotNil(t, secErr)

	section := <-sections
	mapSec := section.(ibus.IMapSection)
	_, _, ok := mapSec.Next() // came with section, no timeout, going to write next
	require.True(t, ok)
	time.Sleep(300 * time.Millisecond)
	_, _, ok = mapSec.Next() //next written, going to check timeout at getSectionsFromNAT()
	require.True(t, ok)
	name, val, ok := mapSec.Next() // closed because timeout
	require.False(t, ok)
	require.Nil(t, val)
	require.Empty(t, name)
	_, ok = <-sections
	require.False(t, ok)
	require.NotNil(t, *secErr) // communicating is terminated because it took too much time
}

func TestStopOnArraySectionNextElemOnTimeout(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartArraySection("secArr", []string{"class"})
		require.Nil(t, rs.SendElement("", "arrEl1"))
		require.Nil(t, rs.SendElement("", "arrEl2"))
		require.Nil(t, rs.SendElement("", "arrEl3"))

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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, 150*time.Millisecond)
	require.Nil(t, err, err)
	require.NotNil(t, secErr)

	section := <-sections
	arrSec := section.(ibus.IArraySection)
	_, ok := arrSec.Next() // came with section, no timeout, going to write next
	require.True(t, ok)
	time.Sleep(300 * time.Millisecond)
	_, ok = arrSec.Next() //next written, going to check timeout at getSectionsFromNAT()
	require.True(t, ok)
	val, ok := arrSec.Next() // closed because timeout
	require.False(t, ok)
	require.Nil(t, val)
	_, ok = <-sections
	require.False(t, ok)
	require.NotNil(t, *secErr) // communicating is terminated because it took too much time
}

func TestMapElementRawBytes(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		elementJSONBytes, err := json.Marshal(&expected3)
		require.Nil(t, err)
		// map element
		rs.StartMapSection("deps", []string{"classifier", "3"})
		rs.SendElement("id3", elementJSONBytes)

		// object
		require.Nil(t, rs.ObjectSection("objSec", []string{"meta"}, elementJSONBytes))
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
	}
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)

	section := <-sections
	secMap := section.(ibus.IMapSection)
	require.Equal(t, "deps", secMap.Type())
	require.Equal(t, []string{"classifier", "3"}, secMap.Path())
	name, value, ok := secMap.Next()
	require.True(t, ok)
	require.Equal(t, "id3", name)
	valMap := map[string]interface{}{}
	require.Nil(t, json.Unmarshal(value, &valMap))
	require.Equal(t, expected3, valMap)
	name, value, ok = secMap.Next()
	require.False(t, ok)
	require.Empty(t, name)
	require.Nil(t, value)

	section = <-sections
	secObj := section.(ibus.IObjectSection)
	require.Equal(t, []string{"meta"}, secObj.Path())
	require.Equal(t, "objSec", secObj.Type())
	valMap = map[string]interface{}{}
	require.Nil(t, json.Unmarshal(secObj.Value(), &valMap))
	require.Equal(t, expected3, valMap)
	require.Nil(t, secObj.Value())

	_, ok = <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func setUp() {
	DeclareTest(1)
	godif.Require(&ibus.SendParallelResponse2)
	godif.Require(&ibus.SendRequest2)
	godif.Require(&ibus.SendResponse)
	godif.Require(&ibus.RequestHandler)
	var err error
	ctx, err = services.ResolveAndStart()
	gochips.PanicIfError(err)
	getService(ctx).Verbose = true
}

func tearDown() {
	services.StopAndReset(ctx)
}

func mapFromJSON(jsonBytes []byte) map[string]interface{} {
	res := map[string]interface{}{}
	if err := json.Unmarshal(jsonBytes, &res); err != nil {
		panic(err)
	}
	return res
}
