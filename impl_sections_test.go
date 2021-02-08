/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/godif"
	"github.com/untillpro/godif/services"
)

func TestSectionedCommunicationBasic(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		require.Panics(t, func() { ibus.SendParallelResponse2(ctx, 42) })

		rs := ibus.SendParallelResponse2(ctx, sender)
		defer rs.Close(errors.New("test error"))

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
	_, sections, secErr, err := ibus.SendRequest2(ctx, req, time.Hour)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

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
	require.NotNil(t, *secErr, *secErr) // test error
}

func TestSectionedEmpty(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)

		// nothingness will not be trasmitted
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
	require.NotNil(t, sections)

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
	require.NotNil(t, sections)

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
	require.NotNil(t, sections)

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

	_, ok = <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
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
	require.Error(t, ibus.ErrTimeoutExpired, err)
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
	require.Nil(t, err)
	require.NotNil(t, sections)

	sec, ok := <-sections
	sec.(ibus.IObjectSection).Value()
	require.True(t, ok)

	_, ok = <-sections
	require.False(t, ok)
	require.Error(t, ibus.ErrTimeoutExpired, *secErr)
	fmt.Println(*secErr)
}

func TestNoConsumerOnContextDone(t *testing.T) {
	ch := make(chan struct{})
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(t, rs.ObjectSection("objSec", []string{"class"}, 42))
		<-ch // wait for context cancel
		// context is closed here so next communication will cause ErrNoConsumer error
		// note: further will cause ibus.ErrTimeoutExpired due of no data on misc inbox
		require.Error(t, ErrNoConsumer, rs.ObjectSection("objSec", []string{"class"}, 43))

		// allowed but senceless
		rs.Close(nil)

		ch <- struct{}{}
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
	require.NotNil(t, sections)

	// first section is ok
	section := <-sections
	objSec := section.(ibus.IObjectSection)
	require.Equal(t, "42", string(objSec.Value()))

	// requester now waits for data from NATS
	// then will check the context
	cancel()
	ch <- struct{}{} //signal to writer to send something more after context cancel
	// requeter receives next section but sees that ctx.Done()

	// will not receive anything more
	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)

	<-ch
}

func TestSlowConsumerObjectSection(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Nil(t, rs.ObjectSection("objSec", []string{"class"}, 42))

		// next section will be actualy sent but `slow consumer` situation will be detected
		require.Error(t, ErrSlowConsumer, rs.ObjectSection("objSec", []string{"class"}, 43))

		// not necessary because requester is unsubscribed from topic after `slow consumer` detection
		// call anyway to cover
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

	atomic.StoreInt32(&srv.AllowedSectionKBitsPerSec, 400) // 50 msecs section consume timeout

	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

	// first read is normal.
	section := <-sections
	objSec := section.(ibus.IObjectSection)
	require.Equal(t, "42", string(objSec.Value()))

	// simulate slow objSec processing. E.g. router sends it to a slow http client

	SetSectionConsumeAddonTimeout(0)
	time.Sleep(200 * time.Millisecond)

	// next section will actually sent to NATS, actually received but failed to write to `sections` channel due of timeout
	_, ok := <-sections
	require.False(t, ok)
	require.Error(t, ErrSlowConsumer, *secErr)
}

func TestSlowConsumerArraySection(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartArraySection("arrSec", []string{"class"})
		require.Nil(t, rs.SendElement("elem", 42))

		// next section will be actualy sent but `slow consumer` situation will be detected
		rs.StartArraySection("arrSec2", []string{"class"})
		require.Error(t, ErrSlowConsumer, rs.SendElement("elem", 42))

		// not necessary because requester is unsubscribed from topic after `slow consumer` detection
		// call anyway to cover
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
	atomic.StoreInt32(&srv.AllowedSectionKBitsPerSec, 400) // 50 msecs section consume timeout

	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

	// first read is normal.
	section := <-sections
	arrSec := section.(ibus.IArraySection)
	val, _ := arrSec.Next()
	require.Equal(t, "42", string(val))

	// simulate slow objSec processing. E.g. router sends it to a slow http client
	SetSectionConsumeAddonTimeout(0)
	time.Sleep(200 * time.Millisecond)

	// next section will actually sent to NATS, actually received but failed to write to `sections` channel due of timeout
	_, ok := <-sections
	require.False(t, ok)
	require.Error(t, ErrSlowConsumer, *secErr)
}

func TestSlowConsumerMapSection(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartMapSection("mapSec", []string{"class"})
		require.Nil(t, rs.SendElement("elem", 42))

		// next section will be actualy sent but `slow consumer` situation will be detected
		rs.StartMapSection("mapSec2", []string{"class"})
		require.Error(t, ErrSlowConsumer, rs.SendElement("elem", 42))

		// not necessary because requester is unsubscribed from topic after `slow consumer` detection
		// call anyway to cover
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
	atomic.StoreInt32(&srv.AllowedSectionKBitsPerSec, 400) // 50 msecs section consume timeout

	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

	// first read is normal.
	section := <-sections
	mapSec := section.(ibus.IMapSection)
	name, val, _ := mapSec.Next()
	require.Equal(t, "elem", name)
	require.Equal(t, "42", string(val))

	// simulate slow objSec processing. E.g. router sends it to a slow http client
	SetSectionConsumeAddonTimeout(0)
	time.Sleep(200 * time.Millisecond)

	// next section will actually sent to NATS, actually received but failed to write to `sections` channel due of timeout
	_, ok := <-sections
	require.False(t, ok)
	require.Error(t, ErrSlowConsumer, *secErr)
}

func TestSlowConsumerFirstElement(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartMapSection("mapSec", []string{"class"})
		// next element will be actualy sent but `slow consumer` situation will be detected
		require.Error(t, ErrSlowConsumer, rs.SendElement("elem", 42))

		// not necessary because requester is unsubscribed from topic after `slow consumer` detection
		// call anyway to cover
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
	atomic.StoreInt32(&srv.AllowedSectionKBitsPerSec, 400) // 50 msecs section consume timeout

	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

	SetSectionConsumeAddonTimeout(0)

	// first read is normal.
	section := <-sections
	mapSec := section.(ibus.IMapSection)

	// simulate slow section processing before read first element

	time.Sleep(200 * time.Millisecond)

	// first elem will actually sent to NATS, actually received but failed to write to `elem` channel due of timeout
	_, _, ok := mapSec.Next()
	require.False(t, ok)

	_, ok = <-sections
	require.False(t, ok)
	require.Error(t, ErrSlowConsumer, *secErr)
}

func TestSlowConsumerNextElement(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartMapSection("mapSec", []string{"class"})
		require.Nil(t, rs.SendElement("elem", 42))

		// next element will be actualy sent but `slow consumer` situation will be detected
		require.Error(t, ErrSlowConsumer, rs.SendElement("elem2", 43))

		// not necessary because requester is unsubscribed from topic after `slow consumer` detection
		// call anyway to cover
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
	atomic.StoreInt32(&srv.AllowedSectionKBitsPerSec, 400) // 50 msecs section consume timeout

	_, sections, secErr, err := ibus.SendRequest2(ctx, req, ibus.DefaultTimeout)
	require.Nil(t, err, err)
	require.NotNil(t, sections)

	// first read is normal.
	section := <-sections
	mapSec := section.(ibus.IMapSection)
	name, val, _ := mapSec.Next()
	require.Equal(t, "elem", name)
	require.Equal(t, "42", string(val))

	// simulate slow section processing before read first element
	SetSectionConsumeAddonTimeout(0)
	time.Sleep(200 * time.Millisecond)

	// next elem will actually sent to NATS, actually received but failed to write to `elem` channel due of timeout
	_, _, ok := mapSec.Next()
	require.False(t, ok)

	_, ok = <-sections
	require.False(t, ok)
	require.Error(t, ErrSlowConsumer, *secErr)
}

func TestStopOnMapSectionNextElemContextDone(t *testing.T) {
	ch := make(chan struct{})
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartMapSection("secArr", []string{"class"})
		require.Nil(t, rs.SendElement("f1", "v1"))
		<-ch // wait for context cancel
		require.Error(t, ErrNoConsumer, rs.SendElement("f1", "v2"))

		// wrong to send anything more. will fail on timeout reading from misc channel
		// require.Error(t, ibus.ErrTimeoutExpired, rs.SendElement("f1", "v3")) // requester will not send to misc inbox -> timeout reading from misc inbox

		// allowed but senceless. Will be sent to NATS and disappeared into the void...
		rs.Close(nil)
		ch <- struct{}{}
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
	require.NotNil(t, sections)

	section := <-sections
	mapSec := section.(ibus.IMapSection)
	_, _, ok := mapSec.Next() // came with section. Writer is going to write next, waiting for `<-ch`
	require.True(t, ok)

	cancel()
	ch <- struct{}{} //signal to writer to send something more after context cancel

	name, val, ok := mapSec.Next() // closed because context is done. Further 2 sections are lost.
	require.False(t, ok)
	require.Nil(t, val)
	require.Empty(t, name)

	// further sections are sent by handler but lost on requester side
	_, ok = <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)

	<-ch
}

func TestStopOnArraySectionNextElemOnContextDone(t *testing.T) {
	ch := make(chan struct{})
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		rs.StartArraySection("secArr", []string{"class"})
		require.Nil(t, rs.SendElement("", "arrEl1"))
		<-ch //wait for context close
		require.Error(t, ErrNoConsumer, rs.SendElement("", "arrEl2"))

		// wrong to send anything more. Will fail on timeout reading from misc channel
		// require.Error(t, ErrNoConsumer, rs.SendElement("", "arrEl3"))

		// allowed but senceless. Will be sent to NATS and disappeared into the void...
		rs.Close(nil)
		ch <- struct{}{}
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
	require.NotNil(t, sections)

	section := <-sections
	arrSec := section.(ibus.IArraySection)
	_, ok := arrSec.Next() // came with section, no timeout, going to write next
	require.True(t, ok)

	cancel()
	ch <- struct{}{} //signal send more on cancelled context

	val, ok := arrSec.Next() // closed due of cancelled context
	require.False(t, ok)
	require.Nil(t, val)

	_, ok = <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)

	<-ch
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
	require.NotNil(t, sections)
	require.NotNil(t, secErr)

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

func TestSendElementNoSection(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.NotNil(t, rs.SendElement("", 42))
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
	require.NotNil(t, sections)

	_, ok := <-sections
	require.False(t, ok)
	require.Nil(t, *secErr)
}

func TestStopOnMiscSendFailed(t *testing.T) {
	godif.Provide(&ibus.RequestHandler, func(ctx context.Context, sender interface{}, request ibus.Request) {
		rs := ibus.SendParallelResponse2(ctx, sender)
		require.Error(t, nats.ErrConnectionClosed, rs.ObjectSection("objSec", []string{"class"}, 42))
		require.Error(t, nats.ErrConnectionClosed, rs.ObjectSection("objSec", []string{"class"}, 43))

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
	require.NotNil(t, sections)

	onBeforeMiscSend = func() {
		getTestServer(ctx).s.Shutdown()
		ctx.Value(nATSKey).(*Service).Stop(ctx)
	}

	section := <-sections
	objSec := section.(ibus.IObjectSection)
	require.Equal(t, "42", string(objSec.Value()))

	// will not receive anything more
	_, ok := <-sections
	require.False(t, ok)
	require.Error(t, nats.ErrConnectionClosed, *secErr)
}

func setUp() {
	Declare(DeclareTest(1))
	godif.Require(&ibus.SendParallelResponse2)
	godif.Require(&ibus.SendRequest2)
	godif.Require(&ibus.SendResponse)
	godif.Require(&ibus.RequestHandler)
	ctx, cancel = context.WithCancel(context.Background())
	var err error
	if ctx, err = services.ResolveAndStartCtx(ctx); err != nil {
		panic(err)
	}
	srv.Verbose = true
}

func tearDown() {
	services.StopAndReset(ctx)
	SetSectionConsumeAddonTimeout(ibus.DefaultTimeout)
	onReconnect = nil
	onBeforeContinuationReceive = nil
	onBeforeMiscSend = nil
}

func mapFromJSON(jsonBytes []byte) map[string]interface{} {
	res := map[string]interface{}{}
	if err := json.Unmarshal(jsonBytes, &res); err != nil {
		panic(err)
	}
	return res
}

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
	ctx    context.Context
	cancel context.CancelFunc
)
