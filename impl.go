/*
 * Copyright (c) 2019-present unTill Pro, Ltd. and Contributors
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package ibusnats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/go-nats"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/gochips"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	asyncName             = "async"
	syncName              = "sync"
	syncNumber            = 25
	firstByteInLastMsg    = 0
	firstByteInRegularMsg = 1
	firstByteInErrorMsg   = 2
)

var requestsWithCallback sync.Map

type natsPublisher struct {
	servers string
	conn    *nats.Conn
}

type natsSubscriber struct {
	queueID          string
	numOfPartition   int
	partitionsNumber int
	worker           *natsPublisher
	subscription     *nats.Subscription
}

func connectPublisher(servers string) (worker *natsPublisher, err error) {
	return connectToNats(servers, "NatsPublisher")
}

func connectSubscribers(s *Service) (subscribers []*natsSubscriber, err error) {
	subscribers = make([]*natsSubscriber, 0)
	worker, err := connectToNats(s.Servers, asyncName)
	if err != nil {
		return subscribers, err
	}
	subscribers = append(subscribers, &natsSubscriber{asyncName, 0, 0, worker, nil})
	for i := 0; i < syncNumber; i++ {
		minPart := 0
		maxPart := syncNumber
		if s.Parts != 0 && s.CurrentPart != 0 {
			if s.Parts > syncNumber {
				if s.CurrentPart >= syncNumber {
					minPart = 0
					maxPart = 0
				} else {
					minPart = s.CurrentPart - 1
					maxPart = s.CurrentPart
				}
			} else {
				partitionsInOnePart := syncNumber / s.Parts
				if s.Parts == s.CurrentPart {
					maxPart = syncNumber
				} else {
					maxPart = s.CurrentPart * partitionsInOnePart
				}
				minPart = partitionsInOnePart * (s.CurrentPart - 1)
			}
		}
		gochips.Info("Partition range:", minPart, "-", maxPart-1)
		for i := minPart; i < maxPart; i++ {
			queueID := syncName + strconv.Itoa(i)
			worker, err := connectToNats(s.Servers, queueID)
			if err != nil {
				return subscribers, err
			}
			subscribers = append(subscribers, &natsSubscriber{queueID, i, syncNumber, worker, nil})
		}
	}
	return subscribers, nil
}

func unsubscribe(subscribers []*natsSubscriber) {
	for _, s := range subscribers {
		err := s.subscription.Unsubscribe()
		if err != nil {
			gochips.Error(err)
		}
	}
}

func disconnectSubscribers(subscribers []*natsSubscriber) {
	for _, s := range subscribers {
		s.worker.conn.Close()
	}
}

func connectToNats(servers string, connectionName string) (worker *natsPublisher, err error) {
	opts := setupConnOptions([]nats.Option{nats.Name(connectionName)})
	opts = setupConnOptions(opts)
	natsConn, err := nats.Connect(servers, opts...)
	if err != nil {
		return nil, err
	}
	worker = &natsPublisher{servers, natsConn}
	return worker, nil
}

func (ns *natsSubscriber) invokeNatsNonPartyHandler(ctx context.Context, handler ibus.NonPartyHandlerChunked) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var req ibus.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			gochips.Error(err)
			return
		}
		if _, ok := req.Attachments[chunkedReqAttachment]; ok {
			ns.chunkedReqMsgHandler(ctx, msg, req, handler)
		} else {
			resp, outChunks, perr := handler(ctx, &req, nil)
			ns.chunkedNatsReply(resp, outChunks, perr, msg.Reply)
		}
	}
}

func (ns *natsSubscriber) chunkedReqMsgHandler(ctx context.Context, msg *nats.Msg, req ibus.Request,
	handler ibus.NonPartyHandlerChunked) {
	conn := ns.worker.conn
	replyTo := randStringRunes(8)
	err := conn.Publish(msg.Reply, []byte(replyTo))
	if err != nil {
		gochips.Error(err)
		return
	}

	sub, err := conn.SubscribeSync(replyTo)
	conn.Flush()

	inChunks := make(chan []byte)
	perr := &err

	max := time.Now().Add(ibus.DefaultTimeout)

	go getChunksFromNats(ctx, inChunks, sub, perr, max, ibus.DefaultTimeout)

	resp, _, _ := handler(ctx, &req, inChunks)
	if *perr != nil {
		ns.natsReply(&ibus.Response{
			Status:     http.StatusText(http.StatusInternalServerError),
			StatusCode: http.StatusInternalServerError,
			Data:       []byte((*perr).Error()),
		}, msg.Reply)
	} else {
		ns.natsReply(resp, msg.Reply)
	}
}

func getChunksFromNats(ctx context.Context, chunks chan []byte, sub *nats.Subscription, perr *error, max time.Time,
	timeout time.Duration) {
	defer func() {
		close(chunks)
		sub.Unsubscribe()
	}()
	var EOM bool
	for time.Now().Before(max) {
		if EOM {
			break
		}
		msg, err := sub.NextMsg(timeout)
		if err != nil {
			*perr = err
			break
		}

		// error can only receive here
		if msg.Data[0] == 2 {
			EOM = true
			msg.Data = msg.Data[1:]
			err := errors.New(string(msg.Data))
			*perr = err
			break
		} else if msg.Data[0] == 0 {
			EOM = true
		}
		select {
		case <-ctx.Done():
			*perr = ctx.Err()
		default:
			chunks <- msg.Data
		}
		if *perr != nil {
			break
		}
	}
}

func (ns *natsSubscriber) createNatsPartitionedHandler(ctx context.Context, handlerFactory ibus.PartitionHandlerFactory) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var req *ibus.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			ns.natsReply(ibus.CreateResponse(http.StatusInternalServerError, err.Error()), msg.Reply)
			return
		}
		var errResp *ibus.Response
		key := ns.queueID + strconv.Itoa(ns.numOfPartition)
		handler, ok := partitionHandlers.Load(key)
		if !ok || handler == nil {
			log.Println("Create handler for partition number", ns.numOfPartition)
			handler, errResp = handlerFactory(ctx, req.QueueID, req.PartitionNumber)
			handler, _ = partitionHandlers.LoadOrStore(key, handler)
		}
		if errResp != nil {
			ns.natsReply(errResp, msg.Reply)
			return
		}
		resp := handler.(ibus.PartitionHandler).Handle(ctx, req)
		ns.natsReply(resp, msg.Reply)
	}
}

func (ns *natsSubscriber) natsReply(resp *ibus.Response, subjToReply string) {
	nc := ns.worker.conn
	data, err := json.Marshal(resp)
	if err != nil {
		ns.publishError(err, subjToReply)
	}
	err = nc.Publish(subjToReply, data)
	if err != nil {
		gochips.Error(err)
	}
}

func (ns *natsSubscriber) chunkedNatsReply(resp *ibus.Response, chunks <-chan []byte, perr *error, subjToReply string) {
	nc := ns.worker.conn

	if perr != nil && *perr != nil {
		ns.publishError(*perr, subjToReply)
		return
	}

	data, err := json.Marshal(resp)
	if err != nil {
		ns.publishError(err, subjToReply)
		return
	}

	if perr == nil {
		perr = &err
	}

	err = nc.Publish(subjToReply, data)
	if err != nil {
		gochips.Error(err)
		*perr = err
		return
	}

	for chunk := range chunks {
		err = nc.Publish(subjToReply, chunk)
		if err != nil {
			gochips.Error(err)
			*perr = err
			return
		}
	}

	if *perr != nil {
		ns.publishError(*perr, subjToReply)
	}
}

func (ns *natsSubscriber) publishError(err error, subjToReply string) {
	nc := ns.worker.conn
	gochips.Error(err)
	resp := ibus.CreateResponse(http.StatusInternalServerError, err.Error())
	data, _ := json.Marshal(resp)
	d := make([]byte, 0, len(data)+1)
	// error
	d[0] = firstByteInErrorMsg
	d = append(d, data...)
	err = nc.Publish(subjToReply, d)
	if err != nil {
		gochips.Error(err)
	}
}

func (ns *natsSubscriber) subscribe(handler nats.MsgHandler) (err error) {
	conn := ns.worker.conn
	ns.subscription, err = conn.QueueSubscribe(conn.Opts.Name, conn.Opts.Name, handler)
	err = conn.Flush()
	if err = conn.LastError(); err != nil {
		return err
	}
	gochips.Info("Subscribe for subj", conn.Opts.Name)
	return nil
}

func prepareInvoke(request *ibus.Request) (reqData []byte, queueName string, err error) {
	reqData, err = json.Marshal(request)
	if err != nil {
		return nil, "", errors.New("can't marshal request body: " + err.Error())
	}
	queueName = substringBefore(request.QueueID, ":")
	if len(queueName) == 0 {
		return nil, "", errors.New("wrong QueueID")
	}
	return
}

func substringBefore(value string, a string) string {
	pos := strings.Index(value, a)
	if pos == -1 {
		return ""
	}
	return value[0:pos]
}

func (w *natsPublisher) pubReqNats(data []byte, partitionKey, replyTo string) error {
	conn := w.conn
	err := conn.PublishRequest(partitionKey, replyTo, data)
	if err != nil {
		if conn.LastError() != nil {
			err = conn.LastError()
		}
		return err
	}
	return nil
}

func (w *natsPublisher) chunkedRespNats(ctx context.Context, data []byte, partitionKey string, timeout time.Duration) (
	resp *ibus.Response, err error, outChunks <-chan []byte, outChunksError *error) {
	conn := w.conn
	replyTo := nats.NewInbox()
	sub, err := conn.SubscribeSync(replyTo)
	if err != nil {
		gochips.Error(err)
		return nil, err, nil, nil
	}
	conn.Flush()

	// Send the request
	err = conn.PublishRequest(partitionKey, replyTo, data)
	if err != nil {
		gochips.Error(err)
		return nil, err, nil, nil
	}

	chunks := make(chan []byte)
	// Wait for a single response
	max := time.Now().Add(timeout)
	msg, err := sub.NextMsg(timeout)
	if err != nil || msg == nil {
		return nil, err, nil, nil
	}

	var r ibus.Response
	err = deserializeResponse(msg.Data, &r)
	if err != nil {
		return nil, err, nil, nil
	}

	if r.StatusCode != http.StatusOK {
		return resp, nil, nil, nil
	}

	outChunksError = &err

	go getChunksFromNats(ctx, chunks, sub, outChunksError, max, ibus.DefaultTimeout)

	return resp, nil, chunks, outChunksError
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		gochips.Error(nc.Opts.Name + " disconnected")
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		gochips.Error(nc.Opts.Name + " reconnected")
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		gochips.Error(nc.Opts.Name + " closed")
	}))
	return opts
}

func sendRequest(ctx context.Context,
	request *ibus.Request, timeout time.Duration) (res *ibus.Response, err error, chunks <-chan []byte, chunksError *error) {
	reqData, qName, err := prepareInvoke(request)
	if err != nil {
		gochips.Error(err)
		return nil, err, nil, nil
	}
	worker := getService(ctx)
	return worker.natsPublisher.chunkedRespNats(ctx, reqData, qName, timeout)
	//conn := worker.natsPublisher.conn
	//uniqSubj := nats.NewInbox()
	//chunks = make(chan []byte)
	//chunksError = &err
	//go func() {
	//	sub, err := conn.SubscribeSync(uniqSubj)
	//	if err != nil {
	//		gochips.Error(err)
	//		chunksError = &err
	//		return
	//	}
	//	conn.Flush()
	//
	//	// Send initial request to get temporary channel for chunked resp
	//	max := time.Now().Add(timeout)
	//	for time.Now().Before(max) {
	//		msg, err := sub.NextMsg(timeout)
	//		if err != nil {
	//			gochips.Error(err)
	//			http.Error(respWriter, err.Error(), http.StatusInternalServerError)
	//		}
	//	}
	//	// TODO check code if == 206 => chunked, if other just send to RespWriter. Or not check exit code
	//}()
	//err = worker.natsPublisher.pubReqNats(reqData, qName, uniqSubj)
	//if err != nil {
	//	gochips.Error(err)
	//	http.Error(respWriter, err.Error(), http.StatusInternalServerError)
	//}
}

// TODO take conn from ctx, send to reply
func postResponse(ctx context.Context, sender interface{}, response ibus.Response, chunks <-chan []byte, chunksError *error) {
	var send *senderImpl
	var ok bool
	if send, ok = sender.(*senderImpl); !ok {
		// error
		return
	}
	if chunk == nil {
		send.ns.natsReply(response, send.replyTo)
	} else if response == nil {

	} else {
		// error
	}
}

type senderImpl struct {
	handler interface{}
	replyTo string
}

func requestHandler(ctx context.Context, sender interface{}, request *ibus.Request) {
	// TODO take reply subj and handler from sender interface, handle, return response
}

// little endian
func serializeResponse(resp *ibus.Response) []byte {
	if resp == nil {
		return nil
	}
	var buf bytes.Buffer
	buf.Write([]byte{byte(len([]byte(resp.ContentType)))})
	buf.Write([]byte(resp.ContentType))
	// separate uint16 on 2 bytes
	buf.Write([]byte{byte(resp.StatusCode), byte(resp.StatusCode >> 8)})
	dl := len(resp.Data)
	// separate uint32 on 4 bytes
	if resp.Data == nil {
		buf.Write([]byte{0})
		return buf.Bytes()
	}
	buf.Write([]byte{byte(dl), byte(dl >> 8), byte(dl >> 16), byte(dl >> 24)})
	buf.Write(resp.Data)
	return buf.Bytes()
}

// little endian
func deserializeResponse(data []byte, resp *ibus.Response) error {
	if len(data) == 0 {
		return errors.New("empty bytes")
	}
	if resp == nil {
		return errors.New("nil response")
	}
	length := data[0]
	ct := data[1:length]
	resp.ContentType = string(ct)
	// status code last byte idx
	length = length + 2
	resp.StatusCode = int(uint16(data[length-1]) | uint16(data[length]<<8))
	// data len idx
	length = length + 1
	if data[length] == 0 && len(data)-1 == int(length) {
		resp.Data = nil
		return nil
	}
	dStartIdx := length + 4
	dLenBuf := data[length:dStartIdx]
	dLen := int(uint32(dLenBuf[0]) | uint32(dLenBuf[1])<<8 | uint32(dLenBuf[2])<<16 | uint32(dLenBuf[3])<<24)
	resp.Data = data[dStartIdx : int(dStartIdx)+dLen]
	return nil
}
