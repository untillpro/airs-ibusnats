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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/go-nats"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/gochips"
	"strconv"
	"time"
)

const (
	firstByteInLastMsg = iota
	firstByteInRegularMsg
	firstByteInErrorMsg
)

type nATSPublisher struct {
	servers string
	conn    *nats.Conn
}

type nATSSubscriber struct {
	queueID          string
	numOfPartition   int
	partitionsNumber int
	worker           *nATSPublisher
	subscription     *nats.Subscription
}

func connectPublisher(servers string) (worker *nATSPublisher, err error) {
	return connectToNATS(servers, "NATSPublisher")
}

func connectSubscribers(s *Service) (subscribers []*nATSSubscriber, err error) {
	if s.CurrentQueueName == "" {
		return
	}
	subscribers = make([]*nATSSubscriber, 0)
	var numOfSubjects int
	var ok bool
	if numOfSubjects, ok = s.Queues[s.CurrentQueueName]; !ok {
		panic("can't find number of subjects in queues map")
	}
	minPart := 0
	maxPart := numOfSubjects
	if s.Parts != 0 && s.CurrentPart != 0 {
		if s.Parts > numOfSubjects {
			if s.CurrentPart >= numOfSubjects {
				minPart = 0
				maxPart = 0
			} else {
				minPart = s.CurrentPart - 1
				maxPart = s.CurrentPart
			}
		} else {
			partitionsInOnePart := numOfSubjects / s.Parts
			if s.Parts == s.CurrentPart {
				maxPart = numOfSubjects
			} else {
				maxPart = s.CurrentPart * partitionsInOnePart
			}
			minPart = partitionsInOnePart * (s.CurrentPart - 1)
		}
	}
	gochips.Info("Partition range:", minPart, "-", maxPart-1)
	for i := minPart; i < maxPart; i++ {
		worker, err := connectToNATS(s.NATSServers, s.CurrentQueueName+strconv.Itoa(i))
		if err != nil {
			return subscribers, err
		}
		subscribers = append(subscribers, &nATSSubscriber{s.CurrentQueueName, i, numOfSubjects, worker, nil})
	}
	return subscribers, nil
}

func unsubscribe(subscribers []*nATSSubscriber) {
	for _, s := range subscribers {
		err := s.subscription.Unsubscribe()
		if err != nil {
			gochips.Error(err)
		}
	}
}

func disconnectSubscribers(subscribers []*nATSSubscriber) {
	for _, s := range subscribers {
		s.worker.conn.Close()
	}
}

func connectToNATS(servers string, subjName string) (worker *nATSPublisher, err error) {
	opts := setupConnOptions([]nats.Option{nats.Name(subjName)})
	opts = setupConnOptions(opts)
	natsConn, err := nats.Connect(servers, opts...)
	if err != nil {
		return nil, err
	}
	worker = &nATSPublisher{servers, natsConn}
	return worker, nil
}

func (ns *nATSSubscriber) invokeNATSHandler(ctx context.Context,
	handler func(ctx context.Context, sender interface{}, request ibus.Request)) nats.MsgHandler {
	return func(msg *nats.Msg) {
		var req ibus.Request
		err := json.Unmarshal(msg.Data, &req)
		if err != nil {
			gochips.Error(err)
			return
		}
		handler(ctx, msg.Reply, req)
	}
}

func getChunksFromNATS(ctx context.Context, chunks chan []byte, sub *nats.Subscription, perr *error, max time.Time,
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

		if msg.Data[0] == firstByteInErrorMsg || msg.Data[0] == firstByteInLastMsg {
			EOM = true
			if msg.Data[0] == firstByteInErrorMsg {
				err := errors.New(string(msg.Data[1:]))
				*perr = err
				break
			}
		}
		msg.Data = msg.Data[1:]
		select {
		case <-ctx.Done():
			*perr = ctx.Err()
		default:
			chunks <- msg.Data
		}
	}
}

func (np *nATSPublisher) nATSReply(resp *ibus.Response, subjToReply string) {
	nc := np.conn
	data := serializeResponse(resp)
	data = prependByte(data, firstByteInLastMsg)
	err := nc.Publish(subjToReply, data)
	if err != nil {
		gochips.Error(err)
	}
}

func (np *nATSPublisher) chunkedNATSReply(resp *ibus.Response, chunks <-chan []byte, perr *error, subjToReply string) {
	nc := np.conn

	data := serializeResponse(resp)
	data = prependByte(data, firstByteInRegularMsg)

	err := nc.Publish(subjToReply, data)
	if err != nil {
		gochips.Error(err)
		*perr = err
		return
	}

	for chunk := range chunks {
		chunk = prependByte(chunk, firstByteInRegularMsg)
		err = nc.Publish(subjToReply, chunk)
		if err != nil {
			gochips.Error(err)
			*perr = err
			return
		}
	}

	if *perr != nil {
		np.publishError(*perr, subjToReply)
	}

	err = nc.Publish(subjToReply, []byte{firstByteInLastMsg})
	if err != nil {
		gochips.Error(err)
		return
	}
}

func (np *nATSPublisher) publishError(err error, subjToReply string) {
	nc := np.conn
	gochips.Error(err)
	data := []byte(err.Error())
	data = prependByte(data, firstByteInErrorMsg)
	err = nc.Publish(subjToReply, data)
	if err != nil {
		gochips.Error(err)
	}
}

func (ns *nATSSubscriber) subscribe(handler nats.MsgHandler) (err error) {
	conn := ns.worker.conn
	ns.subscription, err = conn.QueueSubscribe(conn.Opts.Name, conn.Opts.Name, handler)
	err = conn.Flush()
	if err = conn.LastError(); err != nil {
		return err
	}
	gochips.Info("Subscribe for subj", conn.Opts.Name)
	return nil
}

func (np *nATSPublisher) pubReqNATS(data []byte, partitionKey, replyTo string) error {
	conn := np.conn
	err := conn.PublishRequest(partitionKey, replyTo, data)
	if err != nil {
		if conn.LastError() != nil {
			err = conn.LastError()
		}
		return err
	}
	return nil
}

func (np *nATSPublisher) chunkedRespNATS(ctx context.Context, data []byte, partitionKey string, timeout time.Duration) (
	resp *ibus.Response, outChunks <-chan []byte, outChunksError *error, err error) {
	conn := np.conn
	replyTo := nats.NewInbox()
	sub, err := conn.SubscribeSync(replyTo)
	if err != nil {
		gochips.Error(err)
		return nil, nil, nil, err
	}
	conn.Flush()

	// Send the request
	err = conn.PublishRequest(partitionKey, replyTo, data)
	if err != nil {
		gochips.Error(err)
		return nil, nil, nil, err
	}

	chunks := make(chan []byte)
	// Wait for a single response
	max := time.Now().Add(timeout)
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		return nil, nil, nil, err
	}
	if msg == nil || len(msg.Data) == 0 {
		return nil, nil, nil, errors.New("empty message from NATS")
	}
	statusByteValue := msg.Data[0]
	var r ibus.Response
	err = deserializeResponse(msg.Data[1:], &r)
	if err != nil {
		return nil, nil, nil, err
	}
	if statusByteValue == firstByteInLastMsg || statusByteValue == firstByteInErrorMsg {
		return &r, nil, nil, nil
	} else if statusByteValue == firstByteInRegularMsg {
		outChunksError = &err
		go getChunksFromNATS(ctx, chunks, sub, outChunksError, max, ibus.DefaultTimeout)
		return &r, chunks, outChunksError, nil
	} else {
		return nil, nil, nil, fmt.Errorf("wrong msg from NATS, first byte is %d", statusByteValue)
	}
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

func implSendRequest(ctx context.Context,
	request *ibus.Request, timeout time.Duration) (res *ibus.Response, chunks <-chan []byte, chunksError *error, err error) {
	reqData, err := json.Marshal(request)
	if err != nil {
		gochips.Error(err)
		return nil, nil, nil, err
	}
	worker := getService(ctx)
	qName := request.QueueID + strconv.Itoa(request.PartitionNumber)
	return worker.nATSPublisher.chunkedRespNATS(ctx, reqData, qName, timeout)
}

func implSendResponse(ctx context.Context, sender interface{}, response ibus.Response, chunks <-chan []byte, chunksError *error) {
	var replyTo string
	var ok bool
	if replyTo, ok = sender.(string); !ok {
		gochips.Error("can't cast sender iface to string with subject to reply to")
		return
	}
	worker := getService(ctx)
	if chunks == nil {
		worker.nATSPublisher.nATSReply(&response, replyTo)
	} else {
		worker.nATSPublisher.chunkedNATSReply(&response, chunks, chunksError, replyTo)
	}
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
	stCodeBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(stCodeBytes, uint16(resp.StatusCode))
	buf.Write(stCodeBytes)
	dl := uint32(len(resp.Data))
	// separate uint32 on 4 bytes
	if resp.Data == nil {
		buf.Write([]byte{0})
		return buf.Bytes()
	}
	dlBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dlBytes, dl)
	buf.Write(dlBytes)
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
	if length != 0 {
		ct := data[1 : length+1]
		resp.ContentType = string(ct)
	}
	// status code last byte idx
	length = length + 2
	resp.StatusCode = int(binary.LittleEndian.Uint16(data[length-1 : length+1]))
	// data len idx
	length = length + 1
	if data[length] == 0 && len(data)-1 == int(length) {
		resp.Data = nil
		return nil
	}
	dStartIdx := length + 4
	dLenBuf := data[length:dStartIdx]
	resp.Data = data[dStartIdx : int(dStartIdx)+int(binary.LittleEndian.Uint32(dLenBuf))]
	return nil
}

func prependByte(chunk []byte, b byte) []byte {
	chunk = append(chunk, 0)
	copy(chunk[1:], chunk)
	chunk[0] = b
	return chunk
}
