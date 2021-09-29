/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

//go:generate go get golang.org/x/tools/cmd/stringer
//go:generate stringer -type=busPacketType
// run: `go generate`

package ibusnats

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/valyala/bytebufferpool"
)

// used in tests
var onReconnect func() = nil

type senderImpl struct {
	partNumber int
	replyTo    string
}

type nATSSubscriber struct {
	conn         *nats.Conn
	subscription *nats.Subscription
}

// if called by router: client is disconnected -> ctx.Done()
// also called by narrator on saga narrate
func implSendRequest2(ctx context.Context,
	request ibus.Request, timeout time.Duration) (resp ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	reqData, _ := json.Marshal(request) // assumming ibus.Request can't be unmarshallable (no interfaces etc)
	if _, ok := srv.Queues[request.QueueID]; !ok {
		err = fmt.Errorf("unknown queue: %s", request.QueueID)
		return
	}
	qName := request.QueueID + strconv.Itoa(request.PartitionNumber)

	// Create Inbox
	replyTo := nats.NewInbox()

	// Subscribe to Inbox "replyTo"
	sub, err := srv.nATSPublisher.SubscribeSync(replyTo)
	if err != nil {
		err = fmt.Errorf("SubscribeSync failed: %w", err)
		return
	}

	// Publish request
	if srv.Verbose {
		log.Printf("sending request to NATS: %s->%s\n%s", qName, replyTo, hex.Dump(reqData))
	}
	if err = srv.nATSPublisher.PublishRequest(qName, replyTo, reqData); err != nil {
		err = fmt.Errorf("PublishRequest failed: %w", err)
		return
	}

	// Handle response
	{
		return handleNATSResponse(ctx, sub, qName, replyTo, timeout, bool(srv.Verbose))
	}
}

// panics on wrong sender
func implSendResponse(ctx context.Context, sender interface{}, response ibus.Response) {
	sImpl := sender.(senderImpl)
	srv.nATSSubscribers[sImpl.partNumber].sendSingleResponseToNATS(response, sImpl.replyTo)
}

// panics on wrong sender
func implSendParallelResponse2(ctx context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	sImpl := sender.(senderImpl)
	return &implIResultSenderCloseable{
		subjToReply: sImpl.replyTo,
		nc:          srv.nATSSubscribers[sImpl.partNumber].conn,
	}
}

func logStack(desc string, err error) {
	stackTrace := string(debug.Stack())
	if err == nil {
		log.Printf("%s\n%s\n", desc, stackTrace)
	} else {
		log.Printf("%s: %s\n%s\n", desc, err.Error(), stackTrace)
	}
}

func (ns *nATSSubscriber) sendSingleResponseToNATS(resp ibus.Response, subjToReply string) {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	b.WriteByte(byte(busPacketResponse))
	serializeResponse(b, resp)
	if err := ns.conn.Publish(subjToReply, b.B); err != nil {
		logStack("publish to NATS on NATSReply()", err)
	}
}

func (ns *nATSSubscriber) subscribe(handler nats.MsgHandler) (err error) {
	conn := ns.conn
	if ns.subscription, err = conn.QueueSubscribe(conn.Opts.Name, conn.Opts.Name, handler); err != nil {
		err = fmt.Errorf("conn.QueueSubscribe failed: %w", err)
		return
	}
	if err = conn.Flush(); err != nil {
		err = fmt.Errorf("conn.Flush failed: %w", err)
		return
	}
	if err = conn.LastError(); err != nil {
		err = fmt.Errorf("conn.LastError not nil: %w", err)
		return
	}
	log.Println("Subscribe for subj", conn.Opts.Name)
	return
}

func getNATSResponse(sub *nats.Subscription, timeout time.Duration) (msg *nats.Msg, err error) {
	msg, err = sub.NextMsg(timeout)
	if errors.Is(nats.ErrTimeout, err) {
		err = ibus.ErrTimeoutExpired
	}
	return
}

func handleNATSResponse(ctx context.Context, sub *nats.Subscription, partitionKey string, replyTo string,
	timeout time.Duration, verbose bool) (resp ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	firstMsg, err := getNATSResponse(sub, timeout)
	if err != nil {
		err = fmt.Errorf("first response read failed: %w", err)
		return
	}

	if verbose {
		log.Printf("%s %s first packet received %s:\n%s", partitionKey, replyTo, busPacketType(firstMsg.Data[0]),
			hex.Dump(firstMsg.Data))
	}

	// Check answer type
	// if kind of section -> there will nothing but sections or error
	// response -> there will be nothing more (no sections)
	if firstMsg.Data[0] == byte(busPacketResponse) {
		// Single Response
		// Deserialize Response
		resp = deserializeResponse(firstMsg.Data[1:])
		err = sub.Unsubscribe()
		return
	}

	// Process Sectioned Response
	secError = new(error)
	sectionsW := make(chan ibus.ISection)
	sections = sectionsW
	go getSectionsFromNATS(ctx, sectionsW, sub, secError, timeout, firstMsg, verbose)
	return
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Println(nc.Opts.Name, "disconnected")
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		if onReconnect != nil {
			// happens in tests
			onReconnect()
		}
		log.Println(nc.Opts.Name, "reconnected")
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Println(nc.Opts.Name, "closed")
	}))
	opts = append(opts, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
		log.Println(nc.Opts.Name, "error:", err.Error())
	}))

	return opts
}

func serializeResponse(b *bytebufferpool.ByteBuffer, resp ibus.Response) {
	b.WriteByte(byte(len(resp.ContentType)))
	b.WriteString(resp.ContentType)
	b.B = append(b.B, 0, 0)
	binary.LittleEndian.PutUint16(b.B[len(b.B)-2:len(b.B)], uint16(resp.StatusCode))
	if len(resp.Data) != 0 {
		b.Write(resp.Data)
	}
}

func deserializeResponse(data []byte) (resp ibus.Response) {
	length := data[0]
	pos := int(length + 1)
	if length != 0 {
		resp.ContentType = string(data[1:pos])
	}
	resp.StatusCode = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if len(data) == pos {
		resp.Data = nil
		return
	}
	resp.Data = data[pos:]
	return
}

func closeSection(sec *sectionData) {
	if sec != nil && sec.elems != nil {
		close(sec.elems)
	}
}
