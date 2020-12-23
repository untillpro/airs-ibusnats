/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

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

const (
	busPacketResponse byte = iota
	busPacketClose
	busPacketSectionMap
	busPacketSectionArray
	busPacketSectionObject
	busPacketSectionElement
)

// used in tests
var onReconnect func() = nil

func implSendRequest2(ctx context.Context,
	request ibus.Request, timeout time.Duration) (res ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	var reqData []byte
	reqData, _ = json.Marshal(request) // assumming ibus.Request can't be unmarshallable (no interfaces etc)
	srv := getService(ctx)
	if _, ok := srv.Queues[request.QueueID]; !ok {
		err = fmt.Errorf("unknown queue: %s", request.QueueID)
		return
	}
	qName := request.QueueID + strconv.Itoa(request.PartitionNumber)
	return sendToNATSAndGetResp(ctx, srv.nATSPublisher, reqData, qName, timeout, srv.Verbose)
}

type senderImpl struct {
	partNumber int
	replyTo    string
}

type nATSSubscriber struct {
	conn         *nats.Conn
	subscription *nats.Subscription
}

type sectionData struct {
	sectionType string
	path        []string
	sectionKind ibus.SectionKind
	elems       chan element
}

type sectionDataArray struct {
	*sectionData
}

type sectionDataMap struct {
	*sectionData
}

type sectionDataObject struct {
	*sectionData
	valueGot bool
}

type implIResultSenderCloseable struct {
	subjToReply        string
	nc                 *nats.Conn
	currentSection     byte
	currentSectionType string
	currentSectionPath []string
	sectionStartSent   bool
}

type element struct {
	name  string
	value []byte
}

func (s *sectionData) Type() string {
	return s.sectionType
}

func (s *sectionData) Path() []string {
	return s.path
}

func (s *sectionDataArray) Next() (value []byte, ok bool) {
	elem, ok := <-s.elems
	if !ok {
		return
	}
	return elem.value, true
}

func (s *sectionDataMap) Next() (name string, value []byte, ok bool) {
	elem, ok := <-s.elems
	if !ok {
		return
	}
	return elem.name, elem.value, true
}

func (s *sectionDataObject) Value() []byte {
	if s.valueGot {
		return nil
	}
	elem := <-s.elems // not possible that channel is closed unlike Map and Array sections
	s.valueGot = true
	return elem.value
}

func logStack(desc string, err error) {
	stackTrace := string(debug.Stack())
	if err == nil {
		log.Println(fmt.Sprintf("%s\n%s", desc, stackTrace))
	} else {
		log.Println(fmt.Sprintf("%s: %s\n%s", desc, err.Error(), stackTrace))
	}
}

func getSectionsFromNATS(ctx context.Context, sections chan ibus.ISection, sub *nats.Subscription, secErr *error, max time.Time,
	timeout time.Duration, firstMsg *nats.Msg, verbose bool) {
	var currentSection *sectionData
	defer func() {
		// assumming panic is impossible
		unsubErr := sub.Unsubscribe()
		if *secErr == nil {
			*secErr = unsubErr
		}
		closeSection(currentSection)
		close(sections)
	}()
	msg := firstMsg
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if firstMsg != nil {
			firstMsg = nil
		} else {
			msg, *secErr = sub.NextMsg(timeout)
			if *secErr != nil {
				toWrap := *secErr
				if errors.Is(nats.ErrTimeout, *secErr) {
					toWrap = ibus.ErrTimeoutExpired
				}
				*secErr = fmt.Errorf("response read failed: %w", toWrap)
				return
			}
			if verbose {
				log.Printf("%s packet received %s:\n%s", sub.Subject, busPacketTypeToString(msg.Data), hex.Dump(msg.Data))
			}
		}

		// msg.Data to ISection
		switch msg.Data[0] {
		case busPacketClose:
			if len(msg.Data) > 1 {
				*secErr = errors.New(string(msg.Data[1:]))
			}
			return
		case busPacketSectionMap, busPacketSectionArray, busPacketSectionObject:
			// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
			pos := 1
			lenPath := msg.Data[pos]
			var path []string
			pos++
			if lenPath == 0 {
				path = nil
			} else {
				path = make([]string, lenPath, lenPath)
				for i := 0; i < int(lenPath); i++ {
					lenP := int(msg.Data[pos])
					pos++
					path[i] = string(msg.Data[pos : pos+lenP])
					pos += lenP
				}
			}
			sectionTypeLen := int(msg.Data[pos])
			pos++
			sectionType := string(msg.Data[pos : pos+sectionTypeLen])
			elemName := ""
			pos += sectionTypeLen
			if msg.Data[0] == busPacketSectionMap {
				elemNameLen := int(msg.Data[pos])
				pos++
				elemName = string(msg.Data[pos : pos+elemNameLen])
				pos += elemNameLen
			}
			elemBytes := msg.Data[pos:]

			closeSection(currentSection)
			currentSection = &sectionData{
				sectionType: sectionType,
				path:        path,
				elems:       make(chan element),
			}
			switch msg.Data[0] {
			case busPacketSectionArray:
				currentSection.sectionKind = ibus.SectionKindArray
				sectionArray := &sectionDataArray{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionKindToString(sectionArray.sectionKind), sectionArray.sectionType, sectionArray.path)
				}
				sections <- sectionArray
			case busPacketSectionMap:
				currentSection.sectionKind = ibus.SectionKindMap
				sectionMap := &sectionDataMap{sectionData: currentSection}
				if verbose {
					fmt.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionKindToString(sectionMap.sectionKind), sectionMap.sectionType, sectionMap.path)
				}
				sections <- sectionMap
			case busPacketSectionObject:
				currentSection.sectionKind = ibus.SectionKindObject
				sectionObject := &sectionDataObject{sectionData: currentSection}
				if verbose {
					fmt.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionKindToString(sectionObject.sectionKind), sectionObject.sectionType, sectionObject.path)
				}
				sections <- sectionObject
			}
			currentSection.elems <- element{elemName, elemBytes}
		case busPacketSectionElement:
			elemName := ""
			pos := 1
			if currentSection.sectionKind == ibus.SectionKindMap {
				elemNameLen := int(msg.Data[pos])
				pos++
				elemName = string(msg.Data[pos : pos+elemNameLen])
				pos += elemNameLen
			}
			elemBytes := msg.Data[pos:]
			currentSection.elems <- element{elemName, elemBytes}
		}
	}
}

func (ns *nATSSubscriber) sendSingleResponseToNATS(resp ibus.Response, subjToReply string) {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	b.WriteByte(busPacketResponse)
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

func busPacketTypeToString(data []byte) string {
	switch data[0] {
	case busPacketResponse:
		return "RESP"
	case busPacketClose:
		return "CLOSE"
	case busPacketSectionArray:
		return "SEC_ARR"
	case busPacketSectionMap:
		return "SEC_MAP"
	case busPacketSectionElement:
		return "SEC_ELEM"
	case busPacketSectionObject:
		return "SEC_OBJ"
	default:
		return "<unknown>"
	}
}

func sectionKindToString(kind ibus.SectionKind) string {
	switch kind {
	case ibus.SectionKindArray:
		return "secArr"
	case ibus.SectionKindMap:
		return "secMap"
	case ibus.SectionKindObject:
		return "secObj"
	default:
		return "<unknown>"
	}
}

func sendToNATSAndGetResp(ctx context.Context, publisherConn *nats.Conn, data []byte, partitionKey string, timeout time.Duration, verbose bool) (
	resp ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	replyTo := nats.NewInbox()
	sub, err := publisherConn.SubscribeSync(replyTo)
	if err != nil {
		err = fmt.Errorf("SubscribeSync failed: %w", err)
		return resp, nil, nil, err
	}
	publisherConn.Flush()

	// Send the request
	if verbose {
		log.Printf("sending request to NATS: %s->%s\n%s", partitionKey, replyTo, hex.Dump(data))
	}
	if err = publisherConn.PublishRequest(partitionKey, replyTo, data); err != nil {
		err = fmt.Errorf("PublishRequest failed: %w", err)
		return
	}

	// Wait for a single response
	max := time.Now().Add(timeout)
	msg, err := sub.NextMsg(timeout)
	if err != nil {
		toWrap := err
		if errors.Is(nats.ErrTimeout, err) {
			toWrap = ibus.ErrTimeoutExpired
		}
		err = fmt.Errorf("response read failed: %w", toWrap)
		return
	}

	if verbose {
		fmt.Printf("%s %s first packet received %s:\n%s", partitionKey, replyTo, busPacketTypeToString(msg.Data), hex.Dump(msg.Data))
	}

	// check the last byte. It is the packet type
	// if kind of section -> there will nothing but sections or error
	// response -> there will be nothing more
	if msg.Data[0] == busPacketResponse {
		resp = deserializeResponse(msg.Data[1:])
		err = sub.Unsubscribe()
		return
	}
	var secErr error
	secError = &secErr
	sectionsW := make(chan ibus.ISection)
	sections = sectionsW
	go getSectionsFromNATS(ctx, sectionsW, sub, secError, max, timeout, msg, verbose)
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

// panics if wrong sender provided
func implSendResponse(ctx context.Context, sender interface{}, response ibus.Response) {
	sImpl := sender.(senderImpl)
	srv := getService(ctx)
	srv.nATSSubscribers[sImpl.partNumber].sendSingleResponseToNATS(response, sImpl.replyTo)
}

func (rs *implIResultSenderCloseable) Close(err error) {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	b.WriteByte(busPacketClose)
	if err != nil {
		b.WriteString(err.Error())
	}
	if errPub := rs.nc.Publish(rs.subjToReply, b.B); errPub != nil {
		logStack("failed to publish to NATS on IResultSenderCloseable.Close", errPub)
	}
}

func (rs *implIResultSenderCloseable) StartArraySection(sectionType string, path []string) {
	rs.currentSection = busPacketSectionArray
	rs.currentSectionType = sectionType
	rs.currentSectionPath = path
	rs.sectionStartSent = false
}

func (rs *implIResultSenderCloseable) StartMapSection(sectionType string, path []string) {
	rs.currentSection = busPacketSectionMap
	rs.currentSectionType = sectionType
	rs.currentSectionPath = path
	rs.sectionStartSent = false
}

func (rs *implIResultSenderCloseable) marshalElem(element interface{}) ([]byte, error) {
	if bytesRaw, ok := element.([]byte); ok {
		return bytesRaw, nil
	}
	return json.Marshal(element)
}

func (rs *implIResultSenderCloseable) ObjectSection(sectionType string, path []string, element interface{}) (err error) {
	if element == nil {
		// will not send nothingness
		return nil
	}
	rs.currentSection = busPacketSectionObject
	rs.currentSectionType = sectionType
	rs.currentSectionPath = path
	rs.sectionStartSent = false
	return rs.SendElement("", element)
}

// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
func (rs *implIResultSenderCloseable) SendElement(name string, element interface{}) (err error) {
	if element == nil {
		// will not send nothingness
		return nil
	}
	bytesElem, err := rs.marshalElem(element)
	if err != nil {
		return err
	}
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	if !rs.sectionStartSent {
		b.WriteByte(rs.currentSection)
		b.WriteByte(byte(len(rs.currentSectionPath)))
		for _, p := range rs.currentSectionPath {
			b.WriteByte(byte(len(p)))
			b.WriteString(p)
		}
		b.WriteByte(byte(len(rs.currentSectionType)))
		b.WriteString(rs.currentSectionType)

		rs.sectionStartSent = true
	} else {
		b.WriteByte(busPacketSectionElement)
	}
	if rs.currentSection == busPacketSectionMap {
		b.WriteByte(byte(len(name)))
		b.WriteString(name)
	}
	b.Write(bytesElem)
	return rs.nc.Publish(rs.subjToReply, b.B)
}

// panics on wrong sender
func implSendParallelResponse2(ctx context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	sImpl := sender.(senderImpl)
	srv := getService(ctx)
	return &implIResultSenderCloseable{
		subjToReply: sImpl.replyTo,
		nc:          srv.nATSSubscribers[sImpl.partNumber].conn,
	}
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
