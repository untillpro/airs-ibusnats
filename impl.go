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
	"runtime/debug"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/untillpro/gochips"
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

// used in tests
var onReconnect func() = nil

type implIResultSenderCloseable struct {
	subjToReply        string
	nc                 *nats.Conn
	currentSection     byte
	currentSectionType string
	currentSectionPath []string
	sectionStartSent   bool
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

type element struct {
	name  string
	value []byte
}

func connectSubscribers(s *Service) (subscribers map[int]*nATSSubscriber, err error) {
	if len(s.CurrentQueueName) == 0 {
		return nil, errors.New("empty CurrentQueueName")
	}
	numOfSubjects, ok := s.Queues[s.CurrentQueueName]
	if !ok {
		return nil, errors.New("can't find number of subjects in queues map")
	}
	minPart := 0
	maxPart := numOfSubjects
	// these are zero -> publisher only, e.g. router
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
	subscribers = map[int]*nATSSubscriber{}
	for i := minPart; i < maxPart; i++ {
		conn, err := connectToNATS(s.NATSServers, s.CurrentQueueName+strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		subscribers[i] = &nATSSubscriber{conn, nil}
	}
	return
}

func notify(desc string, err error) {
	stackTrace := string(debug.Stack())
	if err == nil {
		gochips.Error(fmt.Errorf("%s\n%s", desc, stackTrace))
	} else {
		gochips.Error(fmt.Errorf("%s: %w\n%s", desc, err, stackTrace))
	}
}

func unsubscribe(subscribers map[int]*nATSSubscriber) {
	for _, s := range subscribers {
		if err := s.subscription.Unsubscribe(); err != nil {
			notify("unsubscribe failed", err)
		}
	}
}

func disconnectSubscribers(subscribers map[int]*nATSSubscriber) {
	for _, s := range subscribers {
		s.conn.Close()
	}
}

func connectToNATS(servers string, subjName string) (conn *nats.Conn, err error) {
	opts := setupConnOptions([]nats.Option{nats.Name(subjName)})
	opts = setupConnOptions(opts)
	conn, err = nats.Connect(servers, opts...)
	return
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
	for {
		var msg *nats.Msg
		if firstMsg != nil {
			msg = firstMsg
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
				fmt.Printf("%s packet recevied %s:\n%s", sub.Subject, busPacketTypeToString(msg.Data), hex.Dump(msg.Data))
			}
		}

		// msg.Data to ISection
		switch msg.Data[len(msg.Data)-1] {
		case busPacketClose:
			if len(msg.Data) > 1 {
				*secErr = errors.New(string(msg.Data[:len(msg.Data)-1]))
			}
			return
		case busPacketSectionMap, busPacketSectionArray, busPacketSectionObject:
			/*
							| for non-array sections only  |
				elemBytes | elem name | 1x len(elemName) | sectionType | 1xlen(sectionType) | [](path | 1x len(path)) | 1x len(path) | sectionMark |
			*/

			// read len(path)
			pos := byte(len(msg.Data) - 2) // points to len(path)
			lenPath := msg.Data[pos]
			var path []string
			pos--
			if lenPath == 0 {
				path = nil
			} else {
				path = make([]string, lenPath, lenPath)
				for i := int(lenPath) - 1; i >= 0; i-- {
					pathElemLen := msg.Data[pos]
					path[i] = string(msg.Data[pos-pathElemLen : pos])
					pos -= pathElemLen + 1
				}
			}

			sectionTypeLen := msg.Data[pos]
			sectionType := string(msg.Data[pos-sectionTypeLen : pos])
			pos -= sectionTypeLen
			elemName := ""
			if msg.Data[len(msg.Data)-1] != busPacketSectionArray && msg.Data[len(msg.Data)-1] != busPacketSectionObject {
				pos--
				elemNameLen := msg.Data[pos]
				elemName = string(msg.Data[pos-elemNameLen : pos])
				pos -= elemNameLen
			}
			elemBytes := msg.Data[:pos]
			closeSection(currentSection)
			currentSection = &sectionData{
				sectionType: sectionType,
				path:        path,
				elems:       make(chan element),
			}
			switch msg.Data[len(msg.Data)-1] {
			case busPacketSectionArray:
				currentSection.sectionKind = ibus.SectionKindArray
				sectionArray := &sectionDataArray{sectionData: currentSection}
				if verbose {
					fmt.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionKindToString(sectionArray.sectionKind), sectionArray.sectionType, sectionArray.path)
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
			pos := byte(len(msg.Data) - 1)
			if currentSection.sectionKind != ibus.SectionKindArray {
				pos--
				elemNameLen := msg.Data[pos]
				elemName = string(msg.Data[pos-elemNameLen : pos])
				pos -= elemNameLen
			}
			elemBytes := msg.Data[:pos]
			currentSection.elems <- element{elemName, elemBytes}
		}

		if !time.Now().Before(max) {
			break
		}
	}
	*secErr = fmt.Errorf("sectioned communication is terminated: %w", ibus.ErrTimeoutExpired)
}

func (ns *nATSSubscriber) sendSingleResponseToNATS(resp ibus.Response, subjToReply string) {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	serializeResponse(b, resp)
	b.WriteByte(busPacketResponse)
	if err := ns.conn.Publish(subjToReply, b.B); err != nil {
		notify("publish to NATS on NATSReply()", err)
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
	gochips.Info("Subscribe for subj", conn.Opts.Name)
	return
}

func busPacketTypeToString(data []byte) string {
	switch data[len(data)-1] {
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
		fmt.Printf("sending request to NATS: %s->%s\n%s", partitionKey, replyTo, hex.Dump(data))
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
	if msg.Data[len(msg.Data)-1] == busPacketResponse {
		resp = deserializeResponse(msg.Data[:len(msg.Data)-1])
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
		gochips.Error(nc.Opts.Name + " disconnected")
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		if onReconnect != nil {
			// happens in tests
			onReconnect()
		}
		gochips.Error(nc.Opts.Name + " reconnected")
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		gochips.Error(nc.Opts.Name + " closed")
	}))
	opts = append(opts, nats.ErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
		gochips.Error(nc.Opts.Name + " error: " + err.Error())
	}))

	return opts
}

func implSendRequest2(ctx context.Context,
	request ibus.Request, timeout time.Duration) (res ibus.Response, sections <-chan ibus.ISection, secError *error, err error) {
	var reqData []byte
	reqData, _ = json.Marshal(request) // assumming ibus.Request can't be unmarshallable (no interfaces etc)
	srv := getService(ctx)
	qName := request.QueueID + strconv.Itoa(request.PartitionNumber)
	return sendToNATSAndGetResp(ctx, srv.nATSPublisher, reqData, qName, timeout, srv.Verbose)
}

func implSendResponse(ctx context.Context, sender interface{}, response ibus.Response) {
	sImpl, ok := sender.(senderImpl)
	if !ok {
		notify("wrong sender provided", nil)
		return
	}
	srv := getService(ctx)
	srv.nATSSubscribers[sImpl.partNumber].sendSingleResponseToNATS(response, sImpl.replyTo)
}

func (rs *implIResultSenderCloseable) Close(err error) {
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	if err != nil {
		b.WriteString(err.Error())
	}
	b.WriteByte(busPacketClose)
	if errPub := rs.nc.Publish(rs.subjToReply, b.B); errPub != nil {
		notify("failed to publish to NATS on IResultSenderCloseable.Close", errPub)
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

/*
              | for non-array sections only |
	elemBytes | elem name | 1x len(elemName) | sectionType | 1xlen(sectionType) | [](path | 1x len(path)) | 1x len(path) | sectionMark |

*/

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
	b.Write(bytesElem)
	if rs.currentSection != busPacketSectionArray && rs.currentSection != busPacketSectionObject {
		b.WriteString(name)
		b.WriteByte(byte(len(name)))
	}
	if !rs.sectionStartSent {
		// write sectionTypeName
		b.WriteString(rs.currentSectionType)
		b.WriteByte(byte(len(rs.currentSectionType)))
		// write path elements
		for _, p := range rs.currentSectionPath {
			b.WriteString(p)
			b.WriteByte(byte(len(p)))
		}
		// write path elements amount
		b.WriteByte(byte(len(rs.currentSectionPath)))
		// write type of the section (1byte)
		b.WriteByte(rs.currentSection)
		rs.sectionStartSent = true
	} else {
		b.WriteByte(busPacketSectionElement)
	}
	return rs.nc.Publish(rs.subjToReply, b.B)
}

func implSendParallelResponse2(ctx context.Context, sender interface{}) (rsender ibus.IResultSenderClosable) {
	sImpl, ok := sender.(senderImpl)
	if !ok {
		gochips.Error("can't get part number and reply to from sender iface")
		return
	}
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
