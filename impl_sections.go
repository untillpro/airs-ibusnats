/*
 * Copyright (c) 2020-present unTill Pro, Ltd.
 */

package ibusnats

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/valyala/bytebufferpool"
)

var (
	// ErrNoConsumer shows that consumer of further sections is gone. Further sections sending is senceless.
	ErrNoConsumer        = errors.New("no consumer for the stream")
	errCommunicationDone = errors.New("communication done")

	onBeforeMiscSend            func() = nil                        // used in tests
	onBeforeContinuationReceive func() = nil                        // used in tests
	onBeforeCloseSend           func() = nil                        // used in tests
	continuationTimeout                = int64(ibus.DefaultTimeout) // changes in tests
)

// used in tests (also in airs-router2 tests)
func SetContinuationTimeout(t time.Duration) {
	atomic.StoreInt64(&continuationTimeout, int64(t))
}

type busPacketType byte

const (
	busPacketResponse busPacketType = iota
	busPacketClose
	busPacketSectionMap
	busPacketSectionArray
	busPacketSectionObject
	busPacketSectionElement
	busPacketMiscInboxName
)

// ready-to-use byte arrays to easier publish
var (
	busMiscPacketGoOn       = []byte{0}
	busMiscPacketNoConsumer = []byte{1}
)

// if called by router: client is disconnected -> ctx.Done()
func getSectionsFromNATS(ctx context.Context, sections chan<- ibus.ISection, sub *nats.Subscription, secErr *error,
	timeout time.Duration, firstMsg *nats.Msg, verbose bool) {
	var currentSection *sectionData
	inbox := ""
	isStreamForsaken := false
	defer func() {
		// assumming panic is impossible
		unsubErr := sub.Unsubscribe()
		if *secErr == nil {
			*secErr = unsubErr
		}
		if isStreamForsaken {
			if err := srv.nATSPublisher.Publish(inbox, busMiscPacketNoConsumer); err != nil {
				log.Printf("failed to send `no consumer` to the handler: %v\n", err)
			} else if verbose {
				log.Println("`no consumer` sent")
			}
		}
		closeSection(currentSection)
		close(sections)
	}()

	// read from section packets from NATS
	// terminate on ctx.Done() or `Close` packet receive or on any error
	var msg *nats.Msg
	for {
		if firstMsg != nil {
			msg = firstMsg
			firstMsg = nil
		} else if msg, *secErr = getNATSResponse(sub, timeout); *secErr != nil {
			*secErr = fmt.Errorf("response read failed: %w", *secErr)
			isStreamForsaken = true
			return
		}

		select {
		case <-ctx.Done():
			log.Println("received one and further packets will be skipped because the context is closed")
			isStreamForsaken = true
			return
		default:
		}

		if verbose {
			log.Printf("%s packet received %s:\n%s", sub.Subject, busPacketType(msg.Data[0]), hex.Dump(msg.Data))
		}

		// msg.Data to ISection
		switch busPacketType(msg.Data[0]) {
		case busPacketMiscInboxName:
			inbox = string(msg.Data[1:])
		case busPacketClose:
			if len(msg.Data) > 1 {
				*secErr = errors.New(string(msg.Data[1:]))
			}
			return
		case busPacketSectionMap, busPacketSectionArray, busPacketSectionObject:
			// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
			pos := 1
			lenPath := msg.Data[pos]
			var path []string = nil
			pos++
			if lenPath > 0 {
				path = make([]string, lenPath)
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
			if msg.Data[0] == byte(busPacketSectionMap) {
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
			sectionToSend := ibus.ISection(nil)
			switch busPacketType(msg.Data[0]) {
			case busPacketSectionArray:
				currentSection.sectionKind = ibus.SectionKindArray
				sectionToSend = &sectionDataArray{sectionData: currentSection}
			case busPacketSectionMap:
				currentSection.sectionKind = ibus.SectionKindMap
				sectionToSend = &sectionDataMap{sectionData: currentSection}
			case busPacketSectionObject:
				currentSection.sectionKind = ibus.SectionKindObject
				sectionToSend = &sectionDataObject{sectionData: currentSection}
			}
			if verbose {
				log.Printf("%s section %s:`%s` %v\n", sub.Subject, currentSection.sectionKind, sectionType, path)
			}
			sections <- sectionToSend
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
		if busPacketType(msg.Data[0]) != busPacketMiscInboxName {
			if isStreamForsaken = sendMisc(busMiscPacketGoOn, "go on", inbox, verbose); isStreamForsaken {
				// do not send `go on` if inbox name is sent. Section + element will be received without `go on` awaiting
				return
			}
		}
	}
}

func sendMisc(packet []byte, packetDesc string, inbox string, verbose bool) (isStreamForsaken bool) {
	if onBeforeMiscSend != nil {
		// used in tests
		onBeforeMiscSend()
	}
	if err := srv.nATSPublisher.Publish(inbox, packet); err != nil {
		log.Printf("failed to send `%s` to the handler: %v\n", packetDesc, err)
		return true
	}
	if verbose {
		log.Printf("`%s` sent\n", packetDesc)
	}
	return false
}

// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
// will wait for continuation signal via misc NATS inbox:
// - `NoConsumer` packet is received -> `ibusnats.ErrNoConsumer` is returned
// - `SlowConsumer` packet is received -> `ibusnats.SlowConsumer` is returned
// - no messages during `(len(section)/(ibusnats.Service.AllowedSectionKBitsPerSec*1000/8) + ibus.DefaultTimeout)` seconds -> `ibus.ErrTimeoutExpired` is returned. Examples:
//   - AllowedSectionKBitsPerSec = 1000: section len 125000 bytes -> 11 seconds max, 250000 bytes -> 12 seconds max etc
//   - AllowedSectionKBitsPerSec =  100: section len 125000 bytes -> 20 seconds max, 250000 bytes -> 30 seconds max etc
func (rs *implIResultSenderCloseable) SendElement(name string, element interface{}) (err error) {
	if element == nil {
		// will not send nothingness
		return nil
	}
	if !rs.sectionStarted {
		return errors.New("section is not started")
	}
	if rs.lastError != nil {
		return rs.lastError
	}
	bytesElem, ok := element.([]byte)
	if !ok {
		if bytesElem, err = json.Marshal(element); err != nil {
			return
		}
	}
	defer func() { rs.lastError = err }()
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	if rs.sectionStartSent {
		b.WriteByte(byte(busPacketSectionElement))
	} else {
		if rs.miscSub == nil {
			inbox := nats.NewInbox()
			if rs.miscSub, err = rs.nc.SubscribeSync(inbox); err != nil {
				return
			}

			b.WriteByte(byte(busPacketMiscInboxName))
			b.WriteString(inbox)
			if err = rs.nc.Publish(rs.subjToReply, b.B); err != nil {
				return
			}
			b.Reset()
		}

		b.WriteByte(byte(rs.currentSection))
		b.WriteByte(byte(len(rs.currentSectionPath)))
		for _, p := range rs.currentSectionPath {
			b.WriteByte(byte(len(p)))
			b.WriteString(p)
		}
		b.WriteByte(byte(len(rs.currentSectionType)))
		b.WriteString(rs.currentSectionType)

		rs.sectionStartSent = true
	}
	if rs.currentSection == busPacketSectionMap {
		b.WriteByte(byte(len(name)))
		b.WriteString(name)
	}
	b.Write(bytesElem)
	if err = rs.nc.Publish(rs.subjToReply, b.B); err != nil {
		return
	}

	// wait for continuation
	if onBeforeContinuationReceive != nil {
		// used in tests
		onBeforeContinuationReceive()
	}
	miscMsg, err := getNATSResponse(rs.miscSub, time.Duration(atomic.LoadInt64(&continuationTimeout)))

	if err != nil {
		rs.lastError = fmt.Errorf("failed to receive continuation signal: %w", err)
		log.Println(rs.lastError)
		return rs.lastError
	}

	switch miscMsg.Data[0] {
	case busMiscPacketNoConsumer[0]:
		if srv.Verbose {
			log.Printf("`no consumer` received")
		}
		return ErrNoConsumer
	case busMiscPacketGoOn[0]:
		if srv.Verbose {
			log.Printf("`go on` received")
		}
	default:
		if srv.Verbose {
			log.Printf("unknown misc packet received:\n%s", hex.Dump(miscMsg.Data))
		}
		return errors.New("unknown misc packet received: " + hex.EncodeToString(miscMsg.Data))
	}

	return
}

type sectionData struct {
	sectionType string
	path        []string
	sectionKind ibus.SectionKind // need only for: map -> send element name
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
	currentSection     busPacketType
	currentSectionType string
	currentSectionPath []string
	sectionStartSent   bool
	sectionStarted     bool
	miscSub            *nats.Subscription
	lastError          error
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
	return elem.value, ok
}

func (s *sectionDataMap) Next() (name string, value []byte, ok bool) {
	elem, ok := <-s.elems
	return elem.name, elem.value, ok
}

func (s *sectionDataObject) Value() []byte {
	if s.valueGot {
		return nil
	}
	elem := <-s.elems // not possible that channel is closed unlike Map and Array sections
	s.valueGot = true
	return elem.value
}

func (rs *implIResultSenderCloseable) Close(err error) {
	if onBeforeCloseSend != nil {
		onBeforeCloseSend()
	}
	if rs.miscSub != nil {
		if errUnsub := rs.miscSub.Unsubscribe(); errUnsub != nil {
			logStack("failed to unsibscribe from misc inbox", errUnsub)
		}
	}
	if rs.lastError != nil {
		return
	}
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	b.WriteByte(byte(busPacketClose))
	if err != nil {
		b.WriteString(err.Error())
	}
	if errPub := rs.nc.Publish(rs.subjToReply, b.B); errPub != nil {
		logStack("failed to publish to NATS", errPub)
	}
	rs.lastError = errCommunicationDone
}

func (rs *implIResultSenderCloseable) StartArraySection(sectionType string, path []string) {
	rs.currentSection = busPacketSectionArray
	rs.currentSectionType = sectionType
	rs.currentSectionPath = path
	rs.sectionStartSent = false
	rs.sectionStarted = true
}

func (rs *implIResultSenderCloseable) StartMapSection(sectionType string, path []string) {
	rs.currentSection = busPacketSectionMap
	rs.currentSectionType = sectionType
	rs.currentSectionPath = path
	rs.sectionStartSent = false
	rs.sectionStarted = true
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
	rs.sectionStarted = true
	err = rs.SendElement("", element)
	rs.sectionStarted = false
	return err
}
