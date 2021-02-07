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
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/valyala/bytebufferpool"
)

var (
	// ErrNoConsumer shows that consumer of further sections is gone. Further sections sending is senceless.
	ErrNoConsumer = errors.New("no consumer for the stream")
	// ErrSlowConsumer shows that section is processed too slow. Make better internet connection for http client or lower ibusnats.Service.AllowedSectionBytesPerSec
	ErrSlowConsumer = errors.New("section is processed too slow")

	onBeforeMiscSend            func() = nil // used in tests
	onBeforeContinuationReceive func() = nil // used in tests
	timerPool                          = sync.Pool{
		New: func() interface{} {
			return time.NewTimer(0)
		},
	}
	sectionConsumeAddonTimeout = int64(ibus.DefaultTimeout) // changes in tests
)

type busPacketType byte
type busPacketMiscType byte

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
	busMiscPacketGoOn         = []byte{0}
	busMiscPacketNoConsumer   = []byte{1}
	busMiscPacketSlowConsumer = []byte{2}
)

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

	sendMisc := func(packet []byte, packetDesc string) bool {
		if onBeforeMiscSend != nil {
			// used in tests
			onBeforeMiscSend()
		}
		if err := srv.nATSPublisher.Publish(inbox, packet); err != nil {
			log.Printf("failed to send `%s` to the handler: %v\n", packetDesc, err)
			isStreamForsaken = true
			return false
		}
		if verbose {
			log.Printf("`%s` sent\n", packetDesc)
		}
		return true
	}

	timer := timerPool.Get().(*time.Timer)
	defer timerPool.Put(timer)

	allowedBytesPerMillisecond := float32(srv.AllowedSectionKBitsPerSec) / 8

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

		sectionConsumeTimeout := false

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
			switch busPacketType(msg.Data[0]) {
			case busPacketSectionArray:
				currentSection.sectionKind = ibus.SectionKindArray
				sectionArray := &sectionDataArray{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionArray.sectionKind, sectionArray.sectionType, sectionArray.path)
				}
				allowedSectionMillis := float32(len(msg.Data)) / allowedBytesPerMillisecond
				sectionConsumeDuration := time.Duration(atomic.LoadInt64(&sectionConsumeAddonTimeout))
				resetTimer(timer, time.Duration(allowedSectionMillis)*time.Millisecond+sectionConsumeDuration)
				select {
				case sections <- sectionArray:
				case <-timer.C:
					sectionConsumeTimeout = true
				}
			case busPacketSectionMap:
				currentSection.sectionKind = ibus.SectionKindMap
				sectionMap := &sectionDataMap{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionMap.sectionKind, sectionMap.sectionType, sectionMap.path)
				}
				allowedSectionMillis := float32(len(msg.Data)) / allowedBytesPerMillisecond
				sectionConsumeDuration := time.Duration(atomic.LoadInt64(&sectionConsumeAddonTimeout))
				resetTimer(timer, time.Duration(allowedSectionMillis)*time.Millisecond+sectionConsumeDuration)
				select {
				case sections <- sectionMap:
				case <-timer.C:
					sectionConsumeTimeout = true
				}
			case busPacketSectionObject:
				currentSection.sectionKind = ibus.SectionKindObject
				sectionObject := &sectionDataObject{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionObject.sectionKind, sectionObject.sectionType, sectionObject.path)
				}
				allowedSectionMillis := float32(len(msg.Data)) / allowedBytesPerMillisecond
				sectionConsumeDuration := time.Duration(atomic.LoadInt64(&sectionConsumeAddonTimeout))
				resetTimer(timer, time.Duration(allowedSectionMillis)*time.Millisecond+sectionConsumeDuration)
				select {
				case sections <- sectionObject:
				case <-timer.C:
					sectionConsumeTimeout = true
				}
			}
			if !sectionConsumeTimeout {
				allowedElemMillis := float32(len(elemBytes)) / allowedBytesPerMillisecond
				sectionConsumeDuration := time.Duration(atomic.LoadInt64(&sectionConsumeAddonTimeout))
				resetTimer(timer, time.Duration(allowedElemMillis)*time.Millisecond+sectionConsumeDuration)
				select {
				case currentSection.elems <- element{elemName, elemBytes}:
				case <-timer.C:
					sectionConsumeTimeout = true
				}
			}

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
			allowedElemMillis := float32(len(elemBytes)) / allowedBytesPerMillisecond
			sectionConsumeDuration := time.Duration(atomic.LoadInt64(&sectionConsumeAddonTimeout))
			resetTimer(timer, time.Duration(allowedElemMillis)*time.Millisecond+sectionConsumeDuration)
			select {
			case currentSection.elems <- element{elemName, elemBytes}:
			case <-timer.C:
				sectionConsumeTimeout = true
			}
		}

		if sectionConsumeTimeout {
			if sendMisc(busMiscPacketSlowConsumer, "slow consumer") {
				*secErr = ErrSlowConsumer
				return
			}
		} else if busPacketType(msg.Data[0]) != busPacketMiscInboxName && !sendMisc(busMiscPacketGoOn, "go on") {
			// do not send `go on` if inbox name is sent. Section + element will be received without `go on` awaiting
			return
		}
	}
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	if !timer.Stop() && len(timer.C) != 0 {
		<-timer.C
	}
	timer.Reset(timeout)
}

// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
func (rs *implIResultSenderCloseable) SendElement(name string, element interface{}) (err error) {
	if element == nil {
		// will not send nothingness
		return nil
	}
	if !rs.sectionStarted {
		return errors.New("section is not started")
	}
	bytesElem, ok := element.([]byte)
	if !ok {
		if bytesElem, err = json.Marshal(element); err != nil {
			return
		}
	}
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	if !rs.sectionStartSent {
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
	} else {
		b.WriteByte(byte(busPacketSectionElement))
	}
	if rs.currentSection == busPacketSectionMap {
		b.WriteByte(byte(len(name)))
		b.WriteString(name)
	}
	b.Write(bytesElem)
	if err = rs.nc.Publish(rs.subjToReply, b.B); err != nil {
		return
	}

	// will detect slow consumer on requester side because it is harder to measure processing time on handler side: NATS time, network time etc.
	// now will wait for continuation
	// e.g. AllowedSectionKBitsPerSec = 1000: section len 125000 bytes -> 11 seconds max, 250000 bytes -> 12 seconds max etc
	//      AllowedSectionKBitsPerSec =  100: section len 125000 bytes -> 20 seconds max, 250000 bytes -> 30 seconds max etc
	processTimeout := int32(b.Len()) / (atomic.LoadInt32(&srv.AllowedSectionKBitsPerSec) * 1000 / 8)
	if onBeforeContinuationReceive != nil {
		onBeforeContinuationReceive()
	}
	miscMsg, err := getNATSResponse(rs.miscSub, time.Duration(processTimeout)+ibus.DefaultTimeout)
	if err != nil {
		log.Printf("failed to receive continuation signal: %v\n", err)
		return err
	}

	switch miscMsg.Data[0] {
	case busMiscPacketNoConsumer[0]:
		if srv.Verbose {
			log.Printf("`no consumer` received")
		}
		return ErrNoConsumer
	case busMiscPacketSlowConsumer[0]:
		if srv.Verbose {
			log.Printf("`slow consumer` received")
		}
		return ErrSlowConsumer
	default:
		if srv.Verbose {
			log.Printf("`go on` received")
		}
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
	if rs.miscSub != nil {
		if errUnsub := rs.miscSub.Unsubscribe(); errUnsub != nil {
			logStack("failed to unsibscribe from misc inbox", errUnsub)
		}
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
