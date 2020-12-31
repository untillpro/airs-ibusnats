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
	"time"

	"github.com/nats-io/nats.go"
	ibus "github.com/untillpro/airs-ibus"
	"github.com/valyala/bytebufferpool"
)

type busPacketType byte

const (
	busPacketResponse busPacketType = iota
	busPacketClose
	busPacketSectionMap
	busPacketSectionArray
	busPacketSectionObject
	busPacketSectionElement
)

func getSectionsFromNATS(ctx context.Context, sections chan<- ibus.ISection, sub *nats.Subscription, secErr *error,
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

	// terminate on ctx.Done() or `Close` packet receive or on any error
	var msg *nats.Msg
	for {
		if firstMsg != nil {
			msg = firstMsg
			firstMsg = nil
		} else if msg, *secErr = getNATSResponse(sub, timeout); *secErr != nil {
			*secErr = fmt.Errorf("response read failed: %w", *secErr)
			return
		}
		select {
		case <-ctx.Done():
			log.Println("received one and further packets will be skipped because the context is closed")
			return
		default:
		}

		if verbose {
			log.Printf("%s packet received %s:\n%s", sub.Subject, busPacketType(msg.Data[0]), hex.Dump(msg.Data))
		}

		// msg.Data to ISection
		switch busPacketType(msg.Data[0]) {
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
				ctx:         ctx,
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
				sections <- sectionArray
			case busPacketSectionMap:
				currentSection.sectionKind = ibus.SectionKindMap
				sectionMap := &sectionDataMap{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionMap.sectionKind, sectionMap.sectionType, sectionMap.path)
				}
				sections <- sectionMap
			case busPacketSectionObject:
				currentSection.sectionKind = ibus.SectionKindObject
				sectionObject := &sectionDataObject{sectionData: currentSection}
				if verbose {
					log.Printf("%s section %s:`%s` %v\n", sub.Subject, sectionObject.sectionKind, sectionObject.sectionType, sectionObject.path)
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
	return rs.nc.Publish(rs.subjToReply, b.B)
}

type sectionData struct {
	ctx         context.Context
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
	currentSection     busPacketType
	currentSectionType string
	currentSectionPath []string
	sectionStartSent   bool
	sectionStarted     bool
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
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)
	b.WriteByte(byte(busPacketClose))
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
