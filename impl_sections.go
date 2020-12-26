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

const (
	busPacketResponse byte = iota
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
	msg := firstMsg
	for {
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
		select {
		case <-ctx.Done():
			log.Println("received one and further packets will be skipped because the context is closed")
			return
		default:
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
				ctx:         ctx,
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

// sectionMark_1 | len(path)_1 | []( 1x len(path) | path ) |  1xlen(sectionType) | sectionType | 1x len(elemName) | elem name | elemBytes
func (rs *implIResultSenderCloseable) SendElement(name string, element interface{}) (err error) {
	if element == nil {
		// will not send nothingness
		return nil
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
	var elem element
	select {
	case _, ok = <-s.ctx.Done():
		for range s.elems {
		}
		fmt.Println("section", s.sectionType, ": elements are skipped because context is closed")
		return
	case elem, ok = <-s.elems:
		return elem.value, ok
	}
}

func (s *sectionDataMap) Next() (name string, value []byte, ok bool) {
	var elem element
	select {
	case _, ok = <-s.ctx.Done():
		for range s.elems {
		}
		fmt.Println("section", s.sectionType, ": elements are skipped because context is closed")
		return
	case elem, ok = <-s.elems:
		return elem.name, elem.value, ok
	}
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
