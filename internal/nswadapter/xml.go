package nswadapter

import (
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"
)

// XML handoff schema "NSWPortCallEvent" version 1.0
// --------------------------------------------------
// Root:      <NSWPortCallEvent version="1.0">
//
//	EventID       string  — platform event id (UUID), unique per handoff
//	CallReference string  — aggregate reference the event belongs to
//	                        (port call id for port_call.* events, booking id
//	                        for booking/gate/queue events)
//	EventType     string  — outbox event type, e.g. port_call.clearance_decided
//	OccurredAt    string  — event timestamp, RFC 3339 UTC
//	TenantID      string  — platform tenant the event belongs to
//	PayloadSHA256 string  — "sha256:<lowercase hex>" of the raw JSON envelope
//	Payload       string  — the raw platform envelope (FHIR-aligned JSON),
//	                        carried as escaped character data
//
// The document is delivered as application/xml with the RS256 JWS in the
// X-NSW-Signature header; the JWS payload_sha256 claim digests the exact
// serialized document bytes.
const xmlSchemaVersion = "1.0"

// PortCallEvent is the XML serialization of one NSW handoff event.
type PortCallEvent struct {
	XMLName       xml.Name `xml:"NSWPortCallEvent"`
	Version       string   `xml:"version,attr"`
	EventID       string   `xml:"EventID"`
	CallReference string   `xml:"CallReference"`
	EventType     string   `xml:"EventType"`
	OccurredAt    string   `xml:"OccurredAt"`
	TenantID      string   `xml:"TenantID"`
	PayloadSHA256 string   `xml:"PayloadSHA256"`
	Payload       string   `xml:"Payload"`
}

// MarshalPortCallEvent serializes the handoff document with an XML
// declaration. All identity fields are mandatory — an incomplete document is
// never emitted.
func MarshalPortCallEvent(event PortCallEvent) ([]byte, error) {
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.CallReference) == "" ||
		strings.TrimSpace(event.EventType) == "" || strings.TrimSpace(event.TenantID) == "" {
		return nil, errors.New("XML handoff requires event id, call reference, event type and tenant id")
	}
	if _, err := time.Parse(time.RFC3339, event.OccurredAt); err != nil {
		return nil, fmt.Errorf("XML handoff occurred-at must be RFC 3339: %w", err)
	}
	if !strings.HasPrefix(event.PayloadSHA256, "sha256:") || event.Payload == "" {
		return nil, errors.New("XML handoff requires the payload and its SHA-256 digest")
	}
	event.Version = xmlSchemaVersion
	body, err := xml.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal XML handoff: %w", err)
	}
	return append([]byte(xml.Header), body...), nil
}
