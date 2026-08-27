package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMessageBuildsCompliantEnvelope(t *testing.T) {
	envelope, err := Message(
		"booking.paid", TopicBooking, "req-0001", "booking-0001",
		json.RawMessage(`{"booking_id":"booking-0001","status":"PAID"}`),
		map[string]string{"terminal-id": "APAPA-T1"},
		Provenance{PrincipalID: "trucker-1", PrincipalRole: "trucker", LedgerCommitHash: "sha256:abc"},
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	if envelope.EnvelopeVersion != "1.0" || envelope.Producer != Producer || envelope.Classification != ClassificationIntern {
		t.Fatalf("envelope metadata = %#v", envelope)
	}
	if envelope.EventID == "" || envelope.CorrelationID != "req-0001" || envelope.EventType != "booking.paid" {
		t.Fatalf("envelope identity = %#v", envelope)
	}
	var bundle struct {
		ResourceType string `json:"resourceType"`
		Type         string `json:"type"`
		Entry        []struct {
			Resource struct {
				ResourceType string `json:"resourceType"`
				ID           string `json:"id"`
			} `json:"resource"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(envelope.Bundle, &bundle); err != nil {
		t.Fatalf("decode FHIR bundle: %v", err)
	}
	if bundle.ResourceType != "Bundle" || bundle.Type != "message" || len(bundle.Entry) != 1 {
		t.Fatalf("bundle = %#v, want a FHIR message bundle with one entry", bundle)
	}
	if bundle.Entry[0].Resource.ResourceType != "Basic" || bundle.Entry[0].Resource.ID != "booking-0001" {
		t.Fatalf("bundle entry = %#v", bundle.Entry[0])
	}
	if !envelope.VerifySignature() {
		t.Fatal("envelope provenance signature must verify")
	}
}

func TestMessageSignatureDetectsTampering(t *testing.T) {
	envelope, err := Message(
		"gate.scan_approved", TopicGate, "req-0002", "scan-0001",
		json.RawMessage(`{"decision":"APPROVED"}`), nil,
		Provenance{PrincipalID: "gate-officer-1", PrincipalRole: "gate-officer"},
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelope.Bundle = json.RawMessage(`{"resourceType":"Bundle","type":"message","entry":[]}`)
	if envelope.VerifySignature() {
		t.Fatal("tampered bundle must fail signature verification")
	}
}

func TestMessageFailsClosedOnMissingFields(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		topic     string
		corrID    string
		subjectID string
		payload   json.RawMessage
		principal Provenance
	}{
		{"no event type", "", TopicBooking, "c", "s", json.RawMessage(`{}`), Provenance{PrincipalID: "p", PrincipalRole: "r"}},
		{"wrong topic", "e", "orders.v1", "c", "s", json.RawMessage(`{}`), Provenance{PrincipalID: "p", PrincipalRole: "r"}},
		{"no correlation", "e", TopicBooking, "", "s", json.RawMessage(`{}`), Provenance{PrincipalID: "p", PrincipalRole: "r"}},
		{"no subject", "e", TopicBooking, "c", "", json.RawMessage(`{}`), Provenance{PrincipalID: "p", PrincipalRole: "r"}},
		{"invalid payload", "e", TopicBooking, "c", "s", json.RawMessage(`{`), Provenance{PrincipalID: "p", PrincipalRole: "r"}},
		{"no principal", "e", TopicBooking, "c", "s", json.RawMessage(`{}`), Provenance{}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := Message(testCase.eventType, testCase.topic, testCase.corrID, testCase.subjectID, testCase.payload, nil, testCase.principal, time.Now()); err == nil {
				t.Fatal("envelope with missing fields must fail closed")
			}
		})
	}
}
