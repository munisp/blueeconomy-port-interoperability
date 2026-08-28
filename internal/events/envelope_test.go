package events

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

// testSigner builds a throwaway signer for envelope unit tests.
func testSigner(t *testing.T) *Signer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := NewSigner(key, "1")
	if err != nil {
		t.Fatalf("build test signer: %v", err)
	}
	return signer
}

func TestMessageBuildsCompliantEnvelope(t *testing.T) {
	signer := testSigner(t)
	envelope, err := Message(
		"booking.paid", TopicBooking, "req-0001", "booking-0001",
		json.RawMessage(`{"booking_id":"booking-0001","status":"PAID"}`),
		map[string]string{"terminal-id": "APAPA-T1"},
		Provenance{PrincipalID: "trucker-1", PrincipalRole: "trucker", LedgerCommitHash: "sha256:abc"},
		time.Now().UTC(), signer,
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
	if err := json.Unmarshal(envelope.FHIR, &bundle); err != nil {
		t.Fatalf("decode FHIR bundle: %v", err)
	}
	if bundle.ResourceType != "Bundle" || bundle.Type != "message" || len(bundle.Entry) != 1 {
		t.Fatalf("bundle = %#v, want a FHIR message bundle with one entry", bundle)
	}
	if bundle.Entry[0].Resource.ResourceType != "Basic" || bundle.Entry[0].Resource.ID != "booking-0001" {
		t.Fatalf("bundle entry = %#v", bundle.Entry[0])
	}
	if !envelope.VerifySignature(signer.PublicKey()) {
		t.Fatal("envelope provenance signature must verify")
	}
	serialized, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(serialized, &keys); err != nil {
		t.Fatalf("decode serialized envelope: %v", err)
	}
	if _, ok := keys["fhir"]; !ok {
		t.Fatalf("serialized envelope must carry the canonical fhir key, got keys %v", keys)
	}
	if _, ok := keys["bundle"]; ok {
		t.Fatal("serialized envelope must not use the deferred v2 bundle key")
	}
}

func TestMessageBuildsQueueEnvelope(t *testing.T) {
	signer := testSigner(t)
	envelope, err := Message(
		"queue.called_up", TopicQueue, "queue-req-0001", "queue-request-0001",
		json.RawMessage(`{"queue_request_id":"queue-request-0001","status":"CALLED_UP"}`),
		map[string]string{"terminal-id": "APAPA-T1", "priority-class": "PERISHABLE"},
		Provenance{PrincipalID: "callup-engine", PrincipalRole: "callup-engine"},
		time.Now().UTC(), signer,
	)
	if err != nil {
		t.Fatalf("build queue envelope: %v", err)
	}
	if envelope.Classification != ClassificationIntern || envelope.EventType != "queue.called_up" {
		t.Fatalf("queue envelope metadata = %#v", envelope)
	}
	if envelope.Provenance.PrincipalID != "callup-engine" || envelope.Provenance.PrincipalRole != "callup-engine" {
		t.Fatalf("queue envelope provenance = %#v", envelope.Provenance)
	}
	if !envelope.VerifySignature(signer.PublicKey()) {
		t.Fatal("queue envelope provenance signature must verify")
	}
}

func TestMessageSignatureDetectsTampering(t *testing.T) {
	signer := testSigner(t)
	envelope, err := Message(
		"gate.scan_approved", TopicGate, "req-0002", "scan-0001",
		json.RawMessage(`{"decision":"APPROVED"}`), nil,
		Provenance{PrincipalID: "gate-officer-1", PrincipalRole: "gate-officer"},
		time.Now().UTC(), signer,
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelope.FHIR = json.RawMessage(`{"resourceType":"Bundle","type":"message","entry":[]}`)
	if envelope.VerifySignature(signer.PublicKey()) {
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
			if _, err := Message(testCase.eventType, testCase.topic, testCase.corrID, testCase.subjectID, testCase.payload, nil, testCase.principal, time.Now(), testSigner(t)); err == nil {
				t.Fatal("envelope with missing fields must fail closed")
			}
		})
	}
}
