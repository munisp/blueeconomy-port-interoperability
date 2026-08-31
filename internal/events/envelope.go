// Package events builds the platform FHIR-aligned event envelope used for the
// ports.booking.v1, ports.gate.v1 and ports.queue.v1 Kafka topics. Every
// envelope carries a FHIR R4 message Bundle entry and a provenance block
// binding the event to its principal, payload signature and ledger commit.
package events

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	EnvelopeVersion      = "1.0"
	Producer             = "s1-port-interoperability"
	ClassificationIntern = "INTERNAL"

	TopicBooking = "ports.booking.v1"
	TopicGate    = "ports.gate.v1"
	TopicQueue   = "ports.queue.v1"

	TopicDeclarations = "trade.declarations.v1"

	// TopicOffshore carries offshore terminal-call (SBM/SPM) lifecycle events.
	TopicOffshore = "ports.offshore.v1"
	// TopicManifests carries API/BRI passenger-manifest ingest outcomes.
	TopicManifests = "ports.manifests.v1"
	// TopicCruise carries cruise call lifecycle and excursion events.
	TopicCruise = "ports.cruise.v1"
	// TopicRevenueAssessments carries deterministic fee/dues assessments to
	// the financial-controls revenue contract (assessment → settlement →
	// reconciliation chain).
	TopicRevenueAssessments = "finance.revenue-assessments.v1"
	// TopicSecureChain carries Secure Chain (WP-7) verified-chain container
	// release lifecycle events.
	TopicSecureChain = "ports.securechain.v1"
)

// validTopic reports whether the topic is a platform v1 contract topic.
func validTopic(topic string) bool {
	switch topic {
	case TopicBooking, TopicGate, TopicQueue, TopicDeclarations,
		TopicOffshore, TopicManifests, TopicCruise, TopicRevenueAssessments,
		TopicSecureChain:
		return true
	default:
		return false
	}
}

// Provenance binds an event to the acting principal and the integrity chain.
// The signature key is the canonical `signature`, matching every other
// platform producer.
type Provenance struct {
	PrincipalID      string `json:"principalId"`
	PrincipalRole    string `json:"principalRole"`
	Signature        string `json:"signature"`
	LedgerCommitHash string `json:"ledgerCommitHash,omitempty"`
}

// Envelope is the platform event contract (envelopeVersion 1.0). The FHIR
// message bundle is emitted under the canonical `fhir` key, matching every
// other platform producer.
type Envelope struct {
	EnvelopeVersion string          `json:"envelopeVersion"`
	EventID         string          `json:"eventId"`
	EventType       string          `json:"eventType"`
	OccurredAt      time.Time       `json:"occurredAt"`
	Producer        string          `json:"producer"`
	CorrelationID   string          `json:"correlationId"`
	Classification  string          `json:"classification"`
	FHIR            json.RawMessage `json:"fhir"`
	Provenance      Provenance      `json:"provenance"`
}

// fhirBundle is a FHIR R4 Bundle of type "message" whose first entry is a
// Basic resource carrying the domain payload as a codeable extension.
type fhirBasic struct {
	ResourceType string          `json:"resourceType"`
	ID           string          `json:"id"`
	Code         fhirCodeable    `json:"code"`
	Extension    []fhirExtension `json:"extension,omitempty"`
}

type fhirCodeable struct {
	Text string `json:"text"`
}

type fhirExtension struct {
	URL          string `json:"url"`
	ValueString  string `json:"valueString,omitempty"`
	ValueInstant string `json:"valueInstant,omitempty"`
}

type fhirEntry struct {
	FullURL  string    `json:"fullUrl"`
	Resource fhirBasic `json:"resource"`
}

type fhirBundle struct {
	ResourceType string      `json:"resourceType"`
	Type         string      `json:"type"`
	Timestamp    time.Time   `json:"timestamp"`
	Entry        []fhirEntry `json:"entry"`
}

// Message builds a signed envelope for a domain event. subjectID identifies the
// aggregate (booking or gate scan), payloadJSON is the domain payload, and
// extensions carry flat scalar context (slot, terminal, amounts) into the FHIR
// entry. The provenance signature is a JWS compact serialization (EdDSA over
// the JCS-canonical envelope excluding the signature field) produced by the
// mandatory signer; a nil signer fails closed.
func Message(eventType, topic, correlationID, subjectID string, payloadJSON json.RawMessage, extensions map[string]string, principal Provenance, occurredAt time.Time, signer *Signer) (Envelope, error) {
	if signer == nil {
		return Envelope{}, errors.New("an envelope signer is required")
	}
	if strings.TrimSpace(eventType) == "" || !validTopic(topic) {
		return Envelope{}, errors.New("event type and a platform v1 topic are required")
	}
	if strings.TrimSpace(correlationID) == "" || strings.TrimSpace(subjectID) == "" {
		return Envelope{}, errors.New("correlation id and subject id are required")
	}
	if strings.TrimSpace(principal.PrincipalID) == "" || strings.TrimSpace(principal.PrincipalRole) == "" {
		return Envelope{}, errors.New("provenance principal id and role are required")
	}
	if len(payloadJSON) == 0 || !json.Valid(payloadJSON) {
		return Envelope{}, errors.New("payload must be valid JSON")
	}
	entryExtensions := []fhirExtension{{
		URL:         "https://blueeconomy.gov.ng/fhir/StructureDefinition/domain-payload",
		ValueString: string(payloadJSON),
	}}
	keys := make([]string, 0, len(extensions))
	for key := range extensions {
		keys = append(keys, key)
	}
	// Deterministic ordering keeps the bundle bytes stable for signature checks.
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return Envelope{}, errors.New("extension keys must be non-empty")
		}
		entryExtensions = append(entryExtensions, fhirExtension{
			URL:         "https://blueeconomy.gov.ng/fhir/StructureDefinition/" + key,
			ValueString: extensions[key],
		})
	}
	bundle := fhirBundle{
		ResourceType: "Bundle",
		Type:         "message",
		Timestamp:    occurredAt.UTC(),
		Entry: []fhirEntry{{
			FullURL: "urn:uuid:" + uuid.NewString(),
			Resource: fhirBasic{
				ResourceType: "Basic",
				ID:           subjectID,
				Code:         fhirCodeable{Text: eventType},
				Extension:    entryExtensions,
			},
		}},
	}
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode FHIR message bundle: %w", err)
	}
	envelope := Envelope{
		EnvelopeVersion: EnvelopeVersion,
		EventID:         uuid.NewString(),
		EventType:       eventType,
		OccurredAt:      occurredAt.UTC(),
		Producer:        Producer,
		CorrelationID:   correlationID,
		Classification:  ClassificationIntern,
		FHIR:            bundleJSON,
		Provenance:      principal,
	}
	signature, err := signer.Sign(envelope)
	if err != nil {
		return Envelope{}, fmt.Errorf("sign %s envelope: %w", eventType, err)
	}
	envelope.Provenance.Signature = signature
	return envelope, nil
}

// VerifySignature verifies the provenance JWS against the producer's public
// key: alg=EdDSA, a port-interoperability kid, an exact canonical-payload
// match and a valid Ed25519 signature.
func (envelope Envelope) VerifySignature(publicKey ed25519.PublicKey) bool {
	return Verify(envelope, publicKey) == nil
}
