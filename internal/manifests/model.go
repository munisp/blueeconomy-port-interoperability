// Package manifests ingests API/BRI passenger manifests (Advance Passenger
// Information / Batch Representation Interface, per the WCO/IATA/ICAO 2023
// Guidelines on API and BRI for Cruise Ship Travel) for cruise and ferry
// international calls. Manifests arrive as signed envelope v1.0 artifacts —
// a FHIR R4 message Bundle wrap with a JWS-EdDSA signature over the RFC 8785
// JCS-canonical envelope (the platform envelope scheme, internal/events) —
// are verified against the configured manifest authority key, validated per
// record, and every unaccepted record lands in the rejection queue with a
// machine-readable reason. Nothing is silently dropped.
package manifests

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
)

// ManifestKind classifies the passenger service.
type ManifestKind string

const (
	KindCruise ManifestKind = "CRUISE"
	KindFerry  ManifestKind = "FERRY"
)

// Status is the ingest outcome of a manifest.
type Status string

const (
	StatusAccepted               Status = "ACCEPTED"
	StatusAcceptedWithRejections Status = "ACCEPTED_WITH_REJECTIONS"
	StatusRejected               Status = "REJECTED"
)

// EventType is the envelope event type of a manifest submission.
const EventTypeSubmit = "manifest.submit"

// Reason codes for the rejection queue.
const (
	ReasonMalformedEnvelope     = "MALFORMED_ENVELOPE"
	ReasonInvalidSignature      = "INVALID_SIGNATURE"
	ReasonSubjectMismatch       = "SUBJECT_MISMATCH"
	ReasonInvalidPayload        = "INVALID_PAYLOAD"
	ReasonEmptyManifest         = "EMPTY_MANIFEST"
	ReasonRecordCountExceeded   = "RECORD_COUNT_EXCEEDS_LIMIT"
	ReasonInvalidRecordType     = "INVALID_RECORD_TYPE"
	ReasonMissingFamilyName     = "MISSING_FAMILY_NAME"
	ReasonMissingGivenName      = "MISSING_GIVEN_NAME"
	ReasonInvalidDateOfBirth    = "INVALID_DATE_OF_BIRTH"
	ReasonInvalidNationality    = "INVALID_NATIONALITY"
	ReasonInvalidDocumentNumber = "INVALID_DOCUMENT_NUMBER"
	ReasonInvalidSex            = "INVALID_SEX"
)

// MaxRecordsPerManifest bounds one BRI batch; larger batches are rejected
// whole (fail closed against resource exhaustion).
const MaxRecordsPerManifest = 20000

var (
	ErrSignatureVerification = errors.New("manifest envelope signature verification failed")
	ErrMalformedEnvelope     = errors.New("manifest artifact is not a valid envelope v1.0 manifest submission")
	ErrManifestConflict      = errors.New("manifest id conflicts with a retained manifest carrying a different payload")
	ErrNotFound              = errors.New("passenger manifest not found")
)

// Payload is the API/BRI manifest domain payload carried inside the signed
// envelope's FHIR bundle.
type Payload struct {
	ManifestReference string   `json:"manifest_reference"`
	VoyageReference   string   `json:"voyage_reference"`
	CallReference     string   `json:"call_reference"`
	ManifestKind      string   `json:"manifest_kind"`
	VesselIMO         string   `json:"vessel_imo"`
	Records           []Record `json:"records"`
}

// Record is one API/BRI passenger or crew line.
type Record struct {
	RecordType     string `json:"record_type"`
	FamilyName     string `json:"family_name"`
	GivenName      string `json:"given_name"`
	DateOfBirth    string `json:"date_of_birth"`
	Nationality    string `json:"nationality"`
	DocumentNumber string `json:"document_number"`
	Sex            string `json:"sex,omitempty"`
}

// Rejection is one rejection-queue entry.
type Rejection struct {
	ManifestID   string `json:"manifest_id,omitempty"`
	RecordIndex  *int   `json:"record_index,omitempty"`
	ReasonCode   string `json:"reason_code"`
	ReasonDetail string `json:"reason_detail"`
}

// Manifest is the recorded ingest outcome.
type Manifest struct {
	ManifestID        string       `json:"manifest_id"`
	ManifestReference string       `json:"manifest_reference"`
	VoyageReference   string       `json:"voyage_reference"`
	CallReference     string       `json:"call_reference"`
	Kind              ManifestKind `json:"manifest_kind"`
	VesselIMO         string       `json:"vessel_imo"`
	Status            Status       `json:"status"`
	RecordsTotal      int          `json:"records_total"`
	RecordsAccepted   int          `json:"records_accepted"`
	RecordsRejected   int          `json:"records_rejected"`
	PayloadSHA256     string       `json:"payload_sha256"`
	ReceivedBy        string       `json:"received_by"`
	ReceivedAt        time.Time    `json:"received_at"`
}

var (
	referencePattern   = regexp.MustCompile(`^[A-Za-z0-9._:/-]{2,128}$`)
	callRefPattern     = regexp.MustCompile(`^[A-Za-z0-9._:-]{2,64}$`)
	nationalityPattern = regexp.MustCompile(`^[A-Z]{2,3}$`)
	documentPattern    = regexp.MustCompile(`^[A-Z0-9-]{4,24}$`)
)

// validatePayload enforces the manifest header shape; errors are payload
// (not record) rejections.
func validatePayload(payload Payload) (string, bool) {
	switch {
	case !referencePattern.MatchString(payload.ManifestReference):
		return "manifest_reference must be 2-128 canonical characters", false
	case !referencePattern.MatchString(payload.VoyageReference):
		return "voyage_reference must be 2-128 canonical characters", false
	case !callRefPattern.MatchString(payload.CallReference):
		return "call_reference must be 2-64 canonical characters", false
	case payload.ManifestKind != string(KindCruise) && payload.ManifestKind != string(KindFerry):
		return "manifest_kind must be CRUISE or FERRY", false
	case !imonumber.Valid(payload.VesselIMO):
		return "vessel_imo must be a seven-digit IMO number with a valid check digit", false
	}
	return "", true
}

func canonicalName(value string) bool {
	if len(value) < 1 || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character == ' ', character == '-', character == '\'',
			character >= 0x80: // Unicode names (diacritics) are canonical.
		default:
			return false
		}
	}
	return true
}

// validateRecord checks one API/BRI line and returns the rejection reason
// code and detail when invalid.
func validateRecord(record Record, now time.Time) (string, string, bool) {
	if record.RecordType != "PAX" && record.RecordType != "CREW" {
		return ReasonInvalidRecordType, "record_type must be PAX or CREW", false
	}
	if !canonicalName(record.FamilyName) {
		return ReasonMissingFamilyName, "family_name must be 1-128 canonical name characters", false
	}
	if !canonicalName(record.GivenName) {
		return ReasonMissingGivenName, "given_name must be 1-128 canonical name characters", false
	}
	born, err := time.Parse("2006-01-02", record.DateOfBirth)
	if err != nil || !born.Before(now) || born.Before(now.AddDate(-120, 0, 0)) {
		return ReasonInvalidDateOfBirth, "date_of_birth must be a YYYY-MM-DD date in the past (max age 120)", false
	}
	if !nationalityPattern.MatchString(record.Nationality) {
		return ReasonInvalidNationality, "nationality must be an ISO 3166-1 alpha-2/alpha-3 uppercase code", false
	}
	if !documentPattern.MatchString(record.DocumentNumber) {
		return ReasonInvalidDocumentNumber, "document_number must be 4-24 uppercase alphanumeric/dash characters", false
	}
	if record.Sex != "" && record.Sex != "M" && record.Sex != "F" && record.Sex != "X" {
		return ReasonInvalidSex, "sex must be M, F or X when present", false
	}
	return "", "", true
}
