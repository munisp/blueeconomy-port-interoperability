// Package ais implements the PCS AIS vessel-tracking ingestion boundary:
// env-gated feed client, fail-closed schema validation of incoming AIS
// position reports (malformed identities and non-finite or out-of-range
// geo data are rejected; physically implausible speed/course values are
// clamped to the AIS domain envelope), idempotent persistence and honest
// ingestion status. When AIS_FEED_URL is not configured the ingester
// reports configured:false and every data path fails closed — no
// synthetic positions are ever produced.
package ais

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
	"github.com/munisp/blueeconomy-port-interoperability/internal/mmsinumber"
)

const (
	// MaxSpeedKnots is the AIS speed-over-ground domain ceiling (102.2 kn
	// encodes "102.2 knots or higher" in the AIS standard).
	MaxSpeedKnots = 102.2
	// MaxFutureSkew bounds how far a report timestamp may lead the
	// ingestion clock before the message is rejected as malformed.
	MaxFutureSkew = 15 * time.Minute
	// MaxAge bounds how stale a report may be before it is rejected.
	MaxAge = 30 * 24 * time.Hour
)

// ErrInvalidReport marks a schema/integrity rejection of an AIS message.
var ErrInvalidReport = errors.New("invalid AIS position report")

// PositionReport is one validated AIS class-A/B position message.
type PositionReport struct {
	MMSI          string    `json:"mmsi"`
	IMO           string    `json:"imo,omitempty"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	SpeedKnots    float64   `json:"speed_knots"`
	CourseDegrees float64   `json:"course_degrees"`
	Heading       *int      `json:"heading,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
}

// Validate enforces the geo data-integrity contract fail closed. MMSI must
// be a structurally valid nine-digit ship-station identity (any flag —
// AIS receives international traffic, so the registry MID admission table
// deliberately does not apply). IMO, when present, must carry a valid
// check digit. Coordinates must be finite and inside the WGS-84 envelope:
// they are never clamped because moving a fabricated coordinate would
// invent a position. Speed and course are clamped to the AIS domain
// envelope, mirroring the geo data-integrity fix convention. Timestamps
// must be neither stale nor implausibly future. now is injected so the
// check is deterministic under test.
func (report *PositionReport) Validate(now time.Time) error {
	if !mmsinumber.Pattern.MatchString(report.MMSI) {
		return fmt.Errorf("%w: mmsi must be a nine-digit ship-station identity", ErrInvalidReport)
	}
	if report.IMO != "" && !imonumber.Valid(report.IMO) {
		return fmt.Errorf("%w: imo must be a seven-digit IMO number with a valid check digit", ErrInvalidReport)
	}
	if math.IsNaN(report.Latitude) || math.IsInf(report.Latitude, 0) ||
		report.Latitude < -90 || report.Latitude > 90 {
		return fmt.Errorf("%w: latitude must be finite and within [-90, 90]", ErrInvalidReport)
	}
	if math.IsNaN(report.Longitude) || math.IsInf(report.Longitude, 0) ||
		report.Longitude < -180 || report.Longitude > 180 {
		return fmt.Errorf("%w: longitude must be finite and within [-180, 180]", ErrInvalidReport)
	}
	if math.IsNaN(report.SpeedKnots) || math.IsInf(report.SpeedKnots, 0) {
		return fmt.Errorf("%w: speed_knots must be finite", ErrInvalidReport)
	}
	// Clamp absurd speed into the AIS envelope; a clamped value remains
	// truthfully bounded, unlike a fabricated coordinate.
	if report.SpeedKnots < 0 {
		report.SpeedKnots = 0
	}
	if report.SpeedKnots > MaxSpeedKnots {
		report.SpeedKnots = MaxSpeedKnots
	}
	if math.IsNaN(report.CourseDegrees) || math.IsInf(report.CourseDegrees, 0) {
		return fmt.Errorf("%w: course_degrees must be finite", ErrInvalidReport)
	}
	report.CourseDegrees = math.Mod(math.Mod(report.CourseDegrees, 360)+360, 360)
	if report.Heading != nil && (*report.Heading < 0 || *report.Heading > 359) {
		return fmt.Errorf("%w: heading must be within [0, 359]", ErrInvalidReport)
	}
	if report.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidReport)
	}
	if report.Timestamp.After(now.Add(MaxFutureSkew)) {
		return fmt.Errorf("%w: timestamp is implausibly far in the future", ErrInvalidReport)
	}
	if report.Timestamp.Before(now.Add(-MaxAge)) {
		return fmt.Errorf("%w: timestamp is stale", ErrInvalidReport)
	}
	return nil
}

// dedupeKey is the idempotent-ingestion identity of a report.
func (report PositionReport) dedupeKey() string {
	return report.MMSI + "@" + report.Timestamp.UTC().Format(time.RFC3339Nano)
}

// sanitizeMMSI trims surrounding whitespace some feeds emit around the
// identity field; identities with interior whitespace still fail Validate.
func sanitizeMMSI(value string) string {
	return strings.TrimSpace(value)
}
