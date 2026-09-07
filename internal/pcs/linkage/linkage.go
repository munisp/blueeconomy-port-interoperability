// Package linkage implements the PCS port-call correlation join: AIS
// vessel identity (MMSI/IMO, latest validated position) ↔ TOS berth
// assignment ↔ NSW declaration/port-call data (the port-call store, which
// already carries the NSW-ingested calls and their declaration
// references). Every leg is optional-but-honest: when AIS or TOS is
// unconfigured the corresponding leg is nil and the Sources list says
// exactly which systems contributed — degraded joins are reported, never
// fabricated.
package linkage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/munisp/blueeconomy-port-interoperability/internal/imonumber"
	"github.com/munisp/blueeconomy-port-interoperability/internal/mmsinumber"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/ais"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/tos"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
)

// ErrUnconfigured marks linkage queries that need a source which is not
// wired. Handlers map it to 503 SERVICE_UNAVAILABLE.
var ErrUnconfigured = errors.New("port-call linkage source is not configured")

// Source identifiers reported in LinkedPortCall.Sources.
const (
	SourceNSW = "nsw-portcall"
	SourceAIS = "ais"
	SourceTOS = "tos"
)

// AISPositions is the AIS read seam used by the join.
type AISPositions interface {
	Latest(ctx context.Context, mmsi string, limit int) ([]ais.PositionReport, error)
}

// TOSAssignments is the TOS read seam used by the join.
type TOSAssignments interface {
	BerthAssignment(ctx context.Context, imo string) (*tos.BerthAssignment, error)
}

// PortCallLookup is the NSW port-call read seam used by the join;
// *portcall.Store satisfies it.
type PortCallLookup interface {
	ListByIMO(ctx context.Context, imo string, limit int) ([]portcall.PortCall, error)
}

// LinkedPortCall is one NSW port call enriched with the AIS and TOS legs
// that were available and validated at query time.
type LinkedPortCall struct {
	PortCall      portcall.PortCall    `json:"port_call"`
	AISPosition   *ais.PositionReport  `json:"ais_position,omitempty"`
	TOSAssignment *tos.BerthAssignment `json:"tos_assignment,omitempty"`
	// Sources names exactly the systems that contributed to this join row.
	Sources []string `json:"sources"`
}

// Linker correlates the three PCS data planes. AIS and TOS seams may be
// nil (unconfigured); the NSW port-call lookup is mandatory because it is
// the join anchor.
type Linker struct {
	calls PortCallLookup
	ais   AISPositions
	tos   TOSAssignments
}

func NewLinker(calls PortCallLookup, aisPositions AISPositions, tosAssignments TOSAssignments) (*Linker, error) {
	if calls == nil {
		return nil, errors.New("linkage requires the NSW port-call lookup")
	}
	return &Linker{calls: calls, ais: aisPositions, tos: tosAssignments}, nil
}

// Configured reports, per source, whether that leg of the join is wired.
func (linker *Linker) Configured() map[string]bool {
	return map[string]bool{
		SourceNSW: linker.calls != nil,
		SourceAIS: linker.ais != nil,
		SourceTOS: linker.tos != nil,
	}
}

// LinkByIMO joins the newest NSW port calls for a vessel with its latest
// AIS position and current TOS berth assignment.
func (linker *Linker) LinkByIMO(ctx context.Context, imo string, limit int) ([]LinkedPortCall, error) {
	if linker.calls == nil {
		return nil, ErrUnconfigured
	}
	if !imonumber.Valid(imo) {
		return nil, fmt.Errorf("imo must be a seven-digit IMO number with a valid check digit")
	}
	calls, err := linker.calls.ListByIMO(ctx, imo, limit)
	if err != nil {
		return nil, err
	}
	linked := make([]LinkedPortCall, 0, len(calls))
	for _, call := range calls {
		row := LinkedPortCall{PortCall: call, Sources: []string{SourceNSW}}
		if position := linker.latestPosition(ctx, imo); position != nil {
			row.AISPosition = position
			row.Sources = append(row.Sources, SourceAIS)
		}
		if assignment := linker.assignment(ctx, imo); assignment != nil {
			row.TOSAssignment = assignment
			row.Sources = append(row.Sources, SourceTOS)
		}
		linked = append(linked, row)
	}
	return linked, nil
}

// latestPosition resolves the newest AIS position for an IMO by scanning
// the ingester's recent positions. An unavailable AIS leg degrades the
// join to the NSW/TOS legs; it never fabricates a position.
func (linker *Linker) latestPosition(ctx context.Context, imo string) *ais.PositionReport {
	if linker.ais == nil {
		return nil
	}
	reports, err := linker.ais.Latest(ctx, "", 500)
	if err != nil {
		return nil
	}
	for index := range reports {
		if reports[index].IMO == imo {
			return &reports[index]
		}
	}
	return nil
}

// assignment resolves the TOS berth plan for an IMO. An unconfigured TOS
// leg degrades honestly; a vessel unknown to the TOS simply has no
// assignment.
func (linker *Linker) assignment(ctx context.Context, imo string) *tos.BerthAssignment {
	if linker.tos == nil {
		return nil
	}
	assignment, err := linker.tos.BerthAssignment(ctx, imo)
	if err != nil {
		return nil
	}
	return assignment
}

// ResolveMMSI finds the IMO currently correlated with an MMSI from the
// latest validated AIS position, so callers holding only an MMSI can reach
// the IMO-anchored join. ok is false when the MMSI is malformed or no
// validated position carries an IMO mapping.
func (linker *Linker) ResolveMMSI(ctx context.Context, mmsi string) (imo string, ok bool) {
	if linker.ais == nil || !mmsinumber.Pattern.MatchString(strings.TrimSpace(mmsi)) {
		return "", false
	}
	reports, err := linker.ais.Latest(ctx, strings.TrimSpace(mmsi), 1)
	if err != nil || len(reports) == 0 || reports[0].IMO == "" {
		return "", false
	}
	return reports[0].IMO, true
}
