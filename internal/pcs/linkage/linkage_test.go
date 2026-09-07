package linkage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/ais"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/tos"
	"github.com/munisp/blueeconomy-port-interoperability/internal/portcall"
)

type fakeCalls struct {
	calls []portcall.PortCall
	err   error
}

func (fake fakeCalls) ListByIMO(context.Context, string, int) ([]portcall.PortCall, error) {
	return fake.calls, fake.err
}

type fakeAIS struct {
	reports []ais.PositionReport
	err     error
}

func (fake fakeAIS) Latest(_ context.Context, mmsi string, _ int) ([]ais.PositionReport, error) {
	if mmsi == "" {
		return fake.reports, fake.err
	}
	var out []ais.PositionReport
	for _, report := range fake.reports {
		if report.MMSI == mmsi {
			out = append(out, report)
		}
	}
	return out, fake.err
}

type fakeTOS struct {
	assignment *tos.BerthAssignment
	err        error
}

func (fake fakeTOS) BerthAssignment(context.Context, string) (*tos.BerthAssignment, error) {
	return fake.assignment, fake.err
}

func testCall() portcall.PortCall {
	return portcall.PortCall{
		CreateRequest: portcall.CreateRequest{
			CallID: "call-1", VesselIMO: "9074729", PortCode: "APAPA",
			DeclarationRef: "NSW-DECL-1", SubmittedBy: "agent",
			AgencyProfileID: "profile", AgencyProfileVersion: "1",
		},
		Status: portcall.StatusSubmitted, CreatedAt: time.Now().UTC(),
	}
}

func TestFullJoinCorrelatesAllThreePlanes(t *testing.T) {
	linker, err := NewLinker(
		fakeCalls{calls: []portcall.PortCall{testCall()}},
		fakeAIS{reports: []ais.PositionReport{{MMSI: "657123456", IMO: "9074729", Latitude: 6.4, Longitude: 3.4, Timestamp: time.Now().UTC()}}},
		fakeTOS{assignment: &tos.BerthAssignment{BerthID: "AP-1", VesselIMO: "9074729"}},
	)
	if err != nil {
		t.Fatalf("new linker: %v", err)
	}
	linked, err := linker.LinkByIMO(context.Background(), "9074729", 10)
	if err != nil {
		t.Fatalf("link: %v", err)
	}
	if len(linked) != 1 {
		t.Fatalf("expected one row, got %d", len(linked))
	}
	row := linked[0]
	if row.AISPosition == nil || row.TOSAssignment == nil {
		t.Fatalf("join legs missing: %+v", row)
	}
	if len(row.Sources) != 3 {
		t.Fatalf("expected all three sources, got %v", row.Sources)
	}
	if row.PortCall.DeclarationRef != "NSW-DECL-1" {
		t.Fatalf("NSW declaration reference lost: %+v", row.PortCall)
	}
}

func TestDegradedJoinIsHonest(t *testing.T) {
	// AIS unconfigured, TOS has no record: only the NSW leg may appear.
	linker, err := NewLinker(
		fakeCalls{calls: []portcall.PortCall{testCall()}},
		nil,
		fakeTOS{err: tos.ErrNotFound},
	)
	if err != nil {
		t.Fatalf("new linker: %v", err)
	}
	linked, err := linker.LinkByIMO(context.Background(), "9074729", 10)
	if err != nil {
		t.Fatalf("link: %v", err)
	}
	row := linked[0]
	if row.AISPosition != nil || row.TOSAssignment != nil {
		t.Fatalf("degraded legs must stay nil, got %+v", row)
	}
	if len(row.Sources) != 1 || row.Sources[0] != SourceNSW {
		t.Fatalf("sources must admit only NSW, got %v", row.Sources)
	}
	configured := linker.Configured()
	if configured[SourceAIS] || !configured[SourceNSW] || !configured[SourceTOS] {
		t.Fatalf("dishonest configured map: %v", configured)
	}
}

func TestJoinRejectsBadIMO(t *testing.T) {
	linker, err := NewLinker(fakeCalls{}, nil, nil)
	if err != nil {
		t.Fatalf("new linker: %v", err)
	}
	if _, err := linker.LinkByIMO(context.Background(), "9074728", 10); err == nil {
		t.Fatal("invalid IMO check digit must be rejected")
	}
}

func TestJoinFailsClosedWithoutNSWAnchor(t *testing.T) {
	if _, err := NewLinker(nil, nil, nil); err == nil {
		t.Fatal("nil port-call lookup must fail closed")
	}
	linker, _ := NewLinker(fakeCalls{err: errors.New("db down")}, nil, nil)
	if _, err := linker.LinkByIMO(context.Background(), "9074729", 10); err == nil {
		t.Fatal("lookup failure must propagate, not fabricate an empty join")
	}
}

func TestResolveMMSI(t *testing.T) {
	linker, err := NewLinker(
		fakeCalls{},
		fakeAIS{reports: []ais.PositionReport{{MMSI: "657123456", IMO: "9074729", Timestamp: time.Now().UTC()}}},
		nil,
	)
	if err != nil {
		t.Fatalf("new linker: %v", err)
	}
	imo, ok := linker.ResolveMMSI(context.Background(), "657123456")
	if !ok || imo != "9074729" {
		t.Fatalf("MMSI resolution failed: %q %v", imo, ok)
	}
	if _, ok := linker.ResolveMMSI(context.Background(), "not-an-mmsi"); ok {
		t.Fatal("malformed MMSI must not resolve")
	}
	if _, ok := linker.ResolveMMSI(context.Background(), "627999999"); ok {
		t.Fatal("unknown MMSI must not resolve")
	}
	unconfigured, _ := NewLinker(fakeCalls{}, nil, nil)
	if _, ok := unconfigured.ResolveMMSI(context.Background(), "657123456"); ok {
		t.Fatal("AIS-unconfigured linker must not resolve MMSI")
	}
}
