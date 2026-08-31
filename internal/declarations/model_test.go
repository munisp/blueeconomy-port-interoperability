package declarations

import "testing"

func TestValidTransitionHappyPath(t *testing.T) {
	path := []Status{StatusDraft, StatusSubmitted, StatusRiskAssessed, StatusGreenLane, StatusCleared}
	for i := 0; i+1 < len(path); i++ {
		if !ValidTransition(path[i], path[i+1]) {
			t.Fatalf("%s -> %s must be permitted", path[i], path[i+1])
		}
	}
	for _, lane := range []Status{StatusGreenLane, StatusYellowLane, StatusRedLane} {
		if !ValidTransition(StatusRiskAssessed, lane) {
			t.Fatalf("RISK_ASSESSED -> %s must be permitted", lane)
		}
		for _, terminal := range []Status{StatusCleared, StatusRejected} {
			if !ValidTransition(lane, terminal) {
				t.Fatalf("%s -> %s must be permitted", lane, terminal)
			}
		}
	}
	if !ValidTransition(StatusSubmitted, StatusScoringUnavailable) {
		t.Fatal("SUBMITTED -> SCORING_UNAVAILABLE must be permitted (fail-closed scorer outage)")
	}
}

func TestValidTransitionRejectsIllegalPairs(t *testing.T) {
	all := []Status{
		StatusDraft, StatusSubmitted, StatusRiskAssessed, StatusGreenLane, StatusYellowLane,
		StatusRedLane, StatusCleared, StatusRejected, StatusScoringUnavailable, StatusSuperseded,
	}
	illegal := [][2]Status{
		{StatusDraft, StatusRiskAssessed},           // must submit first
		{StatusDraft, StatusGreenLane},              // never auto-laned from draft
		{StatusDraft, StatusCleared},                // no clearance without assessment
		{StatusSubmitted, StatusGreenLane},          // lanes come from risk assessment
		{StatusSubmitted, StatusCleared},            // no direct clearance
		{StatusRiskAssessed, StatusCleared},         // must pass through a lane
		{StatusRiskAssessed, StatusRejected},        // lane decision first
		{StatusScoringUnavailable, StatusGreenLane}, // blocked, never auto-laned
		{StatusScoringUnavailable, StatusSubmitted}, // terminal: amendment only
		{StatusScoringUnavailable, StatusCleared},
		{StatusCleared, StatusDraft},
		{StatusCleared, StatusSubmitted},
		{StatusCleared, StatusSuperseded}, // cleared declarations are immutable
		{StatusSuperseded, StatusDraft},
		{StatusRejected, StatusGreenLane},
		{StatusGreenLane, StatusDraft},
	}
	for _, pair := range illegal {
		if ValidTransition(pair[0], pair[1]) {
			t.Fatalf("%s -> %s must be prohibited", pair[0], pair[1])
		}
	}
	// Every state except the three terminals must have at least one exit.
	for _, status := range all {
		if status == StatusCleared || status == StatusSuperseded {
			continue
		}
		exits := 0
		for _, next := range all {
			if ValidTransition(status, next) {
				exits++
			}
		}
		if exits == 0 {
			t.Fatalf("%s is unexpectedly terminal", status)
		}
	}
}

func TestAmendableScopesSupersession(t *testing.T) {
	for _, status := range []Status{
		StatusDraft, StatusSubmitted, StatusRiskAssessed, StatusGreenLane, StatusYellowLane,
		StatusRedLane, StatusRejected, StatusScoringUnavailable,
	} {
		if !Amendable(status) {
			t.Fatalf("%s must be amendable into a superseding revision", status)
		}
	}
	for _, status := range []Status{StatusCleared, StatusSuperseded} {
		if Amendable(status) {
			t.Fatalf("%s must not be amendable", status)
		}
	}
}

func TestLaneStatesAmendIntoFreshDraft(t *testing.T) {
	// Amendment supersedes an assessed/laned declaration; the new revision is
	// a DRAFT whose only forward path is re-submission and re-scoring.
	for _, status := range []Status{StatusRiskAssessed, StatusGreenLane, StatusYellowLane, StatusRedLane} {
		if !ValidTransition(status, StatusSuperseded) {
			t.Fatalf("%s -> SUPERSEDED must be permitted for amendment", status)
		}
	}
	for _, pair := range [][2]Status{
		{StatusRiskAssessed, StatusSubmitted}, // assessed work is discarded, not resumed
		{StatusGreenLane, StatusSubmitted},
		{StatusYellowLane, StatusSubmitted},
		{StatusRedLane, StatusSubmitted},
		{StatusCleared, StatusSuperseded}, // cleared declarations stay immutable
	} {
		if ValidTransition(pair[0], pair[1]) {
			t.Fatalf("%s -> %s must be prohibited", pair[0], pair[1])
		}
	}
}

func TestLaneStatusMapping(t *testing.T) {
	if LaneStatus(LaneGreen) != StatusGreenLane || LaneStatus(LaneYellow) != StatusYellowLane || LaneStatus(LaneRed) != StatusRedLane {
		t.Fatal("lane statuses must map to their lifecycle states")
	}
}

func TestCreateRequestValidate(t *testing.T) {
	valid := CreateRequest{
		RequestID:          "req-decl-0001",
		DeclarationRef:     "NCS-2026-ABC123",
		DeclarationType:    TypeImport,
		HSCode:             "8703.24",
		GoodsDescription:   "Used motor vehicles for transport",
		CountryOfOrigin:    "DE",
		PortOfEntry:        "APAPA",
		GrossWeightKg:      12000,
		NetWeightKg:        11500,
		NumberOfPackages:   4,
		ConsigneeID:        "consignee-dangote-01",
		OperatorID:         "operator-apapa-01",
		InvoiceAmountMinor: 500000000,
		InvoiceCurrency:    "NGN",
		TariffBPS:          2000,
		VatBPS:             750,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	cases := map[string]func(*CreateRequest){
		"short request id":   func(r *CreateRequest) { r.RequestID = "short" },
		"bad ref":            func(r *CreateRequest) { r.DeclarationRef = "lowercase-ref" },
		"bad type":           func(r *CreateRequest) { r.DeclarationType = "SIDECAR" },
		"bad hs code":        func(r *CreateRequest) { r.HSCode = "123" },
		"short description":  func(r *CreateRequest) { r.GoodsDescription = "cars" },
		"bad origin":         func(r *CreateRequest) { r.CountryOfOrigin = "DEU" },
		"zero gross weight":  func(r *CreateRequest) { r.GrossWeightKg = 0 },
		"net above gross":    func(r *CreateRequest) { r.NetWeightKg = r.GrossWeightKg + 1 },
		"zero packages":      func(r *CreateRequest) { r.NumberOfPackages = 0 },
		"empty consignee":    func(r *CreateRequest) { r.ConsigneeID = "" },
		"negative freight":   func(r *CreateRequest) { r.FreightAmountMinor = -1 },
		"zero invoice":       func(r *CreateRequest) { r.InvoiceAmountMinor = 0 },
		"lowercase currency": func(r *CreateRequest) { r.InvoiceCurrency = "ngn" },
		"tariff above 100%":  func(r *CreateRequest) { r.TariffBPS = 10001 },
		"negative vat":       func(r *CreateRequest) { r.VatBPS = -1 },
	}
	for name, mutate := range cases {
		request := valid
		mutate(&request)
		if err := request.Validate(); err == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
}
