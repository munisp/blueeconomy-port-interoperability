package tariff

import (
	"errors"
	"testing"
	"time"
)

func band(min, max int64) *int64 {
	if max == 0 {
		return nil
	}
	return &max
}

func offshoreSchedule() Schedule {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Schedule{
		ScheduleID:    "npa-offshore-2026.1",
		Domain:        DomainOffshoreTerminal,
		Name:          "NPA offshore terminal tariff 2026.1",
		Currency:      "USD",
		EffectiveFrom: from,
		LegalAnchor:   "NPA tariff — harbour dues private jetty liquid bulk/SBM",
		RegisteredBy:  "npa-tariff-office",
		Active:        true,
		Rules: []Rule{
			{ComponentCode: "NPA_HARBOUR_DUES_SBM", Unit: UnitPerTon, AmountMinor: 56, LegalAnchor: "NPA tariff SBM US$0.56/ton"},
			{ComponentCode: "ENV_PROTECTION_LEVY", Unit: UnitPerTon, AmountMinor: 10, LegalAnchor: "NPA tariff EPL US$0.10/ton"},
			{ComponentCode: "PILOTAGE_ROYALTY_CALL", Unit: UnitPerCall, AmountMinor: 150000, LegalAnchor: "NPA compulsory pilotage royalty — offshore tanker vessels"},
			{ComponentCode: "SEA_PROTECTION_LEVY", Unit: UnitPerGTBand, AmountMinor: 125, BandMin: 0, BandMax: band(0, 50000), LegalAnchor: "Sea Protection Levy Regulations 2012 — foreign-flag US$1.25/GT"},
			{ComponentCode: "SEA_PROTECTION_LEVY", Unit: UnitPerGTBand, AmountMinor: 100, BandMin: 50000, BandMax: band(0, 100000), LegalAnchor: "Sea Protection Levy Regulations 2012 — foreign-flag US$1.00/GT"},
			{ComponentCode: "SEA_PROTECTION_LEVY", Unit: UnitPerGTBand, AmountMinor: 75, BandMin: 100000, BandMax: nil, LegalAnchor: "Sea Protection Levy Regulations 2012 — foreign-flag US$0.75/GT"},
		},
	}
}

func TestComputeIsDeterministicAndExact(t *testing.T) {
	schedule := offshoreSchedule()
	facts := Facts{GrossTonnage: 160000, CargoTonnes: 300000}
	asOf := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	items, total, err := Compute(schedule, facts, asOf)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	// 300000t × (56+10) + 150000 flat + 160000 GT × 75 (band >= 100000)
	want := int64(300000*66 + 150000 + 160000*75)
	if total != want {
		t.Fatalf("total = %d, want %d", total, want)
	}
	// Deterministic: identical inputs give identical line items.
	items2, total2, err := Compute(schedule, facts, asOf)
	if err != nil || total2 != total {
		t.Fatalf("recompute mismatch: %v %d", err, total2)
	}
	if len(items) != len(items2) {
		t.Fatal("line item count changed between recomputes")
	}
	for i := range items {
		if items[i] != items2[i] {
			t.Fatalf("line item %d changed between recomputes", i)
		}
	}
	// Stable ordering: component code then band min.
	for i := 1; i < len(items); i++ {
		if items[i-1].ComponentCode > items[i].ComponentCode {
			t.Fatalf("line items not ordered: %q before %q", items[i-1].ComponentCode, items[i].ComponentCode)
		}
	}
}

func TestComputeSelectsGTBandByTonnage(t *testing.T) {
	schedule := offshoreSchedule()
	asOf := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		grt  int64
		rate int64
	}{
		{30000, 125}, {50000, 100}, {99999, 100}, {100000, 75},
	} {
		items, _, err := Compute(schedule, Facts{GrossTonnage: test.grt, CargoTonnes: 1}, asOf)
		if err != nil {
			t.Fatalf("compute grt=%d: %v", test.grt, err)
		}
		found := false
		for _, item := range items {
			if item.ComponentCode == "SEA_PROTECTION_LEVY" {
				found = true
				if item.RateMinor != test.rate || item.AmountMinor != test.rate*test.grt {
					t.Fatalf("grt=%d: rate=%d amount=%d, want rate %d", test.grt, item.RateMinor, item.AmountMinor, test.rate)
				}
			}
		}
		if !found {
			t.Fatalf("grt=%d: no sea protection levy line", test.grt)
		}
	}
}

func TestComputeFailsClosedOutsideWindow(t *testing.T) {
	schedule := offshoreSchedule()
	before := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
	if _, _, err := Compute(schedule, Facts{GrossTonnage: 1}, before); !errors.Is(err, ErrNotEffective) {
		t.Fatalf("before window: got %v, want ErrNotEffective", err)
	}
	end := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	schedule.EffectiveTo = &end
	if _, _, err := Compute(schedule, Facts{GrossTonnage: 1}, end); !errors.Is(err, ErrNotEffective) {
		t.Fatalf("at exclusive window end: got %v, want ErrNotEffective", err)
	}
	schedule.Active = false
	if _, _, err := Compute(schedule, Facts{GrossTonnage: 1}, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrNotEffective) {
		t.Fatalf("inactive schedule: got %v, want ErrNotEffective", err)
	}
}

func TestComputeFailsClosedOnBandGap(t *testing.T) {
	schedule := offshoreSchedule()
	// Remove the unbounded band: GRT 160000 now matches nothing.
	schedule.Rules = schedule.Rules[:len(schedule.Rules)-1]
	_, _, err := Compute(schedule, Facts{GrossTonnage: 160000, CargoTonnes: 1}, time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrBandGap) {
		t.Fatalf("band gap: got %v, want ErrBandGap", err)
	}
}

func TestComputeFailsClosedOnOverflow(t *testing.T) {
	schedule := offshoreSchedule()
	schedule.Rules = []Rule{{ComponentCode: "NPA_HARBOUR_DUES_SBM", Unit: UnitPerTon, AmountMinor: 1 << 40, LegalAnchor: "overflow fixture"}}
	_, _, err := Compute(schedule, Facts{CargoTonnes: 1 << 40}, time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow: got %v, want ErrOverflow", err)
	}
}

func TestScheduleValidationRejectsBadData(t *testing.T) {
	valid := offshoreSchedule()
	cases := []func(Schedule) Schedule{
		func(s Schedule) Schedule { s.ScheduleID = "bad id!"; return s },
		func(s Schedule) Schedule { s.Domain = "OTHER"; return s },
		func(s Schedule) Schedule { s.Currency = "usd"; return s },
		func(s Schedule) Schedule { s.Rules = nil; return s },
		func(s Schedule) Schedule {
			s.Rules = append(s.Rules, s.Rules[0])
			return s
		},
		func(s Schedule) Schedule {
			s.Rules = []Rule{{ComponentCode: "X_BAD", Unit: "PER_NOSE", AmountMinor: 1, LegalAnchor: "x"}}
			return s
		},
		func(s Schedule) Schedule {
			s.Rules = []Rule{{ComponentCode: "FLAT", Unit: UnitPerCall, AmountMinor: 1, BandMin: 5, LegalAnchor: "x"}}
			return s
		},
	}
	for index, mutate := range cases {
		if err := mutate(valid).Validate(); err == nil {
			t.Fatalf("case %d: expected validation error", index)
		}
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid schedule rejected: %v", err)
	}
}
