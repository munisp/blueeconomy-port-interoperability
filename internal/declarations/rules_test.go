package declarations

import (
	"testing"
	"time"
)

func TestNormalizeHSCode(t *testing.T) {
	for name, input := range map[string]string{
		"plain 6 digits":  "870324",
		"dotted form":     "8703.24",
		"spaced form":     "8703 24 10",
		"10 digit tariff": "8703241000",
	} {
		if _, err := NormalizeHSCode(input); err != nil {
			t.Fatalf("%s (%q) must normalize: %v", name, input, err)
		}
	}
	if cleaned, err := NormalizeHSCode("8703.24"); err != nil || cleaned != "870324" {
		t.Fatalf("dots must be stripped, got %q, %v", cleaned, err)
	}
	for name, input := range map[string]string{
		"too short":   "87032",
		"too long":    "87032410001",
		"non-numeric": "87A324",
		"empty":       "",
		"only dots":   "....",
	} {
		if _, err := NormalizeHSCode(input); err == nil {
			t.Fatalf("%s (%q) must be rejected", name, input)
		}
	}
}

func TestCalculateDutyCIFStacking(t *testing.T) {
	// CIF = 10000 + 1000 + 500 = 11500 minor units.
	// duty 20% = 2300, levy 1% = 115, excise 5% = 575.
	// VAT base = 11500 + 2300 + 115 + 575 = 14490; VAT 7.5% = 1086.75 -> 1087 (half up).
	breakdown, err := CalculateDuty(DutyInput{
		InvoiceAmountMinor:   10000,
		FreightAmountMinor:   1000,
		InsuranceAmountMinor: 500,
		TariffBPS:            2000,
		VatBPS:               750,
		LevyBPS:              100,
		ExciseBPS:            500,
	})
	if err != nil {
		t.Fatalf("calculate duty: %v", err)
	}
	if breakdown.CIFMinor != 11500 {
		t.Fatalf("CIF = %d, want 11500", breakdown.CIFMinor)
	}
	if breakdown.DutyMinor != 2300 || breakdown.LevyMinor != 115 || breakdown.ExciseMinor != 575 {
		t.Fatalf("duty/levy/excise = %d/%d/%d, want 2300/115/575", breakdown.DutyMinor, breakdown.LevyMinor, breakdown.ExciseMinor)
	}
	if breakdown.VatMinor != 1087 {
		t.Fatalf("VAT = %d, want 1087 (VAT stacked on CIF + duty + levy + excise)", breakdown.VatMinor)
	}
	if breakdown.TotalDutyMinor != 2300+115+575+1087 {
		t.Fatalf("total = %d, want %d", breakdown.TotalDutyMinor, 2300+115+575+1087)
	}
}

func TestCalculateDutyZeroRatesAndValidation(t *testing.T) {
	breakdown, err := CalculateDuty(DutyInput{InvoiceAmountMinor: 10000})
	if err != nil {
		t.Fatalf("zero rates must succeed: %v", err)
	}
	if breakdown.TotalDutyMinor != 0 || breakdown.CIFMinor != 10000 {
		t.Fatalf("zero rates must yield zero duty, got %+v", breakdown)
	}
	if _, err := CalculateDuty(DutyInput{InvoiceAmountMinor: 0}); err == nil {
		t.Fatal("zero invoice must be rejected")
	}
	if _, err := CalculateDuty(DutyInput{InvoiceAmountMinor: 10000, TariffBPS: 10001}); err == nil {
		t.Fatal("rates above 10000 bps must be rejected")
	}
	if _, err := CalculateDuty(DutyInput{InvoiceAmountMinor: 10000, FreightAmountMinor: -1}); err == nil {
		t.Fatal("negative freight must be rejected")
	}
}

func TestAssignRiskLaneThresholds(t *testing.T) {
	base := RiskInput{Score: 0, CountryOfOrigin: "DE", HSCode: "870324", HighValueThresholdMinor: 0}
	for score, want := range map[int]RiskLane{0: LaneGreen, 39: LaneGreen, 40: LaneYellow, 69: LaneYellow, 70: LaneRed, 100: LaneRed} {
		input := base
		input.Score = score
		assessment, err := AssignRiskLane(input)
		if err != nil {
			t.Fatalf("score %d: %v", score, err)
		}
		if assessment.Lane != want || assessment.AdjustedScore != score {
			t.Fatalf("score %d: lane = %s (adjusted %d), want %s", score, assessment.Lane, assessment.AdjustedScore, want)
		}
	}
}

func TestAssignRiskLaneAdjustments(t *testing.T) {
	// Sanctioned party: automatic red, score pinned to 100.
	assessment, err := AssignRiskLane(RiskInput{Score: 5, IsSanctioned: true, CountryOfOrigin: "DE", HSCode: "870324"})
	if err != nil || assessment.Lane != LaneRed || assessment.AdjustedScore != 100 {
		t.Fatalf("sanctioned = %+v, want red/100", assessment)
	}
	// High-risk origin +20.
	assessment, _ = AssignRiskLane(RiskInput{Score: 30, CountryOfOrigin: "IR", HSCode: "870324"})
	if assessment.AdjustedScore != 50 || assessment.Lane != LaneYellow {
		t.Fatalf("high-risk origin = %+v, want adjusted 50 yellow", assessment)
	}
	// Controlled HS prefix +15.
	assessment, _ = AssignRiskLane(RiskInput{Score: 30, CountryOfOrigin: "DE", HSCode: "930190"})
	if assessment.AdjustedScore != 45 || assessment.Lane != LaneYellow {
		t.Fatalf("controlled HS = %+v, want adjusted 45 yellow", assessment)
	}
	// High value +10 when a threshold is configured.
	assessment, _ = AssignRiskLane(RiskInput{Score: 30, CountryOfOrigin: "DE", HSCode: "870324", InvoiceAmountMinor: 200000, HighValueThresholdMinor: 100000})
	if assessment.AdjustedScore != 40 || assessment.Lane != LaneYellow {
		t.Fatalf("high value = %+v, want adjusted 40 yellow", assessment)
	}
	// Threshold disabled at 0.
	assessment, _ = AssignRiskLane(RiskInput{Score: 30, CountryOfOrigin: "DE", HSCode: "870324", InvoiceAmountMinor: 1 << 60})
	if assessment.AdjustedScore != 30 {
		t.Fatalf("disabled threshold must not adjust, got %d", assessment.AdjustedScore)
	}
	// AEO -20, floored at 0.
	assessment, _ = AssignRiskLane(RiskInput{Score: 15, IsAEO: true, CountryOfOrigin: "DE", HSCode: "870324"})
	if assessment.AdjustedScore != 0 || assessment.Lane != LaneGreen {
		t.Fatalf("AEO = %+v, want adjusted 0 green", assessment)
	}
	// Adjustments cap at 100.
	assessment, _ = AssignRiskLane(RiskInput{Score: 90, CountryOfOrigin: "RU", HSCode: "930190", InvoiceAmountMinor: 200000, HighValueThresholdMinor: 100000})
	if assessment.AdjustedScore != 100 || assessment.Lane != LaneRed {
		t.Fatalf("stacked adjustments = %+v, want capped 100 red", assessment)
	}
	// Out-of-range scores are rejected — never clamped silently.
	if _, err := AssignRiskLane(RiskInput{Score: 101}); err == nil {
		t.Fatal("score above 100 must be rejected")
	}
	if _, err := AssignRiskLane(RiskInput{Score: -1}); err == nil {
		t.Fatal("negative score must be rejected")
	}
}

func TestCheckPermitValidity(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if _, err := CheckPermitValidity("P-1", "NAFDAC", now.Add(48*time.Hour), now); err != nil {
		t.Fatalf("unexpired permit must be valid: %v", err)
	}
	expired, err := CheckPermitValidity("P-1", "NAFDAC", now.Add(-48*time.Hour), now)
	if err == nil || expired.Valid {
		t.Fatalf("expired permit must be invalid, got %+v, %v", expired, err)
	}
	warning, err := CheckPermitValidity("P-1", "NAFDAC", now.Add(5*24*time.Hour), now)
	if err != nil || warning.Warning == "" {
		t.Fatalf("permit expiring within 7 days must warn, got %+v, %v", warning, err)
	}
	if _, err := CheckPermitValidity("", "NAFDAC", now.Add(48*time.Hour), now); err == nil {
		t.Fatal("missing permit number must be rejected")
	}
	if _, err := CheckPermitValidity("P-1", "NAFDAC", time.Time{}, now); err == nil {
		t.Fatal("missing expiry must be rejected")
	}
}

func TestSLADeadline(t *testing.T) {
	submitted := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if deadline := SLADeadline(submitted, TypeImport, LaneGreen); !deadline.Equal(submitted.Add(4 * time.Hour)) {
		t.Fatalf("import green SLA = %s, want +4h", deadline)
	}
	if deadline := SLADeadline(submitted, TypeTransit, LaneRed); !deadline.Equal(submitted.Add(24 * time.Hour)) {
		t.Fatalf("transit red SLA = %s, want +24h", deadline)
	}
	if deadline := SLADeadline(submitted, TypeTemporaryImport, LaneYellow); !deadline.Equal(submitted.Add(48 * time.Hour)) {
		t.Fatalf("temporary import yellow SLA = %s, want +48h", deadline)
	}
}
