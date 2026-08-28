package declarations

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Business rules ported from singlewindow server/businessRules.ts as pure,
// deterministic logic: HS-code validation (WCO Harmonised System), CIF-based
// duty calculation, risk-lane assignment, permit validity and SLA windows.
// The donor's deterministic HS-hash risk "score" fallback is deliberately NOT
// ported: scoring is an external fail-closed boundary (see scorer.go).

// ─── HS Code Validation (WCO Harmonised System) ─────────────────────────────

// NormalizeHSCode strips dots and spaces and validates the WCO format:
// digits only, at least 6 (international standard), at most 10 digits.
func NormalizeHSCode(hsCode string) (string, error) {
	cleaned := strings.ReplaceAll(strings.ReplaceAll(hsCode, ".", ""), " ", "")
	if cleaned == "" {
		return "", errors.New("hs_code is required")
	}
	for _, digit := range cleaned {
		if digit < '0' || digit > '9' {
			return "", errors.New("hs_code must contain only digits (dots are stripped automatically)")
		}
	}
	if len(cleaned) < 6 {
		return "", errors.New("hs_code must be at least 6 digits for the international standard")
	}
	if len(cleaned) > 10 {
		return "", errors.New("hs_code must not exceed 10 digits")
	}
	return cleaned, nil
}

// ─── Risk Score -> Lane Assignment ───────────────────────────────────────────

// highRiskCountries add +20 to the adjusted score (ported donor list).
var highRiskCountries = map[string]bool{
	"AF": true, "BY": true, "CU": true, "IR": true, "KP": true, "LY": true,
	"MM": true, "RU": true, "SD": true, "SY": true, "VE": true, "YE": true, "ZW": true,
}

// controlledHSPrefixes add +15 (arms, dual-use chemicals/machinery).
var controlledHSPrefixes = map[string]bool{
	"93": true, // arms and ammunition
	"28": true, // chemicals (dual-use)
	"84": true, // nuclear reactors (dual-use)
	"85": true, // electrical machinery (dual-use)
}

// RiskInput feeds the lane assignment rules. Score comes from the external
// fail-closed scorer; HighValueThresholdMinor is configured per deployment in
// the invoice currency minor units (0 disables the high-value adjustment).
type RiskInput struct {
	Score                   int // 0-100, from the scorer
	IsAEO                   bool
	IsSanctioned            bool
	InvoiceAmountMinor      int64
	HighValueThresholdMinor int64
	CountryOfOrigin         string
	HSCode                  string
}

// RiskAssessment is the lane decision with its audit trail.
type RiskAssessment struct {
	Lane          RiskLane
	AdjustedScore int
	Reasons       []string
}

// AssignRiskLane applies the ported lane rules in order: a sanctioned party
// is an automatic red lane; otherwise origin, value, goods-category and AEO
// adjustments move the scorer's score before the 70/40 thresholds assign the
// lane. The rules are pure — they never fabricate a score.
func AssignRiskLane(input RiskInput) (RiskAssessment, error) {
	if input.Score < 0 || input.Score > 100 {
		return RiskAssessment{}, errors.New("risk score must be between 0 and 100")
	}
	if input.IsSanctioned {
		return RiskAssessment{
			Lane:          LaneRed,
			AdjustedScore: 100,
			Reasons:       []string{"sanctioned party detected — automatic red lane"},
		}, nil
	}
	adjusted := input.Score
	var reasons []string
	if highRiskCountries[input.CountryOfOrigin] {
		adjusted = min(100, adjusted+20)
		reasons = append(reasons, fmt.Sprintf("high-risk country of origin: %s", input.CountryOfOrigin))
	}
	if input.HighValueThresholdMinor > 0 && input.InvoiceAmountMinor > input.HighValueThresholdMinor {
		adjusted = min(100, adjusted+10)
		reasons = append(reasons, "high-value shipment")
	}
	if len(input.HSCode) >= 2 && controlledHSPrefixes[input.HSCode[:2]] {
		adjusted = min(100, adjusted+15)
		reasons = append(reasons, fmt.Sprintf("controlled goods category: %s", input.HSCode[:2]))
	}
	if input.IsAEO {
		adjusted = max(0, adjusted-20)
		reasons = append(reasons, "AEO status: risk score reduced by 20 points")
	}
	assessment := RiskAssessment{AdjustedScore: adjusted, Reasons: reasons}
	switch {
	case adjusted >= 70:
		assessment.Lane = LaneRed
		assessment.Reasons = append(assessment.Reasons, fmt.Sprintf("risk score %d >= 70 — physical examination required", adjusted))
	case adjusted >= 40:
		assessment.Lane = LaneYellow
		assessment.Reasons = append(assessment.Reasons, fmt.Sprintf("risk score %d >= 40 — documentary review required", adjusted))
	default:
		assessment.Lane = LaneGreen
		assessment.Reasons = append(assessment.Reasons, fmt.Sprintf("risk score %d < 40 — auto-clearance eligible", adjusted))
	}
	return assessment, nil
}

// ─── Duty Calculation (CIF-based, integer minor units) ──────────────────────

// DutyInput is the CIF duty computation input: integer minor amounts and
// basis-point rates.
type DutyInput struct {
	InvoiceAmountMinor   int64 // FOB
	FreightAmountMinor   int64
	InsuranceAmountMinor int64
	TariffBPS            int
	VatBPS               int
	LevyBPS              int
	ExciseBPS            int
}

// DutyBreakdown is the computed assessment in invoice-currency minor units.
type DutyBreakdown struct {
	CIFMinor       int64
	DutyMinor      int64
	VatMinor       int64
	LevyMinor      int64
	ExciseMinor    int64
	TotalDutyMinor int64
}

// applyBPS multiplies an integer minor amount by basis points, rounding half
// up — deterministic integer money math, no floats.
func applyBPS(amount int64, bps int) int64 {
	if amount == 0 || bps == 0 {
		return 0
	}
	return (amount*int64(bps) + 5000) / 10000
}

// CalculateDuty computes the ported CIF-based assessment: CIF = invoice +
// freight + insurance; duty, levy and excise on CIF; VAT on CIF + duty +
// levy + excise (donor/Ghana standard). All arithmetic is integer.
func CalculateDuty(input DutyInput) (DutyBreakdown, error) {
	if input.InvoiceAmountMinor <= 0 {
		return DutyBreakdown{}, errors.New("invoice amount must be positive")
	}
	if input.FreightAmountMinor < 0 || input.InsuranceAmountMinor < 0 {
		return DutyBreakdown{}, errors.New("freight and insurance amounts must be non-negative")
	}
	for name, bps := range map[string]int{
		"tariff": input.TariffBPS, "vat": input.VatBPS, "levy": input.LevyBPS, "excise": input.ExciseBPS,
	} {
		if bps < 0 || bps > 10000 {
			return DutyBreakdown{}, fmt.Errorf("%s rate must be between 0 and 10000 basis points", name)
		}
	}
	breakdown := DutyBreakdown{}
	breakdown.CIFMinor = input.InvoiceAmountMinor + input.FreightAmountMinor + input.InsuranceAmountMinor
	breakdown.DutyMinor = applyBPS(breakdown.CIFMinor, input.TariffBPS)
	breakdown.LevyMinor = applyBPS(breakdown.CIFMinor, input.LevyBPS)
	breakdown.ExciseMinor = applyBPS(breakdown.CIFMinor, input.ExciseBPS)
	vatBase := breakdown.CIFMinor + breakdown.DutyMinor + breakdown.LevyMinor + breakdown.ExciseMinor
	breakdown.VatMinor = applyBPS(vatBase, input.VatBPS)
	breakdown.TotalDutyMinor = breakdown.DutyMinor + breakdown.VatMinor + breakdown.LevyMinor + breakdown.ExciseMinor
	return breakdown, nil
}

// ─── Permit Validity ─────────────────────────────────────────────────────────

// PermitValidity is the outcome of the ported permit expiry rule.
type PermitValidity struct {
	Valid           bool
	DaysUntilExpiry int
	Warning         string
}

// CheckPermitValidity enforces that an OGA permit is usable at the check
// date: expired permits are invalid, permits expiring within 7/30 days carry
// the ported renewal warnings.
func CheckPermitValidity(permitNumber, issuingAgency string, expiresAt, checkDate time.Time) (PermitValidity, error) {
	if strings.TrimSpace(permitNumber) == "" || strings.TrimSpace(issuingAgency) == "" {
		return PermitValidity{}, errors.New("permit number and issuing agency are required")
	}
	if expiresAt.IsZero() {
		return PermitValidity{}, errors.New("permit expiry is required")
	}
	days := int(expiresAt.UTC().Truncate(24*time.Hour).Sub(checkDate.UTC().Truncate(24*time.Hour)).Hours() / 24)
	validity := PermitValidity{Valid: true, DaysUntilExpiry: days}
	switch {
	case days < 0:
		return PermitValidity{Valid: false, DaysUntilExpiry: days}, fmt.Errorf(
			"permit %s from %s expired %d days ago", permitNumber, issuingAgency, -days)
	case days <= 7:
		validity.Warning = fmt.Sprintf("permit %s expires in %d days — renew immediately", permitNumber, days)
	case days <= 30:
		validity.Warning = fmt.Sprintf("permit %s expires in %d days — renewal recommended", permitNumber, days)
	}
	return validity, nil
}

// ─── SLA Windows (WCO Time Release Study targets, hours) ────────────────────

// SLAWindowHours maps declaration type and lane to the ported TRS target.
var SLAWindowHours = map[DeclarationType]map[RiskLane]int{
	TypeImport:          {LaneGreen: 4, LaneYellow: 24, LaneRed: 72},
	TypeExport:          {LaneGreen: 2, LaneYellow: 12, LaneRed: 48},
	TypeTransit:         {LaneGreen: 1, LaneYellow: 8, LaneRed: 24},
	TypeReExport:        {LaneGreen: 4, LaneYellow: 24, LaneRed: 72},
	TypeTemporaryImport: {LaneGreen: 8, LaneYellow: 48, LaneRed: 120},
}

// SLADeadline returns submittedAt plus the TRS window for the type/lane;
// unknown combinations fall back to the ported 72-hour default.
func SLADeadline(submittedAt time.Time, declarationType DeclarationType, lane RiskLane) time.Time {
	hours := 72
	if lanes, ok := SLAWindowHours[declarationType]; ok {
		if window, ok := lanes[lane]; ok {
			hours = window
		}
	}
	return submittedAt.Add(time.Duration(hours) * time.Hour)
}
