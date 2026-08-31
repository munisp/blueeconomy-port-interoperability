// Package tariff holds the versioned, effective-dated fee schedule used by
// offshore terminal-call and cruise dues assessments. Following the
// tariff-engine lesson, rates are DATA: every charge is a schedule row with a
// legal anchor and an effectiveness window, and Compute is a deterministic
// pure function of (schedule, facts, asOf) — same inputs, same assessment,
// always.
package tariff

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Domain string

const (
	DomainOffshoreTerminal Domain = "OFFSHORE_TERMINAL"
	DomainCruiseDues       Domain = "CRUISE_DUES"
)

// Unit is the measure a rule prices against.
type Unit string

const (
	UnitPerGRT    Unit = "PER_GRT"     // amount_minor × gross register tonnage
	UnitPerTon    Unit = "PER_TON"     // amount_minor × cargo tonnes
	UnitPerPax    Unit = "PER_PAX"     // amount_minor × passengers
	UnitPerCall   Unit = "PER_CALL"    // flat amount per call
	UnitPerGTBand Unit = "PER_GT_BAND" // per-GT rate selected by tonnage band
)

var (
	ErrScheduleInvalid  = errors.New("tariff schedule is invalid")
	ErrRuleInvalid      = errors.New("tariff rule is invalid")
	ErrNotEffective     = errors.New("no active tariff schedule is effective at the requested time")
	ErrBandGap          = errors.New("no per-GT band covers the vessel tonnage (fail closed: tariff data gap)")
	ErrFactsInvalid     = errors.New("assessment facts are invalid")
	ErrOverflow         = errors.New("assessment amount overflows int64")
	ErrNotFound         = errors.New("tariff schedule not found")
	ErrAssessmentReplay = errors.New("assessment idempotency key conflicts with a retained assessment")
)

var (
	scheduleIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{2,64}$`)
	componentPattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)
	currencyPattern   = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Rule is one rate row of a schedule. BandMin/BandMax select the per-GT rate
// for PER_GT_BAND rules; BandMax nil is unbounded.
type Rule struct {
	RuleID        string `json:"rule_id"`
	ComponentCode string `json:"component_code"`
	Unit          Unit   `json:"unit"`
	AmountMinor   int64  `json:"amount_minor"`
	BandMin       int64  `json:"band_min"`
	BandMax       *int64 `json:"band_max,omitempty"`
	LegalAnchor   string `json:"legal_anchor"`
}

// Schedule is an immutable, effective-dated set of rules for one revenue
// domain. Rate changes are registered as a new ScheduleID with a new
// effectiveness window.
type Schedule struct {
	ScheduleID    string     `json:"schedule_id"`
	Domain        Domain     `json:"domain"`
	Name          string     `json:"name"`
	Currency      string     `json:"currency"`
	EffectiveFrom time.Time  `json:"effective_from"`
	EffectiveTo   *time.Time `json:"effective_to,omitempty"`
	LegalAnchor   string     `json:"legal_anchor"`
	RegisteredBy  string     `json:"registered_by"`
	RegisteredAt  time.Time  `json:"registered_at"`
	Active        bool       `json:"active"`
	Rules         []Rule     `json:"rules"`
}

// Facts are the measured quantities of a call that rules price against.
type Facts struct {
	GrossTonnage int64 `json:"gross_tonnage"`
	CargoTonnes  int64 `json:"cargo_tonnes"`
	Passengers   int64 `json:"passengers"`
}

// LineItem is one computed charge line of an assessment.
type LineItem struct {
	ComponentCode string `json:"component_code"`
	Unit          Unit   `json:"unit"`
	Quantity      int64  `json:"quantity"`
	RateMinor     int64  `json:"rate_minor"`
	AmountMinor   int64  `json:"amount_minor"`
	BandMin       int64  `json:"band_min,omitempty"`
	BandMax       *int64 `json:"band_max,omitempty"`
	LegalAnchor   string `json:"legal_anchor"`
}

func canonicalText(value string, min, max int) bool {
	return len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value
}

// Validate enforces the canonical schedule shape before persistence or
// computation; invalid data fails closed, never silently repriced.
func (schedule Schedule) Validate() error {
	if !scheduleIDPattern.MatchString(schedule.ScheduleID) {
		return fmt.Errorf("%w: schedule_id", ErrScheduleInvalid)
	}
	if schedule.Domain != DomainOffshoreTerminal && schedule.Domain != DomainCruiseDues {
		return fmt.Errorf("%w: domain", ErrScheduleInvalid)
	}
	if !canonicalText(schedule.Name, 2, 256) || !canonicalText(schedule.LegalAnchor, 2, 512) || !canonicalText(schedule.RegisteredBy, 2, 256) {
		return fmt.Errorf("%w: name, legal anchor and registered_by must be canonical text", ErrScheduleInvalid)
	}
	if !currencyPattern.MatchString(schedule.Currency) {
		return fmt.Errorf("%w: currency must be an ISO-4217 uppercase code", ErrScheduleInvalid)
	}
	if schedule.EffectiveFrom.IsZero() {
		return fmt.Errorf("%w: effective_from is required", ErrScheduleInvalid)
	}
	if schedule.EffectiveTo != nil && !schedule.EffectiveTo.After(schedule.EffectiveFrom) {
		return fmt.Errorf("%w: effective_to must follow effective_from", ErrScheduleInvalid)
	}
	if len(schedule.Rules) == 0 {
		return fmt.Errorf("%w: at least one rule is required", ErrScheduleInvalid)
	}
	seen := map[string]bool{}
	for _, rule := range schedule.Rules {
		if err := rule.Validate(); err != nil {
			return err
		}
		key := rule.ComponentCode + "/" + fmt.Sprint(rule.BandMin)
		if seen[key] {
			return fmt.Errorf("%w: duplicate component/band rule %q", ErrScheduleInvalid, key)
		}
		seen[key] = true
	}
	return nil
}

// Validate enforces the rule shape mirrored by the tariff_rules constraints.
func (rule Rule) Validate() error {
	if !componentPattern.MatchString(rule.ComponentCode) {
		return fmt.Errorf("%w: component_code", ErrRuleInvalid)
	}
	switch rule.Unit {
	case UnitPerGRT, UnitPerTon, UnitPerPax, UnitPerCall:
		if rule.BandMin != 0 || rule.BandMax != nil {
			return fmt.Errorf("%w: bands apply only to PER_GT_BAND", ErrRuleInvalid)
		}
	case UnitPerGTBand:
		if rule.BandMin < 0 || (rule.BandMax != nil && *rule.BandMax <= rule.BandMin) {
			return fmt.Errorf("%w: invalid GT band", ErrRuleInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown unit %q", ErrRuleInvalid, rule.Unit)
	}
	if rule.AmountMinor < 0 {
		return fmt.Errorf("%w: amount_minor must be non-negative", ErrRuleInvalid)
	}
	if !canonicalText(rule.LegalAnchor, 2, 512) {
		return fmt.Errorf("%w: legal_anchor must be canonical text", ErrRuleInvalid)
	}
	return nil
}

// EffectiveAt reports whether the schedule prices a call as of the instant.
func (schedule Schedule) EffectiveAt(asOf time.Time) bool {
	asOf = asOf.UTC()
	if !schedule.Active || asOf.Before(schedule.EffectiveFrom.UTC()) {
		return false
	}
	return schedule.EffectiveTo == nil || asOf.Before(schedule.EffectiveTo.UTC())
}

// Compute deterministically prices facts against the schedule as of asOf.
// Line items are ordered by component code then band minimum, amounts are
// exact int64 minor-unit products (no floats anywhere on the money path),
// and any data gap (an unmatched GT band, an out-of-window schedule,
// overflow) is an error — never a silently under-computed charge.
func Compute(schedule Schedule, facts Facts, asOf time.Time) ([]LineItem, int64, error) {
	if err := schedule.Validate(); err != nil {
		return nil, 0, err
	}
	if !schedule.EffectiveAt(asOf) {
		return nil, 0, ErrNotEffective
	}
	if facts.GrossTonnage < 0 || facts.CargoTonnes < 0 || facts.Passengers < 0 {
		return nil, 0, ErrFactsInvalid
	}
	rules := make([]Rule, len(schedule.Rules))
	copy(rules, schedule.Rules)
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].ComponentCode != rules[j].ComponentCode {
			return rules[i].ComponentCode < rules[j].ComponentCode
		}
		return rules[i].BandMin < rules[j].BandMin
	})
	items := make([]LineItem, 0, len(rules))
	total := int64(0)
	for _, rule := range rules {
		var quantity int64
		switch rule.Unit {
		case UnitPerGRT:
			quantity = facts.GrossTonnage
		case UnitPerTon:
			quantity = facts.CargoTonnes
		case UnitPerPax:
			quantity = facts.Passengers
		case UnitPerCall:
			quantity = 1
		case UnitPerGTBand:
			if facts.GrossTonnage < rule.BandMin || (rule.BandMax != nil && facts.GrossTonnage >= *rule.BandMax) {
				continue
			}
			quantity = facts.GrossTonnage
		}
		if rule.AmountMinor != 0 && quantity > math.MaxInt64/rule.AmountMinor {
			return nil, 0, ErrOverflow
		}
		amount := rule.AmountMinor * quantity
		if amount != 0 && total > math.MaxInt64-amount {
			return nil, 0, ErrOverflow
		}
		total += amount
		items = append(items, LineItem{
			ComponentCode: rule.ComponentCode,
			Unit:          rule.Unit,
			Quantity:      quantity,
			RateMinor:     rule.AmountMinor,
			AmountMinor:   amount,
			BandMin:       rule.BandMin,
			BandMax:       rule.BandMax,
			LegalAnchor:   rule.LegalAnchor,
		})
	}
	// Fail closed on GT-band data gaps: a vessel whose tonnage matches no
	// band of a banded component must surface a data-gap error, not a zero
	// line for that component.
	for _, component := range bandedComponents(rules) {
		matched := false
		for _, item := range items {
			if item.ComponentCode == component && item.Unit == UnitPerGTBand {
				matched = true
			}
		}
		if !matched {
			return nil, 0, fmt.Errorf("%w: component %s, GRT %d", ErrBandGap, component, facts.GrossTonnage)
		}
	}
	return items, total, nil
}

func bandedComponents(rules []Rule) []string {
	seen := map[string]bool{}
	var components []string
	for _, rule := range rules {
		if rule.Unit == UnitPerGTBand && !seen[rule.ComponentCode] {
			seen[rule.ComponentCode] = true
			components = append(components, rule.ComponentCode)
		}
	}
	return components
}
