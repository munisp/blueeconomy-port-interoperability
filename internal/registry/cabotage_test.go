package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func nigerianRule() *CabotageRule {
	return &CabotageRule{
		RuleID:                  "cabotage-ng-2003",
		RequiredFlag:            "NG",
		MinNationalOwnershipPct: 100, // Cabotage Act: wholly owned by Nigerian citizens
		RequireDomesticBuild:    true,
		WaiverAllowed:           true,
		Status:                  "ACTIVE",
	}
}

func TestEvaluateCabotageFullyEligible(t *testing.T) {
	result := EvaluateCabotage(nigerianRule(), CabotageFacts{
		FlagState:            "NG",
		BuildCountry:         "NG",
		NationalOwnershipPct: 100,
	})
	require.True(t, result.FlagCriterionMet)
	require.True(t, result.OwnershipCriterionMet)
	require.True(t, result.BuildCriterionMet)
	require.True(t, result.Eligible)
	require.False(t, result.Waiverable)
	require.Empty(t, result.Unmet)
}

func TestEvaluateCabotageForeignFlagFailsClosed(t *testing.T) {
	result := EvaluateCabotage(nigerianRule(), CabotageFacts{
		FlagState:            "PA",
		BuildCountry:         "NG",
		NationalOwnershipPct: 100,
	})
	require.False(t, result.Eligible)
	require.Equal(t, []string{"flag"}, result.Unmet)
	require.True(t, result.Waiverable, "waiver-permitting rule keeps a waiver path open")
}

func TestEvaluateCabotageOwnershipBoundary(t *testing.T) {
	rule := nigerianRule()
	// Exactly at threshold passes (>=).
	at := EvaluateCabotage(rule, CabotageFacts{FlagState: "NG", BuildCountry: "NG", NationalOwnershipPct: 100})
	require.True(t, at.OwnershipCriterionMet)
	// One point below fails.
	below := EvaluateCabotage(rule, CabotageFacts{FlagState: "NG", BuildCountry: "NG", NationalOwnershipPct: 99})
	require.False(t, below.OwnershipCriterionMet)
	require.Equal(t, []string{"ownership"}, below.Unmet)

	rule.MinNationalOwnershipPct = 60
	partial := EvaluateCabotage(rule, CabotageFacts{FlagState: "NG", BuildCountry: "NG", NationalOwnershipPct: 60})
	require.True(t, partial.Eligible)
}

func TestEvaluateCabotageBuildCriterionOnlyWhenRequired(t *testing.T) {
	rule := nigerianRule()
	foreignBuilt := EvaluateCabotage(rule, CabotageFacts{FlagState: "NG", BuildCountry: "KR", NationalOwnershipPct: 100})
	require.False(t, foreignBuilt.BuildCriterionMet)
	require.Equal(t, []string{"build"}, foreignBuilt.Unmet)

	rule.RequireDomesticBuild = false
	same := EvaluateCabotage(rule, CabotageFacts{FlagState: "NG", BuildCountry: "KR", NationalOwnershipPct: 100})
	require.True(t, same.BuildCriterionMet, "build criterion must not apply when the rule does not require it")
	require.True(t, same.Eligible)
}

func TestEvaluateCabotageNoRuleFailsClosed(t *testing.T) {
	result := EvaluateCabotage(nil, CabotageFacts{FlagState: "NG", BuildCountry: "NG", NationalOwnershipPct: 100})
	require.False(t, result.Eligible)
	require.False(t, result.Waiverable)
	require.NotEmpty(t, result.Unmet)
}

func TestEvaluateCabotageNoWaiverWhenRuleForbids(t *testing.T) {
	rule := nigerianRule()
	rule.WaiverAllowed = false
	result := EvaluateCabotage(rule, CabotageFacts{FlagState: "PA", BuildCountry: "PA", NationalOwnershipPct: 0})
	require.False(t, result.Eligible)
	require.False(t, result.Waiverable)
	require.ElementsMatch(t, []string{"flag", "ownership", "build"}, result.Unmet)
}
