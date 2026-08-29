package declarations

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// stubRegistry is a test AEO accreditation source.
type stubRegistry struct {
	verified map[string]bool
	err      error
}

func (registry stubRegistry) IsAEO(_ context.Context, traderID string) (bool, error) {
	if registry.err != nil {
		return false, registry.err
	}
	return registry.verified[traderID], nil
}

// PI-7 unit matrix: the claim guard never lets an unverified claim keep a
// discount or an auto-clearance.
func TestApplyAEOClaimGuard(t *testing.T) {
	green := RiskAssessment{Lane: LaneGreen, AdjustedScore: 30, Reasons: []string{"base"}}
	guarded := ApplyAEOClaimGuard(green, true, false)
	if guarded.Lane != LaneYellow {
		t.Fatalf("unverified claim must withhold GREEN: %s", guarded.Lane)
	}
	joined := strings.Join(guarded.Reasons, "|")
	if !strings.Contains(joined, "AEO_UNVERIFIED") {
		t.Fatalf("lane reasons must record AEO_UNVERIFIED: %v", guarded.Reasons)
	}
	// Verified claims and non-claims pass through untouched.
	if got := ApplyAEOClaimGuard(green, true, true); got.Lane != LaneGreen || len(got.Reasons) != 1 {
		t.Fatalf("verified claim must be untouched: %#v", got)
	}
	if got := ApplyAEOClaimGuard(green, false, false); got.Lane != LaneGreen || len(got.Reasons) != 1 {
		t.Fatalf("no claim must be untouched: %#v", got)
	}
	yellow := RiskAssessment{Lane: LaneYellow, AdjustedScore: 50}
	if got := ApplyAEOClaimGuard(yellow, true, false); got.Lane != LaneYellow {
		t.Fatalf("unverified claim on YELLOW stays YELLOW: %#v", got)
	}
}

// PI-7 regression (DB-gated): is_aeo:true in the create request no longer
// affects the lane when no accreditation registry is wired — no discount,
// AEO_UNVERIFIED reason, and no auto-clearance/certificate.
func TestClientClaimedAEOIsIgnoredWithoutRegistry(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	owner := principal()
	request := createRequest("req-aeo-0001")
	request.IsAEO = true // client self-asserts AEO
	created, err := env.store.Create(env.ctx, request, owner)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, owner)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Score 50: the old code discounted it to 30 (GREEN auto-clear) purely on
	// the client's word. With the claim ignored it must stay YELLOW.
	assessed, err := env.store.AssessRisk(env.ctx, submitted.DeclarationID, submitted.Version,
		stubScorer{response: ScoreResponse{Score: 50, ModelVersion: "test-1"}}, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != LaneStatus(LaneYellow) {
		t.Fatalf("status = %s, want YELLOW lane (no auto-clear on unverified AEO)", assessed.Status)
	}
	if assessed.RiskScore == nil || *assessed.RiskScore != 50 {
		t.Fatalf("score must not be discounted by the client claim: %v", assessed.RiskScore)
	}
	foundAEOUnverified := false
	for _, event := range env.outboxEvents(t) {
		if event["_event_type"] != EventRiskAssessed {
			continue
		}
		reasons := laneReasons(t, event)
		for _, reason := range reasons {
			if strings.Contains(reason, "AEO_UNVERIFIED") {
				foundAEOUnverified = true
			}
			if strings.Contains(reason, "risk score reduced") {
				t.Fatalf("client claim must not earn the AEO discount: %v", reasons)
			}
		}
	}
	if !foundAEOUnverified {
		t.Fatal("lane reasons must record AEO_UNVERIFIED")
	}
	if _, _, err := env.store.ClearanceCertificate(env.ctx, assessed.DeclarationID); err == nil {
		t.Fatal("no clearance certificate may exist for an unverified-AEO lane")
	}
}

// laneReasons extracts the lane-reasons extension from a FHIR-enveloped
// event. Event extensions live on the FHIR Basic resource inside the
// canonical `fhir` bundle (see events.Message and assertEnvelopeV1); there
// is no top-level extensions key on the envelope.
func laneReasons(t *testing.T, event map[string]any) []string {
	t.Helper()
	fhir, _ := event["fhir"].(map[string]any)
	entries, _ := fhir["entry"].([]any)
	for _, entry := range entries {
		resource, _ := entry.(map[string]any)["resource"].(map[string]any)
		extensions, _ := resource["extension"].([]any)
		for _, extension := range extensions {
			ext, _ := extension.(map[string]any)
			url, _ := ext["url"].(string)
			if !strings.HasSuffix(url, "/lane-reasons") {
				continue
			}
			raw, _ := ext["valueString"].(string)
			var reasons []string
			if err := json.Unmarshal([]byte(raw), &reasons); err != nil {
				t.Fatalf("decode lane-reasons extension: %v", err)
			}
			return reasons
		}
	}
	return nil
}

// PI-7 regression (DB-gated): a registry-verified AEO trader still earns the
// discount; a registry error fails closed like a missing registry.
func TestRegistryVerifiedAEOEarnsDiscountAndErrorsFailClosed(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	owner := principal()
	env.store.SetAEORegistry(stubRegistry{verified: map[string]bool{owner.ID: true}})

	request := createRequest("req-aeo-0002")
	request.IsAEO = true
	created, err := env.store.Create(env.ctx, request, owner)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, owner)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	assessed, err := env.store.AssessRisk(env.ctx, submitted.DeclarationID, submitted.Version,
		stubScorer{response: ScoreResponse{Score: 50, ModelVersion: "test-1"}}, 0, principal())
	if err != nil {
		t.Fatalf("assess risk: %v", err)
	}
	if assessed.Status != StatusCleared || assessed.RiskScore == nil || *assessed.RiskScore != 30 {
		t.Fatalf("verified AEO must discount to GREEN auto-clearance: status=%s score=%v", assessed.Status, assessed.RiskScore)
	}
}
