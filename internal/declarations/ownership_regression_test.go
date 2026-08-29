package declarations

import (
	"errors"
	"testing"
)

// PI-5 regression: Submit and Amend are bound to the owning trader
// (declaration.trader_id = verified principal); a different trader's subject
// is denied with ErrForbidden, never a silent cross-trader mutation.
func TestSubmitAndAmendRejectCrossTraderPrincipals(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()
	owner := principal()
	intruder := Principal{ID: "mallory-other-trader", Role: "trader"}

	created, err := env.store.Create(env.ctx, createRequest("req-own-0001"), owner)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, intruder); !errors.Is(err, ErrForbidden) {
		t.Fatalf("cross-trader submit: err = %v, want ErrForbidden", err)
	}

	submitted, err := env.store.Submit(env.ctx, created.DeclarationID, created.Version, owner)
	if err != nil {
		t.Fatalf("owner submit: %v", err)
	}
	amendment := createRequest("req-own-0001")
	amendment.GoodsDescription = "Amended by the owner"
	if _, err := env.store.Amend(env.ctx, submitted.DeclarationID, amendment, submitted.Version, intruder); !errors.Is(err, ErrForbidden) {
		t.Fatalf("cross-trader amend: err = %v, want ErrForbidden", err)
	}
	if _, err := env.store.Amend(env.ctx, submitted.DeclarationID, amendment, submitted.Version, owner); err != nil {
		t.Fatalf("owner amend: %v", err)
	}
}
