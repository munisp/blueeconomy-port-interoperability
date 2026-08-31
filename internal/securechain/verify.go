package securechain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// VerifyReleaseHolder implements booking.ReleaseVerifier: it proves, inside
// the caller's tenant transaction, that orgID is the verified tail holder of
// the ACTIVE release chain for containerID. Any deviation — no chain, chain
// on hold, pending nomination, expired chain, wrong org — refuses the
// booking fail-closed.
func (store *Store) VerifyReleaseHolder(ctx context.Context, tx pgx.Tx, containerID, orgID string) error {
	if orgID == "" {
		return errors.New("verified organisation identity is required")
	}
	chain, err := scanChain(tx.QueryRow(ctx, `
		SELECT `+chainColumns+` FROM secure_chains WHERE container_id = $1 AND status = 'ACTIVE'
		FOR SHARE`, containerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: no active secure chain for container %s", ErrNotTailHolder, containerID)
	}
	if err != nil {
		return fmt.Errorf("load secure chain for release check: %w", err)
	}
	if chain.VelocityHold {
		return ErrVelocityHold
	}
	if chain.ExpiresAt.Before(time.Now().UTC()) {
		return fmt.Errorf("%w: secure chain for container %s is past its expiry", ErrNotTailHolder, containerID)
	}
	chainTail, err := tailOf(ctx, tx, chain)
	if err != nil {
		return err
	}
	if chainTail.pendingLink != nil {
		return fmt.Errorf("%w: a secure-chain nomination is still pending for container %s", ErrNotTailHolder, containerID)
	}
	if chainTail.holderOrg == "" || chainTail.holderOrg != orgID {
		return ErrNotTailHolder
	}
	return nil
}
