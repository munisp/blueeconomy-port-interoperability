// GetByIMO returns the single vessel registered under an exact,
// check-digit-valid IMO number. It never fuzzy-matches: an unknown IMO is
// ErrNotFound so the handler answers 404.
