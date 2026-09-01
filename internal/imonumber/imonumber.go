// Package imonumber implements IMO ship-identification number validation
// including the mandatory check digit. Format-only checks (^[0-9]{7}$)
// accept numbers whose check digit was never recomputed; this package
// applies the standard weighted mod-10 algorithm (weights 7..2 over the
// first six digits) so mistyped IMO numbers are rejected fail-closed on
// vessel registration paths.
package imonumber

import "regexp"

// Pattern is the IMO number format: exactly seven digits.
var Pattern = regexp.MustCompile(`^[0-9]{7}$`)

// CheckDigit recomputes the IMO check digit over the first six digits of
// imo. ok is false when imo is not exactly seven digits.
func CheckDigit(imo string) (digit int, ok bool) {
	if !Pattern.MatchString(imo) {
		return 0, false
	}
	sum := 0
	for i := 0; i < 6; i++ {
		sum += int(imo[i]-'0') * (7 - i)
	}
	return sum % 10, true
}

// Valid reports whether imo is a well-formed seven-digit IMO number whose
// check digit recomputes to the trailing digit.
func Valid(imo string) bool {
	digit, ok := CheckDigit(imo)
	if !ok {
		return false
	}
	return int(imo[6]-'0') == digit
}
