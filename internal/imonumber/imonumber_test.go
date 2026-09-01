package imonumber

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidKnownGoodIMONumbers(t *testing.T) {
	for _, imo := range []string{
		"9074729", // canonical IMO worked example
		"9434187",
		"9321483",
		"1234567",
		"0000000",
	} {
		require.True(t, Valid(imo), "%s must validate", imo)
		digit, ok := CheckDigit(imo)
		require.True(t, ok)
		require.Equal(t, int(imo[6]-'0'), digit)
	}
}

func TestValidRejectsBadCheckDigit(t *testing.T) {
	// Single-digit corruptions of a valid IMO number must be rejected.
	for _, imo := range []string{
		"9074720",
		"9074721",
		"9074728",
		"9434186",
		"9321484",
	} {
		require.False(t, Valid(imo), "%s carries a wrong check digit", imo)
	}
}

func TestValidRejectsMalformed(t *testing.T) {
	for _, imo := range []string{
		"",
		"907472",     // too short
		"90747290",   // too long
		"907472A",    // non-digit
		" 9074729",   // untrimmed
		"9074729 ",   // untrimmed
		"IMO9074729", // prefixed
	} {
		require.False(t, Valid(imo), "%q must be rejected", imo)
	}
}
