package containerid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidKnownGoodNumbers(t *testing.T) {
	// Published ISO 6346 examples with correct check digits.
	for _, code := range []string{
		"CSQU3054383", // canonical ISO 6346 worked example
		"MSKU9070323",
	} {
		require.True(t, Valid(code), "%s must validate", code)
		digit, ok := CheckDigit(code)
		require.True(t, ok)
		require.Equal(t, int(code[10]-'0'), digit)
	}
}

func TestValidRejectsBadCheckDigit(t *testing.T) {
	// Every single-digit corruption of a valid number must be rejected:
	// the check digit exists to catch exactly this.
	for _, code := range []string{
		"CSQU3054380",
		"CSQU3054381",
		"CSQU3054382",
		"CSQU3054384",
		"CSQU3054385",
		"CSQU3054386",
		"CSQU3054387",
		"CSQU3054388",
		"CSQU3054389",
		"MSKU9070320",
	} {
		require.False(t, Valid(code), "%s carries a wrong check digit", code)
	}
}

func TestValidRejectsTransposition(t *testing.T) {
	// Adjacent-digit transposition changes the weighted sum.
	require.False(t, Valid("CSQU3054833"))
	require.False(t, Valid("CSQU3504383"))
}

func TestValidRejectsMalformed(t *testing.T) {
	for _, code := range []string{
		"",
		"CSQU305438",   // too short
		"CSQU30543833", // too long
		"csqu3054383",  // lowercase
		"CSQ13054383",  // digit inside owner code
		"CSQU30543A3",  // letter inside serial
		"CSQ U3054383", // whitespace
		" CSQU3054383", // untrimmed
		"CSQU3054383 ", // untrimmed
	} {
		require.False(t, Valid(code), "%q must be rejected", code)
	}
}

func TestCheckDigitRemainderTenFoldsToZero(t *testing.T) {
	// Construct a prefix whose weighted sum is congruent to 10 mod 11 and
	// prove the standard fold 10 -> 0 is applied. Search is deterministic
	// over serial digits of a fixed owner code.
	found := ""
	for serial := 0; serial < 1_000_000; serial++ {
		prefix := []byte("TEMU000000") // 4-letter owner code + 6 serial digits
		digits := []byte{'0' + byte(serial/100000%10), '0' + byte(serial/10000%10), '0' + byte(serial/1000%10), '0' + byte(serial/100%10), '0' + byte(serial/10%10), '0' + byte(serial%10)}
		copy(prefix[4:10], digits)
		sum := 0
		for i := 0; i < 10; i++ {
			c := prefix[i]
			var v int
			if c >= 'A' && c <= 'Z' {
				v = letterValues[c-'A']
			} else {
				v = int(c - '0')
			}
			sum += v << uint(i)
		}
		if sum%11 == 10 {
			found = string(prefix)
			break
		}
	}
	require.NotEmpty(t, found, "a remainder-10 prefix must exist")
	digit, ok := CheckDigit(found + "0")
	require.True(t, ok)
	require.Equal(t, 0, digit, "remainder 10 must fold to check digit 0")
	require.True(t, Valid(found+"0"))
	require.False(t, Valid(found+"1"))
}
