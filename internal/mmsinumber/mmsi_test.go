package mmsinumber

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidAdmitsAllocatedMIDs(t *testing.T) {
	for _, mmsi := range []string{
		"657123456", // Nigeria
		"636018234", // Liberia
		"370111000", // Panama
		"538090909", // Marshall Islands
		"235000777", // United Kingdom
		"565555001", // Singapore
	} {
		require.True(t, Valid(mmsi), "%s must validate", mmsi)
		mid, ok := MID(mmsi)
		require.True(t, ok)
		require.Equal(t, mmsi[:3], mid)
	}
}

func TestValidRejectsUnallocatedMID(t *testing.T) {
	// 999 is not an ITU-allocated MID; structural validity is not enough.
	require.False(t, Valid("999123456"))
	require.False(t, Valid("001123456"))
	require.False(t, Valid("555123456"))
}

func TestValidRejectsMalformed(t *testing.T) {
	for _, mmsi := range []string{
		"",
		"65712345",   // too short
		"6571234567", // too long
		"65712345A",  // non-digit
		" 657123456", // untrimmed
		"657123456 ", // untrimmed
		"MMSI657123456",
		"057123456", // leading zero: coast-station block, not a ship station
	} {
		require.False(t, Valid(mmsi), "%q must be rejected", mmsi)
		_, ok := MID(mmsi)
		require.False(t, ok, "%q must not yield an MID", mmsi)
	}
}
