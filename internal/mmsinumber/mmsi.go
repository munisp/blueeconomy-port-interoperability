// Package mmsinumber implements Maritime Mobile Service Identity (MMSI)
// validation. Ship-station MMSIs are exactly nine digits whose first three
// digits are the Maritime Identification Digits (MID) identifying the
// administration (flag state) that issued the identity. Format-only checks
// accept syntactically plausible but unallocated identities; this package
// validates structure and verifies the MID against the platform-admitted
// ITU-allocated MID table so mistyped or fabricated MMSIs are rejected
// fail-closed on vessel registry paths.
package mmsinumber

import "regexp"

// Pattern is the ship-station MMSI format: exactly nine digits, first digit
// non-zero (leading-zero blocks are coast-station / group-call identities,
// never ship stations).
var Pattern = regexp.MustCompile(`^[1-9][0-9]{8}$`)

// allocatedMIDs is the platform-admitted subset of the ITU-allocated
// Maritime Identification Digits table: Nigeria and its West/Central
// African coastal neighbours plus the major flag administrations calling in
// West African waters. The table is deliberately fail-closed: an MID
// outside the admitted set is rejected at registration and can only be
// admitted by a reviewed code change, never by request input.
var allocatedMIDs = map[string]bool{
	// Nigeria and West/Central African coastal states.
	"657": true, // Nigeria
	"610": true, // Benin
	"613": true, // Cameroon
	"615": true, // Congo (Republic of the)
	"619": true, // Côte d'Ivoire
	"626": true, // Gabon
	"627": true, // Ghana
	"663": true, // Senegal
	"667": true, // Sierra Leone
	"671": true, // Togo
	// Open registries and major trading partners.
	"211": true, // Germany
	"215": true, // Malta
	"219": true, // Denmark
	"220": true, // Denmark (additional)
	"226": true, // France
	"227": true, // France (additional)
	"228": true, // France (additional)
	"232": true, // United Kingdom
	"233": true, // United Kingdom (additional)
	"234": true, // United Kingdom (additional)
	"235": true, // United Kingdom (additional)
	"244": true, // Netherlands
	"245": true, // Netherlands (additional)
	"246": true, // Netherlands (additional)
	"257": true, // Norway
	"265": true, // Sweden
	"266": true, // Sweden (additional)
	"338": true, // United States of America
	"366": true, // United States of America (additional)
	"367": true, // United States of America (additional)
	"368": true, // United States of America (additional)
	"369": true, // United States of America (additional)
	"370": true, // Panama
	"371": true, // Panama
	"372": true, // Panama
	"373": true, // Panama
	"538": true, // Marshall Islands
	"477": true, // Hong Kong (China)
	"412": true, // China
	"413": true, // China (additional)
	"414": true, // China (additional)
	"419": true, // India
	"431": true, // Japan
	"432": true, // Japan (additional)
	"563": true, // Singapore (additional)
	"564": true, // Singapore (additional)
	"565": true, // Singapore
	"616": true, // Comoros
	"620": true, // Comoros (additional)
	"636": true, // Liberia
	"637": true, // Liberia (additional)
}

// MID extracts the Maritime Identification Digits (leading three digits) of
// mmsi. ok is false when mmsi is not structurally a ship-station MMSI.
func MID(mmsi string) (mid string, ok bool) {
	if !Pattern.MatchString(mmsi) {
		return "", false
	}
	return mmsi[:3], true
}

// Valid reports whether mmsi is a well-formed nine-digit ship-station MMSI
// whose MID is a platform-admitted ITU-allocated administration code.
func Valid(mmsi string) bool {
	mid, ok := MID(mmsi)
	if !ok {
		return false
	}
	return allocatedMIDs[mid]
}
