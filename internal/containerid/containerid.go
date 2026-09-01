// Package containerid implements ISO 6346 freight-container number
// validation, including the mandatory check digit. Format-only checks
// (^[A-Z]{4}[0-9]{7}$) accept numbers whose check digit was never
// recomputed; this package recomputes it with the standard weighted
// 2^n mod-11 algorithm so transposed or mistyped numbers are rejected
// fail-closed on ingest/registration paths.
package containerid

import "regexp"

// Pattern is the ISO 6346 format: 4 uppercase letters (owner code +
// category identifier), 6 serial digits and 1 check digit.
var Pattern = regexp.MustCompile(`^[A-Z]{4}[0-9]{7}$`)

// letterValues is the ISO 6346 letter-to-number mapping. The values skip
// the multiples of 11 (11, 22, 33) by design of the standard.
var letterValues = [26]int{
	10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, // A-M
	25, 26, 27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 38, // N-Z
}

// CheckDigit recomputes the ISO 6346 check digit for the first 10
// characters (4 letters + 6 serial digits) of code. ok is false when the
// prefix is malformed.
func CheckDigit(code string) (digit int, ok bool) {
	if len(code) != 11 || !Pattern.MatchString(code) {
		return 0, false
	}
	sum := 0
	for i := 0; i < 10; i++ {
		c := code[i]
		var value int
		if c >= 'A' && c <= 'Z' {
			value = letterValues[c-'A']
		} else {
			value = int(c - '0')
		}
		sum += value << uint(i)
	}
	digit = sum % 11
	if digit == 10 {
		digit = 0
	}
	return digit, true
}

// Valid reports whether code is a well-formed ISO 6346 container number
// whose check digit recomputes to the trailing digit.
func Valid(code string) bool {
	digit, ok := CheckDigit(code)
	if !ok {
		return false
	}
	return int(code[10]-'0') == digit
}
